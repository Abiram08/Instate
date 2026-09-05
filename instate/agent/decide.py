"""Failure → decision pipeline: diagnose → Gate-1 → reason → Gate-2 → intent → execute.
Deterministic paths resolve without the model; judgment uses one enum-constrained LLM call.
Nothing the model emits reaches the gateway unverified (Gate-2).
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import PaymentGateway
from instate.agent.diagnose import diagnose, taxonomy_for
from instate.agent.execute import (
    escalate_to_human,
    execute_action,
    schedule_retry,
)
from instate.adapters.llm import Reasoner, validate_proposal
from instate.core.gate import check_proposal, evaluate
from instate.core.ledger import DuplicateEventError, record_event
from instate.core.locks import get_entity_lock
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_SCHEDULED,
    Decision,
    EntityState,
    Event,
)
from instate.core.projection import fold_events

# Events that trigger the pipeline
FAILURE_TRIGGER_EVENTS = {"PaymentFailed"}


# Result of processing one failure


@dataclass
class ProcessingResult:
    entity_id: str
    root_cause: str
    decision_id: int | None
    path: str  # "gate1_deny" | "deterministic" | "llm" | "policy_default" | "gate2_stop"
    executed_action: str | None
    llm_called: bool = False
    gateway_called: bool = False
    tokens: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def zero_llm(self) -> bool:
        """True if resolved without a model call."""
        return not self.llm_called


# Bounded model digest: state scalars + last 5 events + up to 3 precedents


async def build_context(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    root_cause: str,
    policy_version: int,
    precedents: list[dict] | None = None,
) -> dict:
    """Build the bounded model digest. Advisory dunning_step/customer_tz inform; neither gates."""
    from instate.core.dunning import next_sequence_step

    state = await session.get(EntityState, (merchant_id, entity_id))
    timeline = await session.execute(
        select(Event)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.id.desc())
        .limit(5)
    )
    recent = [
        {
            "event_type": e.event_type,
            "occurred_at": e.occurred_at.isoformat(),
            "payload": _lite_payload(e.payload),
        }
        for e in reversed(timeline.scalars().all())
    ]
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "root_cause": root_cause,
        "status": state.status if state else "ACTIVE",
        "last_failure_reason": state.last_failure_reason if state else None,
        "amount_at_risk_minor": state.amount_at_risk_minor if state else None,
        "open_ptp_due_at": (
            state.open_ptp_due_at.isoformat() if state and state.open_ptp_due_at else None
        ),
        "customer_tz": state.timezone if state else None,
        "policy_version": policy_version,
        "recent_events": recent,
        "precedents": (precedents or [])[:3],  # top_k fixed at 3 — bounded cost
        "dunning_step": await next_sequence_step(
            session, root_cause=root_cause, merchant_id=merchant_id, entity_id=entity_id
        ),
    }


def _lite_payload(payload: dict | None) -> dict | None:
    if not payload:
        return None
    keep = ("amount_minor", "root_cause", "failure_code", "channel", "due_at")
    return {k: payload[k] for k in keep if k in payload}


def context_hash(context: dict) -> bytes:
    canonical = json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).digest()


# One failed event, one decision


async def process_failure(
    session: AsyncSession,
    *,
    event: Event,
    reasoner: Reasoner,
    gateway: PaymentGateway,
    precedents: list[dict] | None = None,
    context: dict | None = None,
    now: datetime | None = None,
) -> ProcessingResult:
    """Run one failure event through the gated pipeline. Holds the per-entity lock for the full run."""
    async with get_entity_lock(event.merchant_id, event.entity_id):
        return await _process_failure_inner(
            session,
            event=event,
            reasoner=reasoner,
            gateway=gateway,
            precedents=precedents,
            context=context,
            now=now,
        )


async def _process_failure_inner(
    session: AsyncSession,
    *,
    event: Event,
    reasoner: Reasoner,
    gateway: PaymentGateway,
    precedents: list[dict] | None = None,
    context: dict | None = None,
    now: datetime | None = None,
) -> ProcessingResult:
    now = now or datetime.now(UTC)
    merchant_id = event.merchant_id
    entity_id = event.entity_id
    entity_type = event.entity_type

    # 1 · DIAGNOSE — deterministic map; UNKNOWN is a safe explicit branch
    failure_code = (event.payload or {}).get("failure_code") or (event.payload or {}).get(
        "error_description"
    )
    root_cause = await diagnose(session, failure_code=failure_code)
    taxonomy = await taxonomy_for(session, root_cause)
    action_class = taxonomy.default_action

    # Merchant context wins; payload method/jurisdiction/confirmed fill gaps.
    payload = event.payload or {}
    gate_context = {
        k: payload.get(k)
        for k in ("method", "jurisdiction", "confirmed")
        if payload.get(k) is not None
    }
    gate_context.update(context or {})

    # 2 · GATE-1 — deterministic (creates the decision row)
    gate1 = await evaluate(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        action_class=action_class,
        root_cause=root_cause,
        context=gate_context,
        now=now,
    )
    decision = await session.get(Decision, gate1.decision_id)

    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="FailureDiagnosed",
        occurred_at=now,
        payload={
            "root_cause": root_cause,
            "failure_code": failure_code,
            "trigger_event_id": event.id,
        },
        source_event_id=f"{event.id}:diag",
        decision_id=decision.id,
    )

    # 3 · Gate-1 DENY / REQUIRE_HUMAN → escalate
    if gate1.verdict == "DENY":
        await escalate_to_human(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            decision=decision,
            reason="gate1_deny",
            now=now,
        )
        await fold_events(session)
        return ProcessingResult(
            entity_id=entity_id,
            root_cause=root_cause,
            decision_id=decision.id,
            path="gate1_deny",
            executed_action="ESCALATE_HUMAN",
        )
    if gate1.verdict == "REQUIRE_HUMAN":
        await escalate_to_human(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            decision=decision,
            reason="gate1_require_human",
            now=now,
        )
        await fold_events(session)
        return ProcessingResult(
            entity_id=entity_id,
            root_cause=root_cause,
            decision_id=decision.id,
            path="gate1_deny",
            executed_action="ESCALATE_HUMAN",
        )

    # 4 · Deterministic route — the policy default IS the decision
    if taxonomy.deterministic:
        if action_class == ACTION_ESCALATE_HUMAN:
            await escalate_to_human(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                decision=decision,
                reason=f"deterministic_route:{root_cause}",
                now=now,
            )
            await fold_events(session)
            return ProcessingResult(
                entity_id=entity_id,
                root_cause=root_cause,
                decision_id=decision.id,
                path="deterministic",
                executed_action="ESCALATE_HUMAN",
            )
        # Deterministic non-escalate actions execute directly.
        response = await execute_action(
            session,
            gateway=gateway,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            decision=decision,
            action=action_class,
            now=now,
        )
        await fold_events(session)
        return ProcessingResult(
            entity_id=entity_id,
            root_cause=root_cause,
            decision_id=decision.id,
            path="deterministic",
            executed_action=action_class,
            gateway_called=True,
            notes=[f"gateway:{response.status}"],
        )

    # 5 · REASON — the single enum-constrained LLM call
    # Digest is separate from merchant `context`; Gate-2 gets the merged gate_context.
    digest = await build_context(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        root_cause=root_cause,
        policy_version=gate1.policy_version or 1,
        precedents=precedents,
    )
    raw = await reasoner.propose(digest)
    proposal = validate_proposal(raw)

    llm_called = proposal is not None
    if proposal is None:
        # Model failure → deterministic policy default.
        proposal = {
            "action": action_class,
            "timing": "T_PLUS_24H",
            "rationale": "policy default — model unavailable or invalid output",
            "confidence": None,
        }
        path = "policy_default"
    else:
        path = "llm"

    decision.proposal = proposal
    decision.model = getattr(reasoner, "model_name", None)
    decision.inputs_hash = context_hash(digest)
    decision.prompt_text = _render_prompt(digest)
    # Precedent informs only; it cannot gate.
    decision.precedent_ids = [
        p["case_id"] for p in (precedents or [])[:3] if isinstance(p, dict) and "case_id" in p
    ] or None
    usage = getattr(reasoner, "last_usage", None)
    decision.tokens_in = len(decision.prompt_text) // 4
    decision.tokens_out = usage[1] if usage else 60
    result_tokens = decision.tokens_in + decision.tokens_out
    await session.flush()

    # 6 · GATE-2 — concrete proposal against policy (deterministic)
    gate2 = await check_proposal(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision_id=decision.id,
        proposal=proposal,
        root_cause=root_cause,
        context=gate_context,
        now=now,
    )
    if gate2.verdict != "ALLOW":
        await escalate_to_human(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            decision=decision,
            reason=f"gate2_{gate2.verdict.lower()}",
            now=now,
        )
        await fold_events(session)
        return ProcessingResult(
            entity_id=entity_id,
            root_cause=root_cause,
            decision_id=decision.id,
            path="gate2_stop",
            executed_action="ESCALATE_HUMAN",
            llm_called=llm_called,
            tokens=result_tokens,
        )

    # 7 · EXECUTE — outbox, action-dependent
    if proposal["action"] == ACTION_RETRY_SCHEDULED:
        await schedule_retry(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            decision=decision,
            timing=proposal.get("timing"),
            root_cause=root_cause,
            now=now,
        )
        await fold_events(session)
        return ProcessingResult(
            entity_id=entity_id,
            root_cause=root_cause,
            decision_id=decision.id,
            path=path,
            executed_action="RETRY_SCHEDULED",
            llm_called=llm_called,
            tokens=result_tokens,
        )

    response = await execute_action(
        session,
        gateway=gateway,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision=decision,
        action=proposal["action"],
        proposal=proposal,
        now=now,
    )
    await fold_events(session)
    return ProcessingResult(
        entity_id=entity_id,
        root_cause=root_cause,
        decision_id=decision.id,
        path=path,
        executed_action=proposal["action"],
        llm_called=llm_called,
        gateway_called=True,
        tokens=result_tokens,
        notes=[f"gateway:{response.status}"],
    )


def _render_prompt(digest: dict) -> str:
    return json.dumps(digest, sort_keys=True, separators=(",", ":"), default=str)


# Drain — webhooks append; the drain runs the pipeline


async def drain_pending(
    session: AsyncSession,
    *,
    reasoner: Reasoner,
    gateway: PaymentGateway,
    precedents: list[dict] | None = None,
    context: dict | None = None,
    now: datetime | None = None,
) -> list[ProcessingResult]:
    """Process every failure event with no FailureDiagnosed yet."""
    now = now or datetime.now(UTC)

    triggers = await session.execute(
        select(Event).where(Event.event_type.in_(FAILURE_TRIGGER_EVENTS)).order_by(Event.id.asc())
    )
    trigger_events = list(triggers.scalars().all())
    if not trigger_events:
        return []

    diagnosed = await session.execute(
        select(Event.payload).where(Event.event_type == "FailureDiagnosed")
    )
    handled_trigger_ids: set[int] = set()
    for (payload,) in diagnosed.all():
        if payload and payload.get("trigger_event_id") is not None:
            handled_trigger_ids.add(payload["trigger_event_id"])

    results: list[ProcessingResult] = []
    for event in trigger_events:
        if event.id in handled_trigger_ids:
            continue
        try:
            result = await process_failure(
                session,
                event=event,
                reasoner=reasoner,
                gateway=gateway,
                precedents=precedents,
                context=context,
                now=now,
            )
        except DuplicateEventError:
            # Lost the race: a concurrent worker diagnosed this trigger
            # between our handled-check and our marker write. Its decision
            # stands; ours never existed. Skip, don't crash the drain.
            await session.rollback()
            continue
        results.append(result)
    return results
