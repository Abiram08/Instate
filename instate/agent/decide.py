"""Instate decide — the thin agent pipeline (§6).

webhook → dedupe → DIAGNOSE → GATE-1 → [det. route?] → REASON → GATE-2
        → INTENT → EXECUTE → COMMIT

This is a workflow, not an autonomous loop (deliberately): the model
directs nothing, the loop is zero turns, and every path is testable
without a model (the tests inject a FakeReasoner).

The majority of events never reach the model:
  - dedupe (ledger-level, upstream)
  - Gate-1 DENY at the ceiling      → EscalatedToHuman, 0 tokens
  - fixed-action routes (fraud_block, mandate_inactive, UNKNOWN)
                                    → the policy default IS the decision
  - LLM failure/unavailable         → the deterministic policy default

Everything that involves judgment (timing, channel, method) goes through
exactly one LLM call, and nothing the model emits reaches Razorpay
unverified — Gate-2 checks the concrete proposal first.
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
from instate.core.ledger import record_event
from instate.core.locks import get_entity_lock
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_SCHEDULED,
    Decision,
    EntityState,
    Event,
)
from instate.core.projection import fold_events

# The webhook events that kick off the pipeline (drain picks these up)
FAILURE_TRIGGER_EVENTS = {"PaymentFailed"}


# ---------------------------------------------------------------------------
# Result — what one processed failure produced (the demo's raw data)
# ---------------------------------------------------------------------------


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
        """Resolved without reaching the model — the headline metric."""
        return not self.llm_called


# ---------------------------------------------------------------------------
# Context builder — the bounded digest the model sees (§7 token accounting)
# ---------------------------------------------------------------------------


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
    """The compact, pre-filtered context: state scalars + last 5 timeline
    events + exactly `top_k` precedent one-liners. Never a raw event dump
    (§7). `precedents` arrives from L3 (Stage 5) — empty until then."""
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
        "policy_version": policy_version,
        "recent_events": recent,
        "precedents": (precedents or [])[:3],  # top_k fixed at 3 — bounded cost
    }


def _lite_payload(payload: dict | None) -> dict | None:
    """Trim a payload to the fields the model could use — tokens are a
    budget, and raw payloads are the fastest way to blow it (§7)."""
    if not payload:
        return None
    keep = ("amount_minor", "root_cause", "failure_code", "channel", "due_at")
    return {k: payload[k] for k in keep if k in payload}


def context_hash(context: dict) -> bytes:
    """sha256 over the canonical context — what `inputs_hash` hashes."""
    canonical = json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).digest()


# ---------------------------------------------------------------------------
# The pipeline — one failed event, one decision
# ---------------------------------------------------------------------------


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
    """Run one failure event through the full gated pipeline.

    `context` carries merchant-side decision facts (e.g. {"dnc": True})
    that rules key on; it flows into both gates.

    Holds the process-wide per-entity lock (`core.locks`) for the whole
    run — Gate-1, the model call, Gate-2, intent, outcome. A concurrent
    pipeline for the SAME entity waits here and then observes this
    run's committed events (ceiling DENY instead of a double retry).
    Different entities never block each other.
    """
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
    """Run one failure event through the full gated pipeline.

    `context` carries merchant-side decision facts (e.g. {"dnc": True})
    that rules key on; it flows into both gates.
    """
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

    # 2 · GATE-1 — deterministic, zero tokens (creates the decision row)
    gate1 = await evaluate(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        action_class=action_class,
        root_cause=root_cause,
        context=context,
        now=now,
    )
    decision = await session.get(Decision, gate1.decision_id)

    # 2b · Write the diagnosis onto the ledger, linked to the decision
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

    # 3 · Gate-1 DENY / REQUIRE_HUMAN → stop here, zero tokens
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
        # A deterministic non-escalate action would execute directly —
        # the current taxonomy has none, but the branch is honest.
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

    # 5 · REASON — the one LLM call, enum-constrained (§7)
    # NOTE: the model's context digest is a separate variable — the incoming
    # merchant `context` (dnc, consent, ...) must reach Gate-2 unspoiled.
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
        # LLM failed/unavailable → the deterministic policy default (§7).
        # No retries, no fallback gymnastics, no drama.
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
    # Reproducibility, literal (§5): the exact context the model saw
    decision.prompt_text = _render_prompt(digest)
    # Auditability of the advisory input: which cases informed this
    # decision (None when L3 was empty — the normal cold-start answer).
    # Precedent can be inspected, never blamed: it cannot gate.
    decision.precedent_ids = [
        p["case_id"] for p in (precedents or [])[:3] if isinstance(p, dict) and "case_id" in p
    ] or None
    # Token accounting (§7): input cost IS the rendered context (bounded
    # digest, flat regardless of history depth); output comes from the
    # reasoner when it can provide it. Zero-token paths never get here,
    # so their tokens stay NULL — exactly how the "% resolved with zero
    # LLM calls" metric is computed.
    usage = getattr(reasoner, "last_usage", None)
    decision.tokens_in = len(decision.prompt_text) // 4
    decision.tokens_out = usage[1] if usage else 60
    result_tokens = decision.tokens_in + decision.tokens_out
    await session.flush()

    # 6 · GATE-2 — the concrete proposal, against policy (deterministic)
    gate2 = await check_proposal(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision_id=decision.id,
        proposal=proposal,
        root_cause=root_cause,
        context=context,
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
    """The exact prompt-side context rendered deterministically — stored
    on the decision row so 'reproducible' is literal, not just verifiable."""
    return json.dumps(digest, sort_keys=True, separators=(",", ":"), default=str)


# ---------------------------------------------------------------------------
# The drain — tick-loop half (webhooks only ever append to the ledger)
# ---------------------------------------------------------------------------


async def drain_pending(
    session: AsyncSession,
    *,
    reasoner: Reasoner,
    gateway: PaymentGateway,
    precedents: list[dict] | None = None,
    context: dict | None = None,
    now: datetime | None = None,
) -> list[ProcessingResult]:
    """Process every failure event that has no FailureDiagnosed yet.

    The webhook handler only appends; this drain is where the pipeline
    actually runs (ledger-first, §6 step 0). Trigger matching is by
    `FailureDiagnosed.payload.trigger_event_id` — computed in Python over
    the (small) diagnosed set, so it stays cross-backend.
    """
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
        result = await process_failure(
            session,
            event=event,
            reasoner=reasoner,
            gateway=gateway,
            precedents=precedents,
            context=context,
            now=now,
        )
        results.append(result)
    return results
