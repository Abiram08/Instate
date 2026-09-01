"""Instate gates — deterministic evaluation of what is allowed (§5-6).

A gate never returns true/false. It returns the evidence: a reason chain of
{rule_id, metric, observed, limit, verdict} entries that is both machine-
readable and human-readable at once. The chain is persisted on the decision
row — this is the track's "explainable" requirement satisfied by a data
structure (§5 of architecture.md).

Gate-1 (`evaluate`) checks the action CLASS against policy before any model
call: entities at their ceiling never reach the LLM at all — zero tokens.

Gate-2 (`check_proposal`) checks the CONCRETE proposal (this action, at this
time, with this confidence) after the one LLM call. Nothing the model emits
reaches Razorpay unverified.

Concurrency — TOCTOU closed: the gate-check → intent-write span holds a
per-entity lock (SELECT ... FOR UPDATE on the entity_state row), so two
concurrent events for the same entity cannot both pass the gate and both
act. On PostgreSQL this is a real row lock; on SQLite (dev/test) the
FOR UPDATE clause is a no-op by dialect design — correctness there comes
from single-writer serialization.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    CONFIDENCE_FLOOR,
    CUSTOMER_CONTACT_ACTIONS,
    EntityState,
    Decision,
    HARD_DECLINE_ROOT_CAUSES,
    MONEY_ATTEMPT_ACTIONS,
    STATUS_ACTIVE,
    VERDICT_ALLOW,
    VERDICT_DENY,
    VERDICT_REQUIRE_HUMAN,
    ACTION_AWAIT_PROMISE,
    ACTION_ESCALATE_HUMAN,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    LEGAL_ACTIONS,
)
from instate.core.policy import (
    active_policy_version,
    get_rules,
    metric_event_types,
    rule_applies_to,
)
from instate.core.projection import (
    get_windowed_count,
    has_new_method_since_last_failure,
)


# ---------------------------------------------------------------------------
# Reason chain helpers
# ---------------------------------------------------------------------------


def reason_entry(
    rule_id: str,
    verdict: str,
    *,
    metric: str | None = None,
    observed: int | None = None,
    limit: int | None = None,
    detail: str | None = None,
) -> dict:
    """One link in a reason chain — evidence, not a boolean (§5)."""
    entry: dict = {
        "rule_id": rule_id,
        "metric": metric,
        "observed": observed,
        "limit": limit,
        "verdict": verdict,
    }
    if detail is not None:
        entry["detail"] = detail
    return entry


def aggregate_verdict(chain: list[dict]) -> str:
    """Combine a reason chain: DENY beats REQUIRE_HUMAN beats ALLOW.

    A single DENY stops the action outright; REQUIRE_HUMAN routes the
    decision to a person; only a chain with neither may proceed.
    """
    verdicts = {entry["verdict"] for entry in chain}
    if VERDICT_DENY in verdicts:
        return VERDICT_DENY
    if VERDICT_REQUIRE_HUMAN in verdicts:
        return VERDICT_REQUIRE_HUMAN
    return VERDICT_ALLOW


# ---------------------------------------------------------------------------
# State-machine legality — the model only chooses among legal moves (§6)
# ---------------------------------------------------------------------------

# From each state-machine position, which action classes are legal at all?
# This is constrained decoding at the domain level — much stronger than a
# prompt that says "don't do that".
LEGAL_ACTIONS_BY_STATUS: dict[str, set[str]] = {
    "ACTIVE": set(LEGAL_ACTIONS),
    "DIAGNOSED": set(LEGAL_ACTIONS),
    "RETRY_SCHEDULED": {
        ACTION_RETRY_NOW,
        ACTION_RETRY_SCHEDULED,
        ACTION_SEND_PAYMENT_LINK,
        ACTION_UPDATE_MANDATE,
        ACTION_REQUEST_PAYMENT_METHOD,
        ACTION_AWAIT_PROMISE,
        ACTION_ESCALATE_HUMAN,
    },
    "AWAITING_PROMISE": {
        ACTION_AWAIT_PROMISE,  # reminder path
        ACTION_SEND_PAYMENT_LINK,
        ACTION_ESCALATE_HUMAN,
    },
    "ESCALATED": {ACTION_ESCALATE_HUMAN},  # a human owns it now
    "RECOVERED": set(),  # done — nothing to decide
    "WRITTEN_OFF": set(),  # done — nothing to decide
}


# ---------------------------------------------------------------------------
# The per-entity lock — TOCTOU closed (§6)
# ---------------------------------------------------------------------------


async def lock_entity_state(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
) -> EntityState:
    """SELECT ... FOR UPDATE on the entity's state row (get-or-create).

    The Gate-1 → intent-write span must hold this lock so two concurrent
    events for the same entity cannot both pass the gate and both act.
    If the entity has no state row yet, one is created (ACTIVE) inside
    the same transaction, so the lock exists before any evaluation.

    This is layer 1 of TOCTOU defense (§6): a real row lock on
    PostgreSQL, a no-op on SQLite by dialect design. Layer 2 is the
    process-wide per-entity lock (`core.locks.get_entity_lock`), held by
    every pipeline entry point (`process_failure`, both agents;
    `run_due_scheduled`; `reconcile_pending`) for the whole
    gate→intent span — which is what actually serializes concurrent
    pipelines on every backend.
    """
    result = await session.execute(
        select(EntityState)
        .where(
            EntityState.merchant_id == merchant_id,
            EntityState.entity_id == entity_id,
        )
        .with_for_update()
    )
    state = result.scalar_one_or_none()
    if state is None:
        state = EntityState(
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            status=STATUS_ACTIVE,
            last_event_id=0,
        )
        session.add(state)
        await session.flush()
    return state


# ---------------------------------------------------------------------------
# Gate-1 — evaluate the action CLASS (deterministic, zero tokens)
# ---------------------------------------------------------------------------


@dataclass
class GateResult:
    """What a gate returns: a verdict plus the evidence for it."""

    verdict: str  # ALLOW | DENY | REQUIRE_HUMAN
    reason_chain: list[dict] = field(default_factory=list)
    policy_version: int | None = None
    decision_id: int | None = None
    status: str | None = None  # state-machine position at evaluation time

    @property
    def allowed(self) -> bool:
        return self.verdict == VERDICT_ALLOW


async def evaluate(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    action_class: str,
    root_cause: str | None = None,
    context: dict | None = None,
    now: datetime | None = None,
    as_of: datetime | None = None,
    record: bool = True,
) -> GateResult:
    """Gate-1: may this entity take this action CLASS at all?

    Holds the per-entity lock; evaluates every in-force policy rule whose
    applies_when matches the context; computes counter metrics as indexed
    L0 counts (never cached, never drifting). DENY stops everything — the
    caller writes EscalatedToHuman and spends zero tokens.

    context carries the decision-time facts rules can key on
    (root_cause, issuer_country, ...). root_cause is shorthand for
    context={"root_cause": ...} and is folded into the context.
    `as_of` pins the knowledge cutoff for counterfactual replay
    (see get_windowed_count); live callers leave it unset.
    """
    now = now or datetime.now(UTC)

    # Context rules key on root_cause — make both spellings work
    full_context = dict(context or {})
    if root_cause is not None:
        full_context.setdefault("root_cause", root_cause)

    # TOCTOU lock: gate-check and the later intent-write share this row lock
    state = await lock_entity_state(session, merchant_id, entity_id, entity_type)

    version = await active_policy_version(session, entity_type)
    rules = await get_rules(session, entity_type, version)

    # Counter rules are action-class-relevant: retry caps guard money
    # attempts, contact caps guard customer contacts. Context rules always
    # fire — they key on the situation, not the action.
    is_money = action_class in MONEY_ATTEMPT_ACTIONS
    is_contact = action_class in CUSTOMER_CONTACT_ACTIONS

    chain: list[dict] = []
    for rule in rules:
        if not rule_applies_to(rule, full_context):
            continue

        if rule.metric is None:
            # Context rule: fires purely on applies_when (already matched)
            chain.append(reason_entry(rule.rule_id, rule.verdict, detail="context rule matched"))
            continue

        relevant = (rule.metric.startswith("retry") and is_money) or (
            rule.metric.startswith("contacts") and is_contact
        )
        if not relevant:
            continue

        # Counter rule: observed is an indexed L0 count over the window —
        # sub-ms at any per-entity volume, and exactly correct by construction
        window = timedelta(seconds=rule.window_seconds or 0)
        observed = await get_windowed_count(
            session,
            merchant_id,
            entity_id,
            rule.metric,
            window,
            event_types=metric_event_types(rule.metric),
            now=now,
            as_of=as_of,
        )
        fired = observed >= rule.limit_value
        chain.append(
            reason_entry(
                rule.rule_id,
                rule.verdict if fired else VERDICT_ALLOW,
                metric=rule.metric,
                observed=observed,
                limit=rule.limit_value,
            )
        )

    verdict = aggregate_verdict(chain)

    decision_id: int | None = None
    if record:
        decision = Decision(
            merchant_id=merchant_id,
            entity_id=entity_id,
            root_cause=full_context.get("root_cause"),
            gate1=chain,
            policy_version=version,
        )
        session.add(decision)
        await session.flush()
        decision_id = decision.id

    return GateResult(
        verdict=verdict,
        reason_chain=chain,
        policy_version=version,
        decision_id=decision_id,
        status=state.status,
    )


# ---------------------------------------------------------------------------
# Gate-2 — check the CONCRETE proposal (deterministic, post-model)
# ---------------------------------------------------------------------------


async def check_proposal(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision_id: int,
    proposal: dict,
    root_cause: str | None = None,
    context: dict | None = None,
    now: datetime | None = None,
    as_of: datetime | None = None,
) -> GateResult:
    """Gate-2: is THIS proposal (this action, now, this confidence) legal?

    The model's structured output is checked against:
    1. The closed action enum (a decoding-constrained model should never
       emit an illegal action — this is the backstop that makes it not matter)
    2. The state machine — the model only chooses among legal moves
    3. Counter policy re-checked for the concrete action class
       (retry caps for money attempts, contact caps for customer contacts)
    4. Do-Not-Contact / consent — what makes "compliant" TRUE, not asserted
    5. The confidence floor — low confidence routes to a human

    The reason chain is persisted onto the SAME decision row (gate2 column),
    so one decision row carries the full evidence of both gates.
    """
    now = now or datetime.now(UTC)
    action = proposal.get("action")
    confidence = proposal.get("confidence")

    full_context = dict(context or {})
    if root_cause is not None:
        full_context.setdefault("root_cause", root_cause)

    # Same per-entity lock as Gate-1 — the whole gate span is serialized
    state = await lock_entity_state(session, merchant_id, entity_id, entity_type)

    chain: list[dict] = []

    # 1. Closed enum — the model cannot invent an action
    if action not in LEGAL_ACTIONS:
        chain.append(
            reason_entry(
                "action_enum",
                VERDICT_DENY,
                detail=f"action {action!r} is outside the closed action space",
            )
        )
        verdict = aggregate_verdict(chain)
        await _persist_gate2(session, decision_id, proposal, chain)
        return GateResult(verdict=verdict, reason_chain=chain, status=state.status)

    # 2. State-machine legality
    legal_for_status = LEGAL_ACTIONS_BY_STATUS.get(state.status, set())
    if action not in legal_for_status:
        chain.append(
            reason_entry(
                "state_machine",
                VERDICT_DENY,
                detail=(
                    f"action {action} is not legal from status {state.status}; "
                    f"legal moves: {sorted(legal_for_status)}"
                ),
            )
        )

    # 3. Counter policy for the concrete action class
    version = await active_policy_version(session, entity_type)
    rules = await get_rules(session, entity_type, version)
    is_money = action in MONEY_ATTEMPT_ACTIONS
    is_contact = action in CUSTOMER_CONTACT_ACTIONS

    for rule in rules:
        if not rule_applies_to(rule, full_context):
            continue
        if rule.metric is None:
            # Context rules re-fire here too: a fraud-blocked entity does not
            # get a pass just because the model proposed something concrete
            chain.append(reason_entry(rule.rule_id, rule.verdict, detail="context rule matched"))
            continue

        relevant = (rule.metric.startswith("retry") and is_money) or (
            rule.metric.startswith("contacts") and is_contact
        )
        if not relevant:
            continue

        window = timedelta(seconds=rule.window_seconds or 0)
        observed = await get_windowed_count(
            session,
            merchant_id,
            entity_id,
            rule.metric,
            window,
            event_types=metric_event_types(rule.metric),
            now=now,
            as_of=as_of,
        )
        fired = observed >= rule.limit_value
        chain.append(
            reason_entry(
                rule.rule_id,
                rule.verdict if fired else VERDICT_ALLOW,
                metric=rule.metric,
                observed=observed,
                limit=rule.limit_value,
            )
        )

    # 3b. Hard-decline method gate (§6, Stripe lesson): a money attempt on a

    # 3b. Hard-decline method gate (§6, Stripe lesson): a money attempt on a
    # hard-declined method is guaranteed to fail AND burns retry budget —
    # it is only legal after a PaymentMethodChanged event unblocks the path.
    if is_money and full_context.get("root_cause") in HARD_DECLINE_ROOT_CAUSES:
        if not await has_new_method_since_last_failure(
            session, merchant_id=merchant_id, entity_id=entity_id
        ):
            chain.append(
                reason_entry(
                    "hard_decline_requires_new_method",
                    VERDICT_DENY,
                    detail=(
                        f"root_cause {full_context.get('root_cause')!r} is a hard "
                        f"decline: no PaymentMethodChanged since the last failure — "
                        f"retry is guaranteed to fail"
                    ),
                )
            )

    # 4. Do-Not-Contact / consent — frequency caps alone don't make "compliant" true
    if is_contact and full_context.get("dnc") is True:
        chain.append(
            reason_entry(
                "dnc_consent",
                VERDICT_DENY,
                detail="entity is on Do-Not-Contact / consent withheld",
            )
        )

    # 5. Confidence floor — low confidence routes to a human, here
    if isinstance(confidence, (int, float)) and confidence < CONFIDENCE_FLOOR:
        chain.append(
            reason_entry(
                "confidence_floor",
                VERDICT_REQUIRE_HUMAN,
                detail=f"confidence {confidence} below floor {CONFIDENCE_FLOOR}",
            )
        )

    verdict = aggregate_verdict(chain)
    await _persist_gate2(session, decision_id, proposal, chain)

    return GateResult(
        verdict=verdict,
        reason_chain=chain,
        policy_version=version,
        decision_id=decision_id,
        status=state.status,
    )


async def _persist_gate2(
    session: AsyncSession,
    decision_id: int,
    proposal: dict,
    chain: list[dict],
) -> None:
    """Write the Gate-2 chain onto the decision row created at Gate-1."""
    decision = await session.get(Decision, decision_id)
    if decision is not None:
        decision.proposal = proposal
        decision.gate2 = chain
        await session.flush()
