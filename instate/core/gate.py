"""Deterministic gates returning reason-chain evidence, persisted on decisions (§5).

Gate-1 checks the action class (zero tokens on DENY); Gate-2 checks the
concrete proposal. Gate→intent spans hold the per-entity lock (§6).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    ACTION_CHECK_METHOD_UPDATED,
    ACTION_RETRY_BACKUP_METHOD,
    CONFIDENCE_FLOOR,
    CUSTOMER_CONTACT_ACTIONS,
    EntityState,
    Decision,
    HARD_DECLINE_ROOT_CAUSES,
    MONEY_ATTEMPT_ACTIONS,
    STATUS_ACTIVE,
    STATUS_PAUSED,
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
    """DENY beats REQUIRE_HUMAN beats ALLOW."""
    verdicts = {entry["verdict"] for entry in chain}
    if VERDICT_DENY in verdicts:
        return VERDICT_DENY
    if VERDICT_REQUIRE_HUMAN in verdicts:
        return VERDICT_REQUIRE_HUMAN
    return VERDICT_ALLOW


# ---------------------------------------------------------------------------
# State-machine legality — the model only chooses among legal moves (§6)
# ---------------------------------------------------------------------------

# Legal action classes per state-machine position (§6).
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
        ACTION_RETRY_BACKUP_METHOD,
        ACTION_CHECK_METHOD_UPDATED,
    },
    "AWAITING_PROMISE": {
        ACTION_AWAIT_PROMISE,  # reminder path
        ACTION_SEND_PAYMENT_LINK,
        ACTION_ESCALATE_HUMAN,
    },
    "ESCALATED": {ACTION_ESCALATE_HUMAN},  # a human owns it now
    "RECOVERED": set(),  # done — nothing to decide
    "WRITTEN_OFF": set(),  # done — nothing to decide
    # Paused, not dead: only human-owned or promise moves are legal.
    # Money attempts are stopped earlier by the paused_no_autoretry rule.
    "PAUSED": {ACTION_ESCALATE_HUMAN, ACTION_AWAIT_PROMISE},
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
    """SELECT ... FOR UPDATE on the entity row (get-or-create ACTIVE).

    Layer 1 of TOCTOU defense; layer 2 is core.locks.get_entity_lock,
    which is what serializes pipelines on SQLite (§6).
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
    """Gate-1: may this entity take this action class at all?

    Holds the per-entity lock; counter metrics are indexed L0 counts.
    `as_of` pins the knowledge cutoff for replay; live callers omit it.
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

    is_money = action_class in MONEY_ATTEMPT_ACTIONS
    is_contact = action_class in CUSTOMER_CONTACT_ACTIONS

    chain: list[dict] = []

    # Paused entities don't auto-act on money; the rest stop here.
    if state.status == STATUS_PAUSED and is_money:
        chain.append(
            reason_entry(
                "paused_no_autoretry",
                VERDICT_DENY,
                detail=f"entity is PAUSED since inattention — {action_class} needs a human or a resume",
            )
        )
    for rule in rules:
        if not rule_applies_to(rule, full_context):
            continue

        if rule.metric is None:
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
    """Gate-2: is this concrete proposal legal now at this confidence?

    Re-checks enum, state machine, counter policy, DNC, and confidence
    floor; persists the chain onto the Gate-1 decision row.
    """
    now = now or datetime.now(UTC)
    action = proposal.get("action")
    confidence = proposal.get("confidence")

    full_context = dict(context or {})
    if root_cause is not None:
        full_context.setdefault("root_cause", root_cause)

    state = await lock_entity_state(session, merchant_id, entity_id, entity_type)

    chain: list[dict] = []

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

    version = await active_policy_version(session, entity_type)
    rules = await get_rules(session, entity_type, version)
    is_money = action in MONEY_ATTEMPT_ACTIONS
    is_contact = action in CUSTOMER_CONTACT_ACTIONS
    # Only allowlisted channels count; anything else falls back to global.
    from instate.core.models import ALLOWED_CHANNELS

    raw_channel = proposal.get("channel")
    channel = raw_channel if raw_channel in ALLOWED_CHANNELS else None

    for rule in rules:
        if not rule_applies_to(rule, full_context):
            continue
        if rule.metric is None:
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
            channel=channel if is_contact and rule.metric.startswith("contacts") else None,
        )
        fired = observed >= rule.limit_value
        chain.append(
            reason_entry(
                rule.rule_id,
                rule.verdict if fired else VERDICT_ALLOW,
                metric=rule.metric,
                observed=observed,
                limit=rule.limit_value,
                detail=f"channel={channel}" if channel and is_contact else None,
            )
        )

    # Hard-decline retry needs a PaymentMethodChanged since the last failure.
    # Exempt: RETRY_BACKUP_METHOD charges a different instrument.
    if (
        is_money
        and action != ACTION_RETRY_BACKUP_METHOD
        and full_context.get("root_cause") in HARD_DECLINE_ROOT_CAUSES
    ):
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
