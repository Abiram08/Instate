"""Outbox execution: intent → gateway → commit.
Intent (idempotency key = source_event_id) is committed before any gateway call.
Dangling intents are reconciled on boot via reconcile.py.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse, PaymentGateway
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_AWAIT_PROMISE,
    ACTION_CHECK_METHOD_UPDATED,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_BACKUP_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    ALLOWED_CHANNELS,
    Decision,
    EntityState,
    HARD_DECLINE_ROOT_CAUSES,
    ScheduledAction,
)
from instate.core.projection import has_new_method_since_last_failure, infer_payday
from instate.core.locks import get_entity_lock


# Action → outcome event the fold understands
# Backup retry counts as a retry; method check is an observation, never an attempt.
OUTCOME_EVENT: dict[str, str] = {
    ACTION_RETRY_NOW: "RetryAttempted",
    ACTION_RETRY_SCHEDULED: "RetryScheduled",
    ACTION_SEND_PAYMENT_LINK: "PaymentLinkSent",
    ACTION_REQUEST_PAYMENT_METHOD: "PaymentLinkSent",
    ACTION_UPDATE_MANDATE: "RecoveryActionSent",
    ACTION_AWAIT_PROMISE: "RecoveryActionSent",
    ACTION_RETRY_BACKUP_METHOD: "RetryAttempted",
    ACTION_CHECK_METHOD_UPDATED: "MethodCheckCompleted",
}

# Channel recorded on CustomerContacted
ACTION_CHANNEL: dict[str, str] = {
    ACTION_SEND_PAYMENT_LINK: "payment_link",
    ACTION_REQUEST_PAYMENT_METHOD: "payment_link",
    ACTION_UPDATE_MANDATE: "mandate_update",
    ACTION_AWAIT_PROMISE: "message",
}


def make_idempotency_key(merchant_id: UUID, entity_id: str, decision_id: int | None) -> str:
    """Deterministic key from the decision; crash+reconcile reuses the same key."""
    return f"{merchant_id}:{entity_id}:d{decision_id or 0}"


def resolve_channel(action: str, proposal: dict | None) -> str | None:
    """Resolve contact channel; only allowlisted proposal values count, else the action default."""
    if proposal:
        candidate = proposal.get("channel")
        if isinstance(candidate, str) and candidate in ALLOWED_CHANNELS:
            return candidate
    return ACTION_CHANNEL.get(action)


# Proposal timing → due date for RETRY_SCHEDULED


def parse_timing(timing: str | None, now: datetime) -> timedelta:
    """Parse timing to timedelta; unknown values fall back to 24h. NEXT_PAYDAY is resolved in schedule_retry."""
    if not timing:
        return timedelta(hours=24)
    t = timing.strip().upper()
    if t == "IMMEDIATE":
        return timedelta(0)
    if t.startswith("T_PLUS_") and t.endswith("H"):
        try:
            return timedelta(hours=int(t[7:-1]))
        except ValueError:
            pass
    if t.startswith("T_PLUS_") and t.endswith("D"):
        try:
            return timedelta(days=int(t[7:-1]))
        except ValueError:
            pass
    return timedelta(hours=24)


# The outbox


async def write_intent(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    occurred_at: datetime | None = None,
) -> str:
    """Write intent durably before the gateway call. Key = source_event_id, so UNIQUE blocks double-intent."""
    occurred_at = occurred_at or datetime.now(UTC)
    key = make_idempotency_key(merchant_id, entity_id, decision.id)

    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="ActionIntended",
        occurred_at=occurred_at,
        payload={"action": action, "idempotency_key": key, "decision_id": decision.id},
        source_event_id=key,  # UNIQUE → double-intent is impossible
        decision_id=decision.id,
    )
    await session.commit()  # durable BEFORE the gateway call
    return key


async def execute_action(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    proposal: dict | None = None,
    now: datetime | None = None,
) -> GatewayResponse:
    """Intent → gateway → commit. "unknown" leaves the intent for reconciliation."""
    now = now or datetime.now(UTC)
    key = await write_intent(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision=decision,
        action=action,
        occurred_at=now,
    )

    response = await gateway.execute(
        action,
        entity_id=entity_id,
        idempotency_key=key,
        payload=(proposal or {}).get("payload"),
    )

    await commit_outcome(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision=decision,
        action=action,
        key=key,
        response=response,
        proposal=proposal,
        now=now,
    )
    return response


async def commit_outcome(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    key: str,
    response: GatewayResponse,
    proposal: dict | None = None,
    now: datetime | None = None,
) -> None:
    """Write completion outcome; "unknown" leaves the intent for reconciliation."""
    now = now or datetime.now(UTC)

    if response.status == "completed":
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="ActionCompleted",
            occurred_at=now,
            payload={
                "idempotency_key": key,
                "action": action,
                "provider_ref": response.provider_ref,
            },
            source_event_id=f"{key}:done",
            decision_id=decision.id,
        )
        await _write_outcome_events(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            action=action,
            decision=decision,
            occurred_at=now,
            success=True,
            amount_minor=response.amount_minor,
            proposal=proposal,
            converted=response.data.get("converted"),
        )
        # A method check finding a new method unblocks retries without burning an attempt.
        if action == ACTION_CHECK_METHOD_UPDATED and response.data.get("method_updated"):
            await record_event(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                event_type="PaymentMethodChanged",
                occurred_at=now,
                payload={"via": "method_check", "decision_id": decision.id},
                source_event_id=f"{key}:methodchanged",
                decision_id=decision.id,
            )
    elif response.status == "failed":
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="ActionFailed",
            occurred_at=now,
            payload={
                "idempotency_key": key,
                "action": action,
                "detail": response.detail,
            },
            source_event_id=f"{key}:failed",
            decision_id=decision.id,
        )
        await _write_outcome_events(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            action=action,
            decision=decision,
            occurred_at=now,
            success=False,
            proposal=proposal,
        )
    # "unknown": the intent stands; reconciliation resolves it

    decision.executed_action = action
    await session.commit()


async def _write_outcome_events(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    action: str,
    decision: Decision,
    occurred_at: datetime,
    success: bool,
    amount_minor: int | None = None,
    proposal: dict | None = None,
    converted: bool | None = None,
) -> None:
    key = make_idempotency_key(merchant_id, entity_id, decision.id)
    channel = resolve_channel(action, proposal)
    variant = (proposal or {}).get("variant") if isinstance(proposal, dict) else None
    variant = variant if isinstance(variant, str) and variant else None
    outcome_type = OUTCOME_EVENT.get(action)
    if outcome_type is not None:
        outcome_payload: dict = {
            "action": action,
            "success": success,
            "decision_id": decision.id,
        }
        # Variant rides on link events so conversion is measurable per variant.
        if variant is not None and outcome_type == "PaymentLinkSent":
            outcome_payload["variant"] = variant
        if converted is not None and outcome_type == "PaymentLinkSent":
            outcome_payload["converted"] = converted
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type=outcome_type,
            occurred_at=occurred_at,
            payload=outcome_payload,
            source_event_id=f"{key}:{outcome_type}",
            decision_id=decision.id,
            channel=channel if outcome_type == "PaymentLinkSent" else None,
        )
        # Successful retry amount rides on RetrySucceeded for net-money accounting.
        if success and action in (ACTION_RETRY_NOW, ACTION_RETRY_BACKUP_METHOD) and amount_minor is not None:
            await record_event(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                event_type="RetrySucceeded",
                occurred_at=occurred_at,
                payload={"amount_minor": amount_minor,
                          "decision_id": decision.id,
                          "via": "backup" if action == ACTION_RETRY_BACKUP_METHOD else "primary"},
                source_event_id=f"{key}:recovered",
                decision_id=decision.id,
            )

    if channel is not None:
        contact_payload: dict = {
            "channel": channel,
            "action": action,
            "decision_id": decision.id,
        }
        if variant is not None:
            contact_payload["variant"] = variant
        if converted is not None:
            contact_payload["converted"] = converted
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="CustomerContacted",
            occurred_at=occurred_at,
            payload=contact_payload,
            source_event_id=f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:contact",
            decision_id=decision.id,
            channel=channel,
        )


# Escalation — deterministic stop, no gateway call


async def escalate_to_human(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    reason: str,
    now: datetime | None = None,
) -> None:
    """Write EscalatedToHuman and mark the decision."""
    now = now or datetime.now(UTC)
    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="EscalatedToHuman",
        occurred_at=now,
        payload={"reason": reason, "decision_id": decision.id},
        source_event_id=f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:esc",
        decision_id=decision.id,
    )
    decision.executed_action = "ESCALATE_HUMAN"
    await session.commit()


# Scheduling — RETRY_SCHEDULED lands in the durable queue


# Indian bank holidays (IST subset — extend from RBI calendar). Shift: T-1, else T-3.
IN_BANK_HOLIDAYS = frozenset(
    {
        "2026-01-26",  # Republic Day
        "2026-08-15",  # Independence Day
        "2026-10-02",  # Gandhi Jayanti
        "2026-11-08",  # Diwali (illustrative)
        "2026-12-25",  # Christmas
        "2027-01-26",
        "2027-08-15",
        "2027-10-02",
        "2027-12-25",
    }
)


def apply_bank_holiday_shift(due_at: datetime) -> datetime:
    """If due date (IST) is a bank holiday, pull back to T-1; if that is one too, T-3."""
    day = due_at.date().isoformat()
    if day not in IN_BANK_HOLIDAYS:
        return due_at
    back1 = due_at - timedelta(days=1)
    if back1.date().isoformat() not in IN_BANK_HOLIDAYS:
        return back1
    return due_at - timedelta(days=3)


def clamp_to_local_morning(due_at: datetime, tz_name: str | None, hour: int = 10) -> datetime:
    """Clamp due time into customer local morning; 09:00–17:00 passes through, unknown tz untouched."""
    if not tz_name:
        return due_at
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name)
    except Exception:
        return due_at
    local = due_at.astimezone(tz)
    if 9 <= local.hour < 17:
        return due_at
    target = local.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return target.astimezone(UTC)


async def schedule_retry(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    timing: str | None,
    root_cause: str | None = None,
    now: datetime | None = None,
) -> ScheduledAction:
    """Queue RETRY_SCHEDULED; tick loop executes it as RETRY_NOW when due. NEXT_PAYDAY learns from history, then holiday shift + morning clamp."""
    now = now or datetime.now(UTC)
    if timing and timing.strip().upper() == "NEXT_PAYDAY":
        learned = await infer_payday(
            session, merchant_id=merchant_id, entity_id=entity_id, now=now
        )
        due_at = learned if learned is not None else now + timedelta(hours=48)
    else:
        due_at = now + parse_timing(timing, now)
    due_at = apply_bank_holiday_shift(due_at)
    state = await session.get(EntityState, (merchant_id, entity_id))
    due_at = clamp_to_local_morning(due_at, state.timezone if state else None)
    key = f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:sched"

    # Uniqueness is on idempotency_key — look up by key, never re-schedule
    result = await session.execute(
        select(ScheduledAction).where(ScheduledAction.idempotency_key == key)
    )
    existing = result.scalar_one_or_none()
    if existing is not None:
        return existing

    scheduled = ScheduledAction(
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        action=ACTION_RETRY_NOW,
        due_at=due_at,
        idempotency_key=key,
        decision_id=decision.id,
        payload={"timing": timing, "root_cause": root_cause},
    )
    session.add(scheduled)
    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="RetryScheduled",
        occurred_at=now,
        payload={"due_at": due_at.isoformat(), "timing": timing, "decision_id": decision.id},
        source_event_id=f"{key}:schedevent",
        decision_id=decision.id,
    )
    decision.executed_action = "RETRY_SCHEDULED"
    await session.commit()
    return scheduled


async def run_due_scheduled(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    now: datetime | None = None,
) -> int:
    """Execute every due scheduled action through the outbox (as RETRY_NOW)."""
    now = now or datetime.now(UTC)
    result = await session.execute(
        select(ScheduledAction).where(
            ScheduledAction.due_at <= now,
            ScheduledAction.executed_at.is_(None),
        )
    )
    due = list(result.scalars().all())

    executed = 0
    for scheduled in due:
        # Per-entity lock: concurrent pipelines for this entity wait here.
        async with get_entity_lock(scheduled.merchant_id, scheduled.entity_id):
            if await _run_one_scheduled(session, gateway=gateway, scheduled=scheduled, now=now):
                executed += 1
    if executed:
        # Outcome events change derived state — refold so L1 never goes stale
        # between worker steps (a stale L1 is what `instate rebuild` reports as drift).
        from instate.core.projection import fold_events

        await fold_events(session)
    return executed


async def _run_one_scheduled(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    scheduled: ScheduledAction,
    now: datetime,
) -> bool:
    """Execute one due row. Returns True if it fired (vs deferred/missing)."""
    decision = (
        await session.get(Decision, scheduled.decision_id)
        if scheduled.decision_id is not None
        else None
    )
    if decision is None:
        scheduled.executed_at = now
        await session.commit()
        return False

    # Hard declines stay queued until PaymentMethodChanged unblocks them.
    root_cause = (scheduled.payload or {}).get("root_cause")
    if root_cause in HARD_DECLINE_ROOT_CAUSES and not await (
        has_new_method_since_last_failure(
            session,
            merchant_id=scheduled.merchant_id,
            entity_id=scheduled.entity_id,
        )
    ):
        return False  # stays pending — the next tick re-checks

    await execute_action(
        session,
        gateway=gateway,
        merchant_id=scheduled.merchant_id,
        entity_id=scheduled.entity_id,
        entity_type=scheduled.entity_type,
        decision=decision,
        action=scheduled.action,  # RETRY_NOW
        proposal={"payload": scheduled.payload},
        now=now,
    )
    scheduled.executed_at = now
    await session.commit()
    return True
