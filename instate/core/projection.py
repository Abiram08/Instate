"""L1 projection: pure fold of L0 into current entity state (§2).

Windowed counts are computed as indexed L0 counts, never cached.
Fold and rebuild walk events in insertion (id) order.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    Event,
    EntityState,
    STATUS_ACTIVE,
    STATUS_DIAGNOSED,
    STATUS_PAUSED,
    STATUS_RETRY_SCHEDULED,
    STATUS_AWAITING_PROMISE,
    STATUS_ESCALATED,
    STATUS_RECOVERED,
    STATUS_WRITTEN_OFF,
)


# ---------------------------------------------------------------------------
# State machine transitions
# ---------------------------------------------------------------------------

# Event type → status transition (§6).
TRANSITIONS: dict[str, str | None] = {
    # Failure events
    "PaymentFailed": STATUS_DIAGNOSED,
    "FailureDiagnosed": STATUS_DIAGNOSED,
    # Retry events
    "RetryAttempted": STATUS_RETRY_SCHEDULED,
    "RetryScheduled": STATUS_RETRY_SCHEDULED,
    "RetrySucceeded": STATUS_RECOVERED,
    "RetryFailed": STATUS_DIAGNOSED,
    # Payment method events
    "PaymentMethodChanged": STATUS_DIAGNOSED,  # unblocks retry path
    # Recovery actions
    "RecoveryActionSent": None,  # no status change; sets last_contact_at
    "PaymentLinkSent": None,
    # Promise-to-pay aggregate
    "PromiseMade": STATUS_AWAITING_PROMISE,
    "PromiseHonored": STATUS_RECOVERED,
    "PromiseBroken": STATUS_DIAGNOSED,
    "PromiseReminderSent": None,
    # Escalation
    "EscalatedToHuman": STATUS_ESCALATED,
    "HumanResolved": STATUS_RECOVERED,
    # Terminal
    "PaymentRecovered": STATUS_RECOVERED,
    "RecoveryReversed": STATUS_DIAGNOSED,  # refund/chargeback after recovery
    "WrittenOff": STATUS_WRITTEN_OFF,
    # Contact tracking (no status change)
    "CustomerContacted": None,
    # Pause lifecycle — parked, not dead (reactivation is one event)
    "SubscriptionPaused": STATUS_PAUSED,
    "SubscriptionResumed": STATUS_DIAGNOSED,
    # Prevention + method-check observations (no status change)
    "CardExpiring": None,
    "MethodCheckCompleted": None,
    # Checkout/invoice lifecycle (thin consumers)
    "CheckoutAbandoned": STATUS_DIAGNOSED,
    "InvoiceOverdue": STATUS_DIAGNOSED,
}

# Events that set last_contact_at (for contact-frequency caps)
CONTACT_EVENTS = {"CustomerContacted", "RecoveryActionSent", "PaymentLinkSent"}

# Events that set open_ptp_due_at
PTP_SET_EVENTS = {"PromiseMade"}
PTP_CLEAR_EVENTS = {"PromiseHonored", "PromiseBroken"}

# Events that set last_failure_reason
FAILURE_REASON_EVENTS = {"PaymentFailed", "FailureDiagnosed"}

# Events that set amount_at_risk_minor
AMOUNT_EVENTS = {"PaymentFailed", "InvoiceOverdue", "CheckoutAbandoned"}


# ---------------------------------------------------------------------------
# Fold — apply an event to entity state
# ---------------------------------------------------------------------------


def apply_event(state: EntityState, event: Event) -> EntityState:
    """Apply one event to EntityState; deterministic for the same order."""
    new_status = TRANSITIONS.get(event.event_type)
    if new_status is not None:
        state.status = new_status

    if event.event_type in CONTACT_EVENTS:
        state.last_contact_at = event.occurred_at

    if event.event_type in PTP_SET_EVENTS and event.payload:
        due_at = event.payload.get("due_at")
        if due_at:
            if isinstance(due_at, str):
                from datetime import datetime as dt
                due_at = dt.fromisoformat(due_at)
            state.open_ptp_due_at = due_at
    elif event.event_type in PTP_CLEAR_EVENTS:
        state.open_ptp_due_at = None

    if event.event_type in FAILURE_REASON_EVENTS and event.payload:
        state.last_failure_reason = event.payload.get("root_cause") or event.payload.get(
            "failure_code"
        )

    if event.event_type in AMOUNT_EVENTS and event.payload:
        amount = event.payload.get("amount_minor")
        if amount is not None and isinstance(amount, int):
            state.amount_at_risk_minor = amount

    # Store only IANA timezones zoneinfo accepts; bad values are ignored.
    if event.event_type == "PaymentFailed" and event.payload:
        tz_name = event.payload.get("customer_tz")
        if isinstance(tz_name, str):
            try:
                from zoneinfo import ZoneInfo

                ZoneInfo(tz_name)
                state.timezone = tz_name
            except Exception:
                pass

    # Partial card-expiry data is ignored, never stored half-true.
    if event.event_type == "CardExpiring" and event.payload:
        year = event.payload.get("exp_year")
        month = event.payload.get("exp_month")
        if (
            isinstance(year, int)
            and isinstance(month, int)
            and 1 <= month <= 12
        ):
            state.card_exp_year = year
            state.card_exp_month = month

    state.last_event_id = event.id

    return state


# ---------------------------------------------------------------------------
# fold_events — incremental update from watermark
# ---------------------------------------------------------------------------


async def fold_events(session: AsyncSession) -> int:
    """Fold events past each entity's watermark; returns events folded."""
    entities_needing_fold = await session.execute(
        select(Event.merchant_id, Event.entity_id, func.max(Event.id).label("max_id"))
        .group_by(Event.merchant_id, Event.entity_id)
    )
    entity_ids: list[tuple[UUID, str, int]] = [
        (row.merchant_id, row.entity_id, row.max_id)
        for row in entities_needing_fold
    ]

    folded_count = 0

    for merchant_id, entity_id, max_event_id in entity_ids:
        state = await session.get(EntityState, (merchant_id, entity_id))
        if state is None:
            first_event = await session.execute(
                select(Event)
                .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
                .order_by(Event.id.asc())
                .limit(1)
            )
            first = first_event.scalar_one_or_none()
            state = EntityState(
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=first.entity_type if first else "payment",
                status=STATUS_ACTIVE,
                last_event_id=0,
            )
            session.add(state)
            await session.flush()

        if state.last_event_id >= max_event_id:
            continue

        new_events = await session.execute(
            select(Event)
            .where(
                Event.merchant_id == merchant_id,
                Event.entity_id == entity_id,
                Event.id > state.last_event_id,
            )
            .order_by(Event.id.asc())
        )

        for event in new_events.scalars():
            apply_event(state, event)
            folded_count += 1

        await session.flush()

    return folded_count


# ---------------------------------------------------------------------------
# rebuild — drop L1, replay L0, diff
# ---------------------------------------------------------------------------


async def rebuild(session: AsyncSession) -> dict[str, int]:
    """Drop L1, replay L0, and report drift (mismatches = projection bug)."""
    old_states = await session.execute(select(EntityState))
    old_state_map: dict[tuple[UUID, str], dict[str, Any]] = {
        (s.merchant_id, s.entity_id): {
            "status": s.status,
            "last_contact_at": s.last_contact_at,
            "last_failure_reason": s.last_failure_reason,
            "open_ptp_due_at": s.open_ptp_due_at,
            "amount_at_risk_minor": s.amount_at_risk_minor,
            "last_event_id": s.last_event_id,
        }
        for s in old_states.scalars()
    }

    await session.execute(delete(EntityState))
    await session.flush()

    events_folded = await fold_events(session)

    new_states = await session.execute(select(EntityState))
    new_state_map: dict[tuple[UUID, str], dict[str, Any]] = {
        (s.merchant_id, s.entity_id): {
            "status": s.status,
            "last_contact_at": s.last_contact_at,
            "last_failure_reason": s.last_failure_reason,
            "open_ptp_due_at": s.open_ptp_due_at,
            "amount_at_risk_minor": s.amount_at_risk_minor,
            "last_event_id": s.last_event_id,
        }
        for s in new_states.scalars()
    }

    matches = 0
    mismatches = 0
    initial_build = not old_state_map
    for key, new_vals in new_state_map.items():
        old_vals = old_state_map.get(key)
        if old_vals is None:
            if not initial_build:
                mismatches += 1  # entity appeared unexpectedly
            else:
                matches += 1  # first build: every entity is new, and correct
        elif old_vals != new_vals:
            mismatches += 1  # state drifted
        else:
            matches += 1

    for key in old_state_map:
        if key not in new_state_map:
            mismatches += 1  # entity disappeared (shouldn't happen)

    return {
        "events_folded": events_folded,
        "entities": len(new_state_map),
        "matches": matches,
        "mismatches": mismatches,
        "drift_detected": mismatches > 0,
    }


# ---------------------------------------------------------------------------
# get_windowed_count — indexed L0 count (the gate-check primitive)
# ---------------------------------------------------------------------------


# Event types that count as "retries" for retry_count_7d
RETRY_EVENT_TYPES = {"RetryAttempted", "RetryScheduled", "RetryNow"}

# Event types that count as "contacts" for contacts_24h
CONTACT_EVENT_TYPES = {"CustomerContacted", "RecoveryActionSent", "PaymentLinkSent"}


async def get_windowed_count(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
    metric: str,  # "retry_count_7d" | "contacts_24h" | custom event type
    window: timedelta,
    *,
    event_types: set[str] | None = None,
    now: datetime | None = None,
    as_of: datetime | None = None,
    channel: str | None = None,
) -> int:
    """Indexed L0 count over a window; never cached, never drifts.

    `as_of` is the knowledge cutoff (replay passes now=as_of=T); `channel`
    filters the indexed Event.channel column, never the payload.
    """
    now = now or datetime.now(UTC)
    if event_types is None:
        if metric == "retry_count_7d":
            event_types = RETRY_EVENT_TYPES
        elif metric == "contacts_24h":
            event_types = CONTACT_EVENT_TYPES
        else:
            # Custom metric: treat the metric name as an event type
            event_types = {metric}

    filters = [
        Event.merchant_id == merchant_id,
        Event.entity_id == entity_id,
        Event.event_type.in_(event_types),
        Event.occurred_at > now - window,
    ]
    if as_of is not None:
        filters.append(Event.recorded_at <= as_of)
    if channel is not None:
        filters.append(Event.channel == channel)

    count_result = await session.execute(
        select(func.count(Event.id)).where(*filters)
    )
    return count_result.scalar_one()


# ---------------------------------------------------------------------------
# Payment-method dimension (§6): the hard-decline unblock
# ---------------------------------------------------------------------------


async def payment_method_changed_since(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    since: datetime,
    now: datetime | None = None,
) -> bool:
    """True if a PaymentMethodChanged event exists after `since`."""
    result = await session.execute(
        select(func.count(Event.id)).where(
            Event.merchant_id == merchant_id,
            Event.entity_id == entity_id,
            Event.event_type == "PaymentMethodChanged",
            Event.occurred_at > since,
        )
    )
    return result.scalar_one() > 0


async def has_new_method_since_last_failure(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    now: datetime | None = None,
) -> bool:
    """True if a PaymentMethodChanged postdates the last PaymentFailed.

    No change on file → False; change with no failure → True.
    """
    failure_q = await session.execute(
        select(Event.occurred_at)
        .where(
            Event.merchant_id == merchant_id,
            Event.entity_id == entity_id,
            Event.event_type == "PaymentFailed",
        )
        .order_by(Event.occurred_at.desc())
        .limit(1)
    )
    last_failure = failure_q.scalar_one_or_none()

    change_q = await session.execute(
        select(Event.occurred_at)
        .where(
            Event.merchant_id == merchant_id,
            Event.entity_id == entity_id,
            Event.event_type == "PaymentMethodChanged",
        )
        .order_by(Event.occurred_at.desc())
        .limit(1)
    )
    last_change = change_q.scalar_one_or_none()

    if last_change is None:
        return False  # no evidence — the hard-decline block stands
    if last_failure is None:
        return True  # fresh method on file, nothing dead-ended
    return last_change > last_failure


# ---------------------------------------------------------------------------
# Payday inference — learned timing, not guessed (§6, Stripe lesson)
# ---------------------------------------------------------------------------

# Event types whose occurred_at marks "money was here" — the payday signal.
SUCCESS_EVENT_TYPES = {"RetrySucceeded", "PaymentRecovered", "PromiseHonored"}


async def infer_payday(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    now: datetime | None = None,
) -> datetime | None:
    """Next likely payday (day-of-month cluster, ≥2 hits, 10:00 UTC) or None.

    Sparse history returns None; caller falls back to T+48H.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=90)
    result = await session.execute(
        select(Event.occurred_at).where(
            Event.merchant_id == merchant_id,
            Event.entity_id == entity_id,
            Event.event_type.in_(SUCCESS_EVENT_TYPES),
            Event.occurred_at > cutoff,
            Event.occurred_at <= now,
        )
    )
    doms = sorted(ts.day for ts in (r[0] for r in result.all()) if ts is not None)
    if len(doms) < 2:
        return None

    # ±1-day buckets over events (not distinct days); month edges wrap.
    best_day: int | None = None
    for anchor in doms:
        bucket = [d for d in doms if abs(d - anchor) <= 1 or abs(d - anchor) >= 27]
        if len(bucket) >= 2:
            best_day = anchor
            break
    if best_day is None:
        return None

    # Next occurrence at 10:00 UTC; today counts if morning hasn't passed.
    import calendar

    def _payday(year: int, month: int) -> datetime:
        last_day = calendar.monthrange(year, month)[1]
        return datetime(year, month, min(best_day, last_day), 10, 0, tzinfo=UTC)

    candidate = _payday(now.year, now.month)
    if candidate <= now:
        month = now.month + 1
        year = now.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        candidate = _payday(year, month)
    return candidate
