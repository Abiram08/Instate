"""Instate L1 projection — pure fold over L0, rebuildable at any moment.

This is the "now" tier (§2 of architecture.md). It holds only what
folds cleanly: the state-machine position and point-in-time scalars.

Key design decisions:
- Windowed counters (retry_count_7d, contacts_24h) are NOT stored here.
  A fold over an append-only log cannot age out a 7-day window —
  caching them would drift by construction. Instead, gate-check
  computes them as indexed L0 counts (get_windowed_count below).
- The fold processes events in id order (recorded_at semantics).
  A late-arriving event (out-of-order occurred_at) appends and
  affects only FUTURE decisions; it never rewrites a past one.
- instate rebuild() drops L1, replays all of L0, and diffs —
  a ten-second proof that the ledger is complete and the derived
  state is honest.
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
    STATUS_RETRY_SCHEDULED,
    STATUS_AWAITING_PROMISE,
    STATUS_ESCALATED,
    STATUS_RECOVERED,
    STATUS_WRITTEN_OFF,
)


# ---------------------------------------------------------------------------
# State machine transitions
# ---------------------------------------------------------------------------

# Event type → status transition (the state machine from §6)
# This is the complete fold logic: each event maps to a state change.
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
    """Apply a single event to an EntityState (pure fold step).

    This is the ONLY function that mutates EntityState. It is called
    by fold_events() (incremental) and rebuild() (full replay).

    The fold is deterministic: same events in same order → same state.
    """
    # Status transition
    new_status = TRANSITIONS.get(event.event_type)
    if new_status is not None:
        state.status = new_status

    # Contact tracking
    if event.event_type in CONTACT_EVENTS:
        state.last_contact_at = event.occurred_at

    # Promise-to-pay tracking
    if event.event_type in PTP_SET_EVENTS and event.payload:
        due_at = event.payload.get("due_at")
        if due_at:
            if isinstance(due_at, str):
                from datetime import datetime as dt
                due_at = dt.fromisoformat(due_at)
            state.open_ptp_due_at = due_at
    elif event.event_type in PTP_CLEAR_EVENTS:
        state.open_ptp_due_at = None

    # Failure reason
    if event.event_type in FAILURE_REASON_EVENTS and event.payload:
        state.last_failure_reason = event.payload.get("root_cause") or event.payload.get(
            "failure_code"
        )

    # Amount at risk
    if event.event_type in AMOUNT_EVENTS and event.payload:
        amount = event.payload.get("amount_minor")
        if amount is not None and isinstance(amount, int):
            state.amount_at_risk_minor = amount

    # Watermark
    state.last_event_id = event.id

    return state


# ---------------------------------------------------------------------------
# fold_events — incremental update from watermark
# ---------------------------------------------------------------------------


async def fold_events(session: AsyncSession) -> int:
    """Incrementally fold new events into entity_state (from watermark).

    For each entity with events past its watermark:
    1. Fetch events after last_event_id
    2. Apply each in order (apply_event)
    3. Update the watermark

    Returns the number of events folded.
    """
    # Find all entities that have events past their watermark
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
        # Get or create the entity state
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

        # Skip if no new events
        if state.last_event_id >= max_event_id:
            continue

        # Fetch events past the watermark
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
    """Drop all of L1, replay all of L0, and report the diff.

    This is the ten-second proof that the ledger is complete and
    the derived state is honest. If rebuild produces different state
    than what was there before, the projection had drifted —
    and that's a bug worth knowing about.
    """
    # Snapshot current state for the diff
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

    # Drop all L1 rows
    await session.execute(delete(EntityState))
    await session.flush()

    # Replay all of L0
    events_folded = await fold_events(session)

    # Diff: compare new state against snapshot
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

    # Compare (a drifted projection would show differences).
    # An initial build (old_state_map empty) is not drift — it's the
    # first time the fold has run.
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
) -> int:
    """Compute a windowed count over L0 (indexed, sub-ms, never drifts).

    This is the gate-check primitive (§2 of architecture.md):
    windowed counters are deliberately NOT cached in L1 because a
    fold cannot age out a window. This count is always exactly correct
    because it queries the immutable ledger directly.

    `now` is injectable so window-edge tests can pin the clock.
    `as_of` is the KNOWLEDGE cutoff (§1b bi-temporal): when given, only
    events recorded at or before `as_of` count. It defaults to None
    (no cutoff — live checks see everything learned so far), so all
    existing callers are unaffected. Replaying a past decision passes
    `now=as_of=T`: the window anchors at T AND the knowledge cutoff sits
    at T, reproducing EXACTLY what was known at T — a late-arriving
    event (old occurred_at, new recorded_at) affects only future
    decisions, never rewrites a past one.
    """
    now = now or datetime.now(UTC)
    # Resolve the metric to event types
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

    count_result = await session.execute(
        select(func.count(Event.id)).where(*filters)
    )
    return count_result.scalar_one()


# ---------------------------------------------------------------------------
# Payment-method dimension (§6, Stripe lesson) — the hard-decline unblock
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
    """Does the entity have NEW-method evidence — a PaymentMethodChanged
    event that postdates its most recent failure?

    This is the gate the hard-decline rule consults: a retry for a
    hard-declined method is only legal once the customer has actually
    changed the method AFTER the failure that dead-ended it.

    - No PaymentMethodChanged on file at all → False (no evidence, no
      unblock — a hard decline stays blocked).
    - Method change but no failure → True (a fresh method is on file).
    - Otherwise: change must postdate the last failure.
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
