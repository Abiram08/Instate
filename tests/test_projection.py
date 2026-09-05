"""L1 projection tests: fold, rebuild, and windowed counts."""

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import EntityState
from instate.core.projection import (
    fold_events,
    get_windowed_count,
    rebuild,
)
from tests.conftest import days_ago, hours_ago, make_merchant_id, now_utc


# ---------------------------------------------------------------------------
# fold_events — the incremental fold
# ---------------------------------------------------------------------------


async def test_fold_creates_entity_state(session: AsyncSession):
    """Folding events creates an entity_state row."""
    merchant = make_merchant_id()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_fold", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now_utc(),
        payload={"amount_minor": 50000, "root_cause": "insufficient_funds"},
    )
    await session.commit()

    folded = await fold_events(session)
    await session.commit()
    assert folded == 1

    state = await session.get(EntityState, (merchant, "sub_fold"))
    assert state is not None
    assert state.status == "DIAGNOSED"
    assert state.last_failure_reason == "insufficient_funds"
    assert state.amount_at_risk_minor == 50000
    assert state.last_event_id > 0


async def test_fold_processes_state_transitions(session: AsyncSession):
    """The fold walks the full state machine: DIAGNOSED → RETRY_SCHEDULED → RECOVERED."""
    merchant = make_merchant_id()
    base = now_utc()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_sm", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=base,
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_sm", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=base + timedelta(hours=1),
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_sm", entity_type="subscription",
        event_type="RetrySucceeded", occurred_at=base + timedelta(hours=2),
    )
    await session.commit()

    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_sm"))
    assert state.status == "RECOVERED"


async def test_fold_promise_lifecycle(session: AsyncSession):
    """Promise-to-pay: PromiseMade → AWAITING_PROMISE, PromiseHonored → RECOVERED."""
    merchant = make_merchant_id()
    base = now_utc()
    ptp_due = base + timedelta(days=3)

    await record_event(
        session, merchant_id=merchant, entity_id="sub_ptp", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=base,
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_ptp", entity_type="subscription",
        event_type="PromiseMade", occurred_at=base + timedelta(hours=1),
        payload={"due_at": ptp_due.isoformat()},
    )
    await session.commit()

    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_ptp"))
    assert state.status == "AWAITING_PROMISE"
    assert state.open_ptp_due_at is not None

    await record_event(
        session, merchant_id=merchant, entity_id="sub_ptp", entity_type="subscription",
        event_type="PromiseHonored", occurred_at=base + timedelta(days=3),
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_ptp"))
    assert state.status == "RECOVERED"
    assert state.open_ptp_due_at is None


async def test_fold_contacts_set_last_contact_at(session: AsyncSession):
    """CustomerContacted and RecoveryActionSent set last_contact_at."""
    merchant = make_merchant_id()
    base = now_utc()
    contact_time = base + timedelta(hours=2)

    await record_event(
        session, merchant_id=merchant, entity_id="sub_contact", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=base,
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_contact", entity_type="subscription",
        event_type="CustomerContacted", occurred_at=contact_time,
        payload={"channel": "email"},
    )
    await session.commit()

    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_contact"))
    assert state.last_contact_at is not None
    assert abs((state.last_contact_at - contact_time).total_seconds()) < 1


async def test_fold_is_incremental(session: AsyncSession):
    """fold_events only processes events past the watermark (incremental)."""
    merchant = make_merchant_id()
    base = now_utc()

    for i in range(3):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_incr", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=base + timedelta(minutes=i),
        )
    await session.commit()

    folded1 = await fold_events(session)
    await session.commit()
    assert folded1 == 3

    state1 = await session.get(EntityState, (merchant, "sub_incr"))
    watermark1 = state1.last_event_id

    for i in range(3, 5):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_incr", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=base + timedelta(minutes=i),
        )
    await session.commit()

    folded2 = await fold_events(session)
    await session.commit()
    assert folded2 == 2

    state2 = await session.get(EntityState, (merchant, "sub_incr"))
    assert state2.last_event_id > watermark1


# ---------------------------------------------------------------------------
# rebuild — drop L1, replay L0, diff
# ---------------------------------------------------------------------------


async def test_rebuild_produces_identical_state(session: AsyncSession):
    """rebuild drops L1, replays L0, and produces the same state (no drift)."""
    merchant = make_merchant_id()
    base = now_utc()

    events_spec = [
        ("PaymentFailed", 0, {"amount_minor": 50000, "root_cause": "card_expired"}),
        ("FailureDiagnosed", 1, {"root_cause": "card_expired"}),
        ("CustomerContacted", 2, {"channel": "email"}),
        ("PaymentMethodChanged", 5, {"new_method": "upi"}),
        ("RetryAttempted", 6, {}),
        ("RetrySucceeded", 7, {}),
    ]
    for event_type, hours_offset, payload in events_spec:
        await record_event(
            session, merchant_id=merchant, entity_id="sub_rb", entity_type="subscription",
            event_type=event_type, occurred_at=base + timedelta(hours=hours_offset),
            payload=payload or None,
        )
    await session.commit()

    await fold_events(session)
    await session.commit()

    result = await rebuild(session)
    await session.commit()

    assert result["events_folded"] == 6
    assert result["entities"] == 1
    assert result["matches"] == 1
    assert result["mismatches"] == 0
    assert result["drift_detected"] is False


async def test_rebuild_on_empty_ledger(session: AsyncSession):
    """rebuild on an empty ledger is a no-op (zero events, zero entities)."""
    result = await rebuild(session)
    await session.commit()

    assert result["events_folded"] == 0
    assert result["entities"] == 0
    assert result["drift_detected"] is False


# ---------------------------------------------------------------------------
# get_windowed_count — the gate-check primitive
# ---------------------------------------------------------------------------


async def test_windowed_count_within_window(session: AsyncSession):
    """Events within the window are counted."""
    merchant = make_merchant_id()

    for i in range(3):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_wc", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=hours_ago(i + 1),
        )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_wc", "retry_count_7d", timedelta(days=7)
    )
    assert count == 3


async def test_windowed_count_excludes_old_events(session: AsyncSession):
    """Events outside the window are NOT counted (the window ages out)."""
    merchant = make_merchant_id()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc2", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=hours_ago(12),
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc2", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=days_ago(2),
    )
    # 10 days ago: outside the 7-day window.
    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc2", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=days_ago(10),
    )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_wc2", "retry_count_7d", timedelta(days=7)
    )
    assert count == 2


async def test_windowed_count_contacts_24h(session: AsyncSession):
    """contacts_24h counts contact events within 24 hours."""
    merchant = make_merchant_id()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc3", entity_type="subscription",
        event_type="CustomerContacted", occurred_at=hours_ago(3),
        payload={"channel": "email"},
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc3", entity_type="subscription",
        event_type="PaymentLinkSent", occurred_at=hours_ago(10),
    )
    # 2 days ago: outside the 24h window.
    await record_event(
        session, merchant_id=merchant, entity_id="sub_wc3", entity_type="subscription",
        event_type="CustomerContacted", occurred_at=days_ago(2),
        payload={"channel": "sms"},
    )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_wc3", "contacts_24h", timedelta(hours=24)
    )
    assert count == 2


# ---------------------------------------------------------------------------
# Stopping-rule boundaries
# ---------------------------------------------------------------------------


async def test_stopping_rule_boundary_at_limit(session: AsyncSession):
    """Exactly at the limit (3 retries in 7d) → count == 3 → Gate-1 would DENY."""
    merchant = make_merchant_id()

    for i in range(3):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_boundary", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=days_ago(i + 1),
        )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_boundary", "retry_count_7d", timedelta(days=7)
    )
    assert count == 3


async def test_stopping_rule_boundary_below_limit(session: AsyncSession):
    """One below the limit (2 retries in 7d) → count == 2 → Gate-1 would ALLOW."""
    merchant = make_merchant_id()

    for i in range(2):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_boundary2", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=days_ago(i + 1),
        )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_boundary2", "retry_count_7d", timedelta(days=7)
    )
    assert count == 2


async def test_stopping_rule_boundary_exactly_at_window_edge(session: AsyncSession):
    """An event at exactly the window edge — the boundary of the boundary."""
    merchant = make_merchant_id()

    # Just inside the 7-day window.
    await record_event(
        session, merchant_id=merchant, entity_id="sub_edge", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=days_ago(7) + timedelta(minutes=1),
    )
    await session.commit()

    count = await get_windowed_count(
        session, merchant, "sub_edge", "retry_count_7d", timedelta(days=7)
    )
    assert count == 1


async def test_stopping_rule_dedupe_doesnt_double_count(session: AsyncSession):
    """A redelivered webhook (dedupe) doesn't double-count in the window."""
    merchant = make_merchant_id()
    from instate.core.ledger import DuplicateEventError

    await record_event(
        session, merchant_id=merchant, entity_id="sub_dbl", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=hours_ago(1),
        source_event_id="evt_boundary_dedupe",
    )
    await session.commit()

    with pytest.raises(DuplicateEventError):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_dbl", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=hours_ago(1),
            source_event_id="evt_boundary_dedupe",
        )
    await session.rollback()

    count = await get_windowed_count(
        session, merchant, "sub_dbl", "retry_count_7d", timedelta(days=7)
    )
    assert count == 1


# ---------------------------------------------------------------------------
# Multiple entities — isolation
# ---------------------------------------------------------------------------


async def test_windowed_count_entity_isolation(session: AsyncSession):
    """Counts are per-entity — entity A's retries don't count for entity B."""
    merchant = make_merchant_id()

    for i in range(3):
        await record_event(
            session, merchant_id=merchant, entity_id="entity_A", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=hours_ago(i + 1),
        )
    await record_event(
        session, merchant_id=merchant, entity_id="entity_B", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=hours_ago(1),
    )
    await session.commit()

    count_a = await get_windowed_count(
        session, merchant, "entity_A", "retry_count_7d", timedelta(days=7)
    )
    count_b = await get_windowed_count(
        session, merchant, "entity_B", "retry_count_7d", timedelta(days=7)
    )
    assert count_a == 3
    assert count_b == 1


async def test_fold_multiple_entities(session: AsyncSession):
    """Fold handles multiple entities in a single pass."""
    merchant = make_merchant_id()
    base = now_utc()

    for entity in ["e1", "e2", "e3"]:
        await record_event(
            session, merchant_id=merchant, entity_id=entity, entity_type="subscription",
            event_type="PaymentFailed", occurred_at=base,
        )
        await record_event(
            session, merchant_id=merchant, entity_id=entity, entity_type="subscription",
            event_type="RetryAttempted", occurred_at=base + timedelta(hours=1),
        )
    await session.commit()

    folded = await fold_events(session)
    await session.commit()
    assert folded == 6

    for entity in ["e1", "e2", "e3"]:
        state = await session.get(EntityState, (merchant, entity))
        assert state is not None
        assert state.status == "RETRY_SCHEDULED"
