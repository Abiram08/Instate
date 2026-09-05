"""L0 ledger tests: hash chain, dedupe, append-only, verify."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import (
    DuplicateEventError,
    compute_event_hash,
    compute_payload_hash,
    get_event_by_source_id,
    get_timeline,
    record_event,
    redact_payload,
    verify_chain,
)
from tests.conftest import days_ago, make_merchant_id, now_utc


# ---------------------------------------------------------------------------
# record_event — the basics
# ---------------------------------------------------------------------------


async def test_record_event_creates_event(session: AsyncSession):
    """A single event is stored with all fields intact."""
    merchant = make_merchant_id()
    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_123",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"amount_minor": 500000, "failure_code": "insufficient_funds"},
        source_event_id="evt_wh_001",
    )
    await session.commit()

    assert event.id is not None
    assert event.entity_id == "sub_123"
    assert event.event_type == "PaymentFailed"
    assert event.payload["amount_minor"] == 500000
    assert event.source_event_id == "evt_wh_001"
    assert event.prev_hash is None  # genesis event (first for this entity)
    assert event.hash is not None
    assert len(event.hash) == 32  # sha256 = 32 bytes
    assert event.payload_hash is not None
    assert len(event.payload_hash) == 32


async def test_record_event_multiple_entities_no_contention(session: AsyncSession):
    """Events for different entities have independent chains (no global lock)."""
    merchant = make_merchant_id()

    # Two entities, interleaved events
    for i in range(3):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="entity_A",
            entity_type="payment",
            event_type="PaymentFailed" if i == 0 else "RetryAttempted",
            occurred_at=now_utc() - timedelta(hours=3 - i),
            source_event_id=f"evt_A_{i}",
        )
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="entity_B",
            entity_type="payment",
            event_type="PaymentFailed" if i == 0 else "RetryAttempted",
            occurred_at=now_utc() - timedelta(hours=3 - i),
            source_event_id=f"evt_B_{i}",
        )
    await session.commit()

    result_a = await verify_chain(session, merchant, "entity_A")
    result_b = await verify_chain(session, merchant, "entity_B")

    assert result_a.verified
    assert result_a.event_count == 3
    assert result_b.verified
    assert result_b.event_count == 3


async def test_timeline_as_of_pins_a_snapshot(session: AsyncSession):
    """Point-in-time read: only events recorded at/before as_of are visible."""
    merchant = make_merchant_id()
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 6, 1, tzinfo=UTC)
    e1 = await record_event(
        session, merchant_id=merchant, entity_id="sub_snap", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=t1,
        payload={"failure_code": "insufficient_funds"}, source_event_id="snap_1")
    e1.recorded_at = t1
    e2 = await record_event(
        session, merchant_id=merchant, entity_id="sub_snap", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=t2, source_event_id="snap_2")
    e2.recorded_at = t2
    await session.commit()

    early = await get_timeline(session, merchant, "sub_snap", as_of=datetime(2026, 3, 1, tzinfo=UTC))
    assert [e.id for e in early] == [e1.id]
    full = await get_timeline(session, merchant, "sub_snap")
    assert [e.id for e in full] == [e1.id, e2.id]


# ---------------------------------------------------------------------------
# Dedupe — exactly-once
# ---------------------------------------------------------------------------


async def test_dedupe_same_source_event_id_is_inert(session: AsyncSession):
    """A webhook redelivery with the same source_event_id is inert (no duplicate)."""
    merchant = make_merchant_id()

    event1 = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_123",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"amount_minor": 100000},
        source_event_id="evt_wh_dedupe_test",
    )
    await session.commit()

    with pytest.raises(DuplicateEventError):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_123",
            entity_type="subscription",
            event_type="PaymentFailed",
            occurred_at=now_utc(),
            payload={"amount_minor": 100000},
            source_event_id="evt_wh_dedupe_test",
        )
    await session.rollback()

    timeline = await get_timeline(session, merchant, "sub_123")
    assert len(timeline) == 1
    assert timeline[0].id == event1.id


async def test_dedupe_different_source_ids_both_stored(session: AsyncSession):
    """Different webhooks (different source_event_ids) are both stored."""
    merchant = make_merchant_id()

    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_123",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        source_event_id="evt_wh_001",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_123",
        entity_type="subscription",
        event_type="RetryAttempted",
        occurred_at=now_utc() + timedelta(minutes=1),
        source_event_id="evt_wh_002",
    )
    await session.commit()

    timeline = await get_timeline(session, merchant, "sub_123")
    assert len(timeline) == 2


async def test_no_source_event_id_allows_multiple(session: AsyncSession):
    """Events without source_event_id (internal events) have no dedupe constraint."""
    merchant = make_merchant_id()

    for i in range(3):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_123",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() + timedelta(minutes=i),
        )
    await session.commit()

    timeline = await get_timeline(session, merchant, "sub_123")
    assert len(timeline) == 3


# ---------------------------------------------------------------------------
# Per-entity hash chain
# ---------------------------------------------------------------------------


async def test_hash_chain_links_per_entity(session: AsyncSession):
    """Each event's prev_hash is the previous event's hash for the SAME entity."""
    merchant = make_merchant_id()

    events = []
    for i in range(5):
        event = await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_chain",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() + timedelta(minutes=i),
        )
        events.append(event)
    await session.commit()

    assert events[0].prev_hash is None

    for i in range(1, len(events)):
        assert events[i].prev_hash == events[i - 1].hash, (
            f"event {i}'s prev_hash should be event {i-1}'s hash"
        )


async def test_hash_chain_is_deterministic(session: AsyncSession):
    """Same inputs → same hash (the chain is reproducible)."""
    merchant = make_merchant_id()
    occurred = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_det",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=occurred,
        payload={"key": "value"},
    )
    await session.commit()

    payload_hash = compute_payload_hash({"key": "value"})
    expected = compute_event_hash(
        None,  # genesis
        merchant,
        "sub_det",
        "PaymentFailed",
        occurred,
        payload_hash,
    )
    assert event.hash == expected


async def test_verify_chain_valid(session: AsyncSession):
    """A clean chain verifies with zero breaks."""
    merchant = make_merchant_id()

    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_verify",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() + timedelta(minutes=i),
        )
    await session.commit()

    result = await verify_chain(session, merchant, "sub_verify")
    assert result.verified
    assert result.event_count == 4
    assert result.error is None


async def test_verify_chain_empty_entity(session: AsyncSession):
    """An entity with no events verifies trivially (empty chain)."""
    merchant = make_merchant_id()
    result = await verify_chain(session, merchant, "nonexistent")
    assert result.verified
    assert result.event_count == 0


# ---------------------------------------------------------------------------
# Redaction — PII ages out, chain still verifies
# ---------------------------------------------------------------------------


async def test_redact_payload_chain_still_verifies(session: AsyncSession):
    """Redacted payload still verifies; payload_hash is stored separately."""
    merchant = make_merchant_id()

    event1 = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_redact",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"customer_email": "john@example.com", "amount": 50000},
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_redact",
        entity_type="subscription",
        event_type="RecoveryActionSent",
        occurred_at=now_utc() + timedelta(hours=1),
        payload={"channel": "email"},
    )
    await session.commit()

    redacted = await redact_payload(session, event1.id)
    await session.commit()
    assert redacted

    result = await verify_chain(session, merchant, "sub_redact")
    assert result.verified, f"chain should verify after redaction, got: {result.error}"
    assert result.event_count == 2

    from sqlalchemy import select
    from instate.core.models import Event

    refreshed = await session.execute(select(Event).where(Event.id == event1.id))
    event1_db = refreshed.scalar_one()
    assert event1_db.payload is None
    assert event1_db.payload_hash is not None


# ---------------------------------------------------------------------------
# Bi-temporal — occurred_at vs recorded_at
# ---------------------------------------------------------------------------


async def test_bi_temporal_columns(session: AsyncSession):
    """occurred_at and recorded_at are stored separately."""
    merchant = make_merchant_id()
    occurred = days_ago(3)

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_bitemp",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=occurred,
    )
    await session.commit()

    assert event.occurred_at is not None
    time_diff = (event.recorded_at - event.occurred_at).total_seconds()
    assert time_diff > 2 * 24 * 3600


# ---------------------------------------------------------------------------
# Timeline reads
# ---------------------------------------------------------------------------


async def test_get_timeline_ordered_oldest_first(session: AsyncSession):
    """Timeline returns events in chronological order."""
    merchant = make_merchant_id()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_tl", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=now_utc() + timedelta(hours=2),
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_tl", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now_utc(),
    )
    await record_event(
        session, merchant_id=merchant, entity_id="sub_tl", entity_type="subscription",
        event_type="RecoveryActionSent", occurred_at=now_utc() + timedelta(hours=1),
    )
    await session.commit()

    timeline = await get_timeline(session, merchant, "sub_tl")
    assert len(timeline) == 3
    assert timeline[0].event_type == "PaymentFailed"
    assert timeline[1].event_type == "RecoveryActionSent"
    assert timeline[2].event_type == "RetryAttempted"


async def test_get_timeline_limit_poka_yoke(session: AsyncSession):
    """Timeline limit bounds the read."""
    merchant = make_merchant_id()

    for i in range(20):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_limit", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=now_utc() + timedelta(minutes=i),
        )
    await session.commit()

    timeline = await get_timeline(session, merchant, "sub_limit", limit=5)
    assert len(timeline) == 5


async def test_get_event_by_source_id(session: AsyncSession):
    """Fetch an event by its webhook source_event_id."""
    merchant = make_merchant_id()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_src", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now_utc(),
        source_event_id="evt_lookup_test",
    )
    await session.commit()

    found = await get_event_by_source_id(session, "evt_lookup_test")
    assert found is not None
    assert found.entity_id == "sub_src"

    not_found = await get_event_by_source_id(session, "evt_nonexistent")
    assert not_found is None
