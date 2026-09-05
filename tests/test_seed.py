"""Seed generator determinism, volume, and coverage."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Event
from instate.core.projection import rebuild
from instate.seed.generate import generate_failure_batch, seed_history
from tests.conftest import make_merchant_id


async def _events(session, merchant):
    result = await session.execute(
        select(Event).where(Event.merchant_id == merchant).order_by(Event.id.asc())
    )
    return list(result.scalars().all())


async def test_seed_history_is_deterministic(session: AsyncSession):
    m1, m2 = make_merchant_id(), make_merchant_id()
    s1 = await seed_history(session, merchant_id=m1, entities=10, seed=42)
    s2 = await seed_history(session, merchant_id=m2, entities=10, seed=42)

    assert s1 == s2
    seq1 = [(e.entity_id, e.event_type) for e in await _events(session, m1)]
    seq2 = [(e.entity_id, e.event_type) for e in await _events(session, m2)]
    assert seq1 == seq2


async def test_seed_history_volume(session: AsyncSession):
    """Pins ~40+ events and 2 checkouts for 10 entities."""
    merchant = make_merchant_id()
    stats = await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()

    assert stats["entities"] == 10
    assert stats["events"] >= 40
    assert stats["checkouts"] == 2
    events = await _events(session, merchant)
    assert len(events) >= stats["events"]


async def test_seed_folds_with_zero_drift(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=8, seed=7)
    await session.commit()

    report = await rebuild(session)
    await session.commit()
    assert report["drift_detected"] is False
    assert report["matches"] >= 8


async def test_seed_history_covers_all_states(session: AsyncSession):

    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()

    types = {e.event_type for e in await _events(session, merchant)}
    assert "RetrySucceeded" in types
    assert "PromiseHonored" in types
    assert "PromiseBroken" in types
    assert "PaymentMethodChanged" in types
    assert "EscalatedToHuman" in types
    assert "RecoveryReversed" in types


async def test_batch_is_deterministic_and_targeted(session: AsyncSession):
    merchant = make_merchant_id()
    b1 = await generate_failure_batch(session, merchant_id=merchant, count=4, seed=99)
    b2 = await generate_failure_batch(
        session, merchant_id=merchant, count=4, seed=99, prefix="batch2"
    )
    codes1 = [e.payload["failure_code"] for e in b1]
    codes2 = [e.payload["failure_code"] for e in b2]
    assert codes1 == codes2

    aimed = await generate_failure_batch(
        session,
        merchant_id=merchant,
        entity_ids=["sub_004", "sub_008"],
        codes=["insufficient_funds", "GATEWAY_TIMEOUT"],
        seed=1,
        prefix="aimed",
    )
    assert [e.entity_id for e in aimed] == ["sub_004", "sub_008"]
    assert aimed[0].payload["failure_code"] == "insufficient_funds"
