"""Tests for the seed generator — determinism, volume, honesty.

The comparison's fairness stands on the seed: same seed → identical
history and batch for both agents. The rebuild check proves the seeded
history folds cleanly (zero drift) — the projection stays honest even
on synthetic data.
"""

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
    """Same seed → identical event-type sequence. The comparison's
    fairness depends on this."""
    m1, m2 = make_merchant_id(), make_merchant_id()
    s1 = await seed_history(session, merchant_id=m1, entities=10, seed=42)
    s2 = await seed_history(session, merchant_id=m2, entities=10, seed=42)

    assert s1 == s2
    seq1 = [(e.entity_id, e.event_type) for e in await _events(session, m1)]
    seq2 = [(e.entity_id, e.event_type) for e in await _events(session, m2)]
    assert seq1 == seq2


async def test_seed_history_volume(session: AsyncSession):
    """~10 entities yield 40+ events with 2 thin checkouts — scaled to
    the demo's ~300/30 target."""
    merchant = make_merchant_id()
    stats = await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()

    assert stats["entities"] == 10
    assert stats["events"] >= 40
    assert stats["checkouts"] == 2
    events = await _events(session, merchant)
    assert len(events) >= stats["events"]


async def test_seed_folds_with_zero_drift(session: AsyncSession):
    """The synthetic history folds cleanly: rebuild diffs to zero —
    the projection is honest even on generated data."""
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=8, seed=7)
    await session.commit()

    report = await rebuild(session)
    await session.commit()
    assert report["drift_detected"] is False
    assert report["matches"] >= 8


async def test_seed_history_covers_all_states(session: AsyncSession):
    """Every archetype's terminal state exists: recovered, awaiting
    (broken), escalated, at-ceiling — the memory has real shape."""

    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()

    types = {e.event_type for e in await _events(session, merchant)}
    assert "RetrySucceeded" in types  # recovered by retry
    assert "PromiseHonored" in types  # promise kept
    assert "PromiseBroken" in types  # promise broken
    assert "PaymentMethodChanged" in types  # method updater
    assert "EscalatedToHuman" in types  # fraud escalation
    assert "RecoveryReversed" in types  # chargeback on a recovery


async def test_batch_is_deterministic_and_targeted(session: AsyncSession):
    """The batch: fresh entities by default; aimed entities/codes when
    given — identical for both agents under the same seed."""
    merchant = make_merchant_id()
    b1 = await generate_failure_batch(session, merchant_id=merchant, count=4, seed=99)
    # same seed, fresh entities → same codes sequence
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
