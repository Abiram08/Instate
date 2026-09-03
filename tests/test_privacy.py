"""Network-scope privacy — the moat only exists if trust does (§15).

k=3 is a DEMO threshold (three merchants is a group chat, not
anonymity); production uses PRODUCTION_K=10 + Laplace noise. These
tests pin both the mechanism and the honesty about the default.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Case
from instate.core.precedent import HashingEmbedder
from instate.core.privacy import PRODUCTION_K, publishable_patterns
from tests.conftest import make_merchant_id


async def _private_case(session: AsyncSession, merchant, idx: int):
    session.add(
        Case(
            merchant_id=merchant,
            scope="private",
            entity_type="subscription",
            root_cause="insufficient_funds",
            situation=f"case {idx}",
            action_taken="retry",
            outcome="recovered",
            recovered_minor=49900,
            embedding=HashingEmbedder().embed(f"case {idx}"),
            source_entity_id=f"e_{merchant.hex[:6]}_{idx}",
        )
    )


async def test_pattern_publishable_at_demo_k(session: AsyncSession):
    merchants = [make_merchant_id() for _ in range(3)]
    for m in merchants:
        await _private_case(session, m, 0)
    await session.commit()

    patterns = await publishable_patterns(session)
    assert len(patterns) == 1
    assert patterns[0]["root_cause"] == "insufficient_funds"
    assert patterns[0]["merchants"] == 3


async def test_production_k_holds_back_small_groups(session: AsyncSession):
    """Three merchants clear the demo bar but NOT the production bar —
    the threshold is a deployment knob, and the demo says so."""
    assert PRODUCTION_K == 10
    merchants = [make_merchant_id() for _ in range(3)]
    for m in merchants:
        await _private_case(session, m, 0)
    await session.commit()

    assert await publishable_patterns(session, k=PRODUCTION_K) == []


async def test_pattern_reaches_production_k(session: AsyncSession):
    merchants = [make_merchant_id() for _ in range(PRODUCTION_K)]
    for m in merchants:
        await _private_case(session, m, 0)
    await session.commit()

    patterns = await publishable_patterns(session, k=PRODUCTION_K)
    assert len(patterns) == 1
    assert patterns[0]["merchants"] == PRODUCTION_K


async def test_single_merchant_pattern_never_publishable(session: AsyncSession):
    merchant = make_merchant_id()
    await _private_case(session, merchant, 0)
    await _private_case(session, merchant, 1)
    await session.commit()

    # Two cases, one merchant — distinct-merchant counting holds
    assert await publishable_patterns(session) == []
