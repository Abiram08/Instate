"""Bi-temporal frozen decisions (§1b).

The compliance question is "was this decision correct given what we knew
at the time?" — so re-evaluating a past decision must reproduce its
verdict EXACTLY, even after a late-arriving event lands. The mechanism:
`as_of` pins the knowledge cutoff (`recorded_at <= as_of`) alongside
the window anchor (`now`). A late event (old occurred_at, new
recorded_at) affects only future decisions — never rewrites a past one.
"""

from datetime import timedelta

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.gate import evaluate
from instate.core.ledger import record_event, verify_chain
from instate.core.models import Event
from instate.core.policy import seed_default_policy
from tests.conftest import make_merchant_id, now_utc


async def _backdate_recorded(session: AsyncSession, source_id: str, when):
    """Test-only time travel: pretend a row was LEARNED at `when`.

    recorded_at is not part of the chain hash, so this cannot break
    verification — it only changes archive/replay eligibility, which is
    exactly what these tests exercise.
    """
    await session.execute(
        update(Event).where(Event.source_event_id == source_id).values(recorded_at=when)
    )
    await session.commit()


async def test_late_arrival_does_not_rewrite_a_past_decision(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    base = now_utc()
    t0 = base - timedelta(days=5)

    # Two retries, learned when they happened. Placed at t0-40h and
    # t0-30h: inside T0's 7d ceiling window (2/3 → ALLOW), outside its
    # 24h spacing window (spacing stays quiet), AND strictly inside the
    # live window at base (so the live re-check below can reach 3).
    for i, back in enumerate([timedelta(hours=40), timedelta(hours=30)]):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_frozen",
            entity_type="subscription", event_type="RetryAttempted",
            occurred_at=t0 - back, source_event_id=f"frozen_r{i}",
        )
    await session.commit()
    await _backdate_recorded(session, "frozen_r0", t0 - timedelta(hours=40))
    await _backdate_recorded(session, "frozen_r1", t0 - timedelta(hours=30))

    # The original decision at T0: 2/3 → ALLOW, recorded
    d1 = await evaluate(
        session, merchant_id=merchant, entity_id="sub_frozen",
        entity_type="subscription", action_class="RETRY_NOW",
        root_cause="insufficient_funds", now=t0, as_of=t0, record=True,
    )
    await session.commit()
    assert d1.verdict == "ALLOW"
    assert d1.decision_id is not None

    # LATE arrival: occurred 12h before T0 (inside T0's window!),
    # but only learned NOW — appends at the chain tail.
    await record_event(
        session, merchant_id=merchant, entity_id="sub_frozen",
        entity_type="subscription", event_type="RetryAttempted",
        occurred_at=t0 - timedelta(hours=6), source_event_id="frozen_late",
    )
    await session.commit()

    # The chain absorbs out-of-order appends without breaking
    check = await verify_chain(session, merchant, "sub_frozen")
    assert check.verified, f"late append must not break the chain: {check.error}"

    # Replaying the decision AS OF T0 reproduces it EXACTLY — the late
    # event was unknown then, so it does not count.
    replay = await evaluate(
        session, merchant_id=merchant, entity_id="sub_frozen",
        entity_type="subscription", action_class="RETRY_NOW",
        root_cause="insufficient_funds", now=t0, as_of=t0, record=False,
    )
    assert replay.verdict == "ALLOW"
    entry = next(e for e in replay.reason_chain if e["rule_id"] == "retry_ceiling_7d")
    assert entry["observed"] == 2


async def test_late_arrival_affects_future_decisions(session: AsyncSession):
    """The other half: once learned, the late event IS live knowledge —
    a fresh evaluation sees all 3 attempts and DENYs."""
    merchant = make_merchant_id()
    await seed_default_policy(session)
    base = now_utc()
    t0 = base - timedelta(days=5)

    for i, back in enumerate([timedelta(hours=40), timedelta(hours=30)]):
        await record_event(
            session, merchant_id=merchant, entity_id="sub_live",
            entity_type="subscription", event_type="RetryAttempted",
            occurred_at=t0 - back, source_event_id=f"live_r{i}",
        )
    await session.commit()
    await _backdate_recorded(session, "live_r0", t0 - timedelta(hours=40))
    await _backdate_recorded(session, "live_r1", t0 - timedelta(hours=30))

    await record_event(
        session, merchant_id=merchant, entity_id="sub_live",
        entity_type="subscription", event_type="RetryAttempted",
        occurred_at=t0 - timedelta(hours=6), source_event_id="live_late",
    )
    await session.commit()

    live = await evaluate(
        session, merchant_id=merchant, entity_id="sub_live",
        entity_type="subscription", action_class="RETRY_NOW",
        root_cause="insufficient_funds", record=False,
    )
    assert live.verdict == "DENY"
    entry = next(e for e in live.reason_chain if e["rule_id"] == "retry_ceiling_7d")
    assert entry["observed"] == 3


async def test_as_of_without_now_still_cuts_knowledge(session: AsyncSession):
    """as_of alone (live window anchor) still excludes not-yet-learned
    rows — the two times are independent for a reason."""
    from datetime import timedelta as td

    from instate.core.projection import get_windowed_count

    merchant = make_merchant_id()
    base = now_utc()
    await record_event(
        session, merchant_id=merchant, entity_id="sub_cut",
        entity_type="subscription", event_type="RetryAttempted",
        occurred_at=base - td(hours=1), source_event_id="cut_r0",
    )
    await session.commit()
    await _backdate_recorded(session, "cut_r0", base - td(days=30))

    full = await get_windowed_count(
        session, merchant, "sub_cut", "retry_count_7d", td(days=7)
    )
    assert full == 1
    frozen = await get_windowed_count(
        session, merchant, "sub_cut", "retry_count_7d", td(days=7),
        as_of=base - td(days=31),
    )
    assert frozen == 0  # learned later → invisible as of then
