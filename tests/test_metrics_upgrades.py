"""Recovery-by-value metrics: time-to-recovery, at-risk, lift."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.projection import fold_events
from instate.replay.metrics import (
    RunMetrics,
    at_risk_revenue,
    compute_run_metrics,
    format_comparison,
    time_to_recovery_hours,
)
from tests.conftest import make_merchant_id, now_utc


def make_metrics():
    baseline = RunMetrics(
        gross_recovered_minor=50000,
        reversed_minor=10000,
        retry_violations=2,
        contact_violations=1,
        decisions=4,
        zero_llm_decisions=0,
        llm_input_tokens=24000,
        llm_output_tokens=240,
        chain_breaks=0,
        entities=5,
        events=30,
    )
    instate = RunMetrics(
        gross_recovered_minor=50000,
        reversed_minor=0,
        retry_violations=0,
        contact_violations=0,
        decisions=4,
        zero_llm_decisions=3,
        llm_input_tokens=900,
        llm_output_tokens=60,
        chain_breaks=0,
        entities=5,
        events=30,
    )
    return baseline, instate


async def _recovered_entity(session, merchant, entity_id, fail_days_ago, recover_days_ago, amount):
    base = now_utc()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=base - timedelta(days=fail_days_ago),
        payload={"amount_minor": amount, "failure_code": "x"},
        source_event_id=f"{entity_id}_f",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="RetrySucceeded",
        occurred_at=base - timedelta(days=recover_days_ago),
        payload={"amount_minor": amount},
        source_event_id=f"{entity_id}_r",
    )


async def test_ttr_median_ignores_open_entities(session: AsyncSession):
    merchant = make_merchant_id()
    await _recovered_entity(session, merchant, "t1", 10, 8, 1000)
    await _recovered_entity(session, merchant, "t2", 10, 9, 1000)
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="t3",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc() - timedelta(days=20),
        payload={"amount_minor": 1000},
        source_event_id="t3_f",
    )
    await session.commit()

    ttrs = await time_to_recovery_hours(session, merchant_id=merchant)
    assert sorted(ttrs) == [24.0, 48.0]


async def test_at_risk_counts_only_open_entities(session: AsyncSession):
    merchant = make_merchant_id()
    await _recovered_entity(session, merchant, "r1", 10, 8, 50000)
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="r2",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc() - timedelta(days=1),
        payload={"amount_minor": 25000, "failure_code": "x"},
        source_event_id="r2_f",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    assert await at_risk_revenue(session, merchant_id=merchant) == 25000


async def test_compute_includes_new_fields(session: AsyncSession):
    merchant = make_merchant_id()
    await _recovered_entity(session, merchant, "m1", 10, 8, 50000)
    await session.commit()
    await fold_events(session)
    await session.commit()

    m = await compute_run_metrics(session, merchant_id=merchant)
    assert m.median_ttr_hours == 48.0
    assert m.at_risk_minor == 0


async def test_format_has_lift_and_new_rows():
    baseline, instate = make_metrics()
    table = format_comparison(baseline, instate)
    assert "lift over baseline" in table
    assert "median time-to-recovery" in table
    assert "at-risk revenue (open)" in table
