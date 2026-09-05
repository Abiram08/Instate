"""Ledger-derived run metrics (money, compliance, tokens, integrity)."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import Decision
from instate.replay.metrics import (
    RunMetrics,
    compute_run_metrics,
    format_comparison,
    money_flow,
    scan_compliance,
)
from tests.conftest import days_ago, hours_ago, make_merchant_id, now_utc


async def test_money_flow_nets_refunds(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e1",
        entity_type="subscription",
        event_type="RetrySucceeded",
        occurred_at=now_utc(),
        payload={"amount_minor": 49900},
        source_event_id="s1",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e2",
        entity_type="subscription",
        event_type="PromiseHonored",
        occurred_at=now_utc(),
        payload={"amount_minor": 99900},
        source_event_id="s2",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e1",
        entity_type="subscription",
        event_type="RecoveryReversed",
        occurred_at=now_utc(),
        payload={"amount_minor": 49900, "reason": "chargeback"},
        source_event_id="r1",
    )
    await session.commit()

    gross, reversed_total = await money_flow(session, merchant_id=merchant)
    assert gross == 149800
    assert reversed_total == 49900


async def test_scan_compliance_catches_the_fourth_retry(session: AsyncSession):
    """Pins 3 allowed in window; 4th violates."""
    merchant = make_merchant_id()
    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="v1",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(4 - i),
            source_event_id=f"v1_r{i}",
        )
    await session.commit()

    retries, contacts = await scan_compliance(session, merchant_id=merchant)
    assert retries == 1


async def test_scan_compliance_catches_contact_flood(session: AsyncSession):
    merchant = make_merchant_id()
    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="v2",
            entity_type="subscription",
            event_type="CustomerContacted",
            occurred_at=hours_ago(6 - i),
            payload={"channel": "email"},
            source_event_id=f"v2_c{i}",
        )
    await session.commit()

    retries, contacts = await scan_compliance(session, merchant_id=merchant)
    assert contacts == 2


async def test_scan_compliance_window_edges(session: AsyncSession):
    """Pins 7-day window expiry."""
    merchant = make_merchant_id()
    for i, back in enumerate([10, 9, 8, 1, 0.5]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="v3",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() - timedelta(days=back),
            source_event_id=f"v3_r{i}",
        )
    await session.commit()

    retries, _ = await scan_compliance(session, merchant_id=merchant)
    assert retries == 0


async def test_scan_compliance_entity_isolation(session: AsyncSession):
    merchant = make_merchant_id()
    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="vA",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(1),
            source_event_id=f"vA_r{i}",
        )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="vB",
        entity_type="subscription",
        event_type="RetryAttempted",
        occurred_at=days_ago(1),
        source_event_id="vB_r0",
    )
    await session.commit()

    retries, _ = await scan_compliance(session, merchant_id=merchant)
    assert retries == 1


async def test_compute_run_metrics_full_picture(session: AsyncSession):
    merchant = make_merchant_id()
    session.add(Decision(merchant_id=merchant, entity_id="e1"))
    session.add(Decision(merchant_id=merchant, entity_id="e2", tokens_in=900, tokens_out=60))
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e1",
        entity_type="subscription",
        event_type="RetrySucceeded",
        occurred_at=now_utc(),
        payload={"amount_minor": 49900},
        source_event_id="s1",
    )
    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="e3",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(1),
            source_event_id=f"e3_r{i}",
        )
    await session.commit()

    m = await compute_run_metrics(session, merchant_id=merchant)

    assert m.decisions == 2
    assert m.zero_llm_decisions == 1
    assert m.zero_llm_share == 0.5
    assert m.avg_input_tokens == 450
    assert m.gross_recovered_minor == 49900
    assert m.net_recovered_minor == 49900
    assert m.retry_violations == 1
    assert m.compliance_violations == 1
    assert m.chain_verified is True


def test_format_comparison_rows():
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
    table = format_comparison(baseline, instate)

    assert "₹400" in table
    assert "₹500" in table
    assert "0%" in table
    assert "75%" in table
    assert "yes" in table
