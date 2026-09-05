"""Razorpay-native retry and jurisdiction policy rows."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.gate import evaluate
from instate.core.ledger import record_event
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from tests.conftest import make_merchant_id, now_utc


async def seed_policy(session: AsyncSession):
    await seed_default_policy(session)
    await session.commit()


def chain_entry(result, rule_id: str) -> dict:
    return next(e for e in result.reason_chain if e["rule_id"] == rule_id)


async def _two_retries_today(session, merchant, entity_id: str):
    base = now_utc()
    for i, back in enumerate([timedelta(hours=5), timedelta(hours=1)]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=base - back,
            source_event_id=f"{entity_id}_r{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()


# Razorpay retry model


async def test_upi_daily_cap_fires_on_second_same_day_attempt(session: AsyncSession):
    """Pins 24h window: 2nd attempt DENY."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await _two_retries_today(session, merchant, "sub_upi")

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_upi",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
        context={"method": "upi"},
    )
    await session.commit()

    assert result.verdict == "DENY"
    entry = chain_entry(result, "upi_daily_retry_cap")
    assert entry["observed"] == 2
    assert entry["limit"] == 1


async def test_method_cap_ignores_other_methods(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    await _two_retries_today(session, merchant, "sub_nomethod")

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_nomethod",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    await session.commit()

    assert result.verdict == "DENY"
    assert "upi_daily_retry_cap" not in {e["rule_id"] for e in result.reason_chain}


async def test_emandate_spacing_is_72h(session: AsyncSession):
    """Pins 72h spacing for emandate."""
    merchant = make_merchant_id()
    await seed_policy(session)
    base = now_utc()
    for i, back in enumerate([timedelta(hours=30), timedelta(hours=1)]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_em",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=base - back,
            source_event_id=f"sub_em_r{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_em",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="mandate_inactive",
        context={"method": "emandate", "confirmed": True},
    )
    await session.commit()

    assert result.verdict == "DENY"
    assert chain_entry(result, "emandate_retry_spacing_72h")["observed"] == 2


async def test_emandate_unconfirmed_requires_human(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_em2",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="mandate_inactive",
        context={"method": "emandate", "confirmed": False},
    )
    await session.commit()

    assert result.verdict == "REQUIRE_HUMAN"
    assert any(e["rule_id"] == "emandate_require_confirmation" for e in result.reason_chain)


# Jurisdiction caps


async def _one_contact_today(session, merchant, entity_id: str):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="CustomerContacted",
        occurred_at=now_utc() - timedelta(hours=1),
        payload={"channel": "sms"},
        source_event_id=f"{entity_id}_c0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()


async def test_trai_cap_is_stricter_than_generic(session: AsyncSession):
    """Pins TRAI 1/24h vs generic 2/24h; edge at observed==limit."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await _one_contact_today(session, merchant, "sub_trai")

    indian = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_trai",
        entity_type="subscription",
        action_class="SEND_PAYMENT_LINK",
        root_cause="card_expired",
        context={"jurisdiction": "IN"},
    )
    await session.commit()
    assert indian.verdict == "DENY"
    entry = chain_entry(indian, "contact_freq_24h_TRAI")
    assert entry["observed"] == 1
    assert entry["limit"] == 1

    generic = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_trai",
        entity_type="subscription",
        action_class="SEND_PAYMENT_LINK",
        root_cause="card_expired",
    )
    await session.commit()
    assert generic.verdict == "ALLOW"
