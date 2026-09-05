"""Channel allowlist and per-channel contact caps."""

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.diagnose import diagnose, seed_default_diagnosis, taxonomy_for
from instate.agent.execute import ACTION_CHANNEL, resolve_channel
from instate.core.gate import check_proposal, evaluate
from instate.core.ledger import record_event
from instate.core.models import ALLOWED_CHANNELS, Event
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events, get_windowed_count
from tests.conftest import make_merchant_id, now_utc


async def seed_all(session: AsyncSession):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    from instate.agent.diagnose import seed_default_taxonomy

    await seed_default_taxonomy(session)
    await session.commit()


async def _contacts(session, merchant, entity_id, specs):
    """specs: list of (channel, hours_ago)."""
    base = now_utc()
    for i, (channel, back) in enumerate(specs):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="CustomerContacted",
            occurred_at=base - timedelta(hours=back),
            payload={"channel": channel},
            source_event_id=f"{entity_id}_c{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()


async def _gate1_id(session, merchant, entity_id, action_class="SEND_PAYMENT_LINK", **kw):
    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        action_class=action_class,
        **kw,
    )
    await session.commit()
    return result.decision_id


# Channel resolution

def test_whatsapp_is_allowlisted():
    assert "whatsapp" in ALLOWED_CHANNELS
    assert "upi" in ALLOWED_CHANNELS


def test_resolve_channel_honors_valid_override():
    assert resolve_channel("SEND_PAYMENT_LINK", {"channel": "whatsapp"}) == "whatsapp"


def test_resolve_channel_falls_back_on_garbage():
    assert resolve_channel("SEND_PAYMENT_LINK", {"channel": "pigeon"}) == "payment_link"
    assert resolve_channel("SEND_PAYMENT_LINK", None) == "payment_link"
    assert resolve_channel("SEND_PAYMENT_LINK", {}) == "payment_link"


def test_action_channel_defaults_unchanged():
    assert ACTION_CHANNEL["SEND_PAYMENT_LINK"] == "payment_link"


# Denormalization


async def test_event_channel_denormalized_from_payload(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_ch",
        entity_type="subscription",
        event_type="CustomerContacted",
        occurred_at=now_utc(),
        payload={"channel": "whatsapp"},
        source_event_id="sub_ch_0",
    )
    await session.commit()

    result = await session.execute(select(Event.channel).where(Event.source_event_id == "sub_ch_0"))
    assert result.scalar_one() == "whatsapp"


async def test_event_channel_none_without_payload_channel(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_ch2",
        entity_type="subscription",
        event_type="RetryAttempted",
        occurred_at=now_utc(),
        source_event_id="sub_ch2_0",
    )
    await session.commit()

    result = await session.execute(
        select(Event.channel).where(Event.source_event_id == "sub_ch2_0")
    )
    assert result.scalar_one() is None


# Per-channel caps at Gate-2


async def test_per_channel_cap_fires_only_on_that_channel(session: AsyncSession):
    """Pins per-channel keying: 2 whatsapp in 24h blocks whatsapp only."""
    merchant = make_merchant_id()
    await seed_all(session)
    await _contacts(session, merchant, "sub_pc", [("whatsapp", 5), ("whatsapp", 1)])
    decision_id = await _gate1_id(session, merchant, "sub_pc")

    wa = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_pc",
        entity_type="subscription",
        decision_id=decision_id,
        proposal={"action": "SEND_PAYMENT_LINK", "channel": "whatsapp", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()
    assert wa.verdict == "DENY"
    entry = next(e for e in wa.reason_chain if e["rule_id"] == "contact_freq_24h")
    assert entry["observed"] == 2
    assert "whatsapp" in (entry.get("detail") or "")

    sms = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_pc",
        entity_type="subscription",
        decision_id=decision_id,
        proposal={"action": "SEND_PAYMENT_LINK", "channel": "sms", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()
    assert sms.verdict == "ALLOW"


async def test_no_channel_falls_back_to_global_count(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    await _contacts(session, merchant, "sub_glob", [("email", 5), ("sms", 1)])
    decision_id = await _gate1_id(session, merchant, "sub_glob")

    result = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_glob",
        entity_type="subscription",
        decision_id=decision_id,
        proposal={"action": "SEND_PAYMENT_LINK", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()
    assert result.verdict == "DENY"
    entry = next(e for e in result.reason_chain if e["rule_id"] == "contact_freq_24h")
    assert entry["observed"] == 2
    assert entry.get("detail") is None


async def test_windowed_count_channel_filter(session: AsyncSession):
    merchant = make_merchant_id()
    await _contacts(session, merchant, "sub_wc", [("whatsapp", 5), ("email", 3), ("whatsapp", 1)])

    total = await get_windowed_count(
        session, merchant, "sub_wc", "contacts_24h", timedelta(hours=24)
    )
    assert total == 3
    wa = await get_windowed_count(
        session, merchant, "sub_wc", "contacts_24h", timedelta(hours=24), channel="whatsapp"
    )
    assert wa == 2
    email = await get_windowed_count(
        session, merchant, "sub_wc", "contacts_24h", timedelta(hours=24), channel="email"
    )
    assert email == 1


# customer_error


async def test_customer_error_diagnosed(session: AsyncSession):
    await seed_default_diagnosis(session)
    await session.commit()
    assert await diagnose(session, failure_code="WRONG_UPI_PIN") == "customer_error"
    assert await diagnose(session, failure_code="INCORRECT_CVV") == "customer_error"
    assert await diagnose(session, failure_code="INVALID_CARD_NUMBER") == "customer_error"


async def test_customer_error_taxonomy_is_link_not_retry(session: AsyncSession):
    from instate.agent.diagnose import seed_default_taxonomy

    await seed_default_taxonomy(session)
    await session.commit()
    rule = await taxonomy_for(session, "customer_error")
    assert rule.default_action == "SEND_PAYMENT_LINK"
    assert rule.deterministic is False
