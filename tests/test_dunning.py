"""Dunning sequences as versioned data."""

from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.decide import build_context
from instate.core.dunning import (
    next_sequence_step,
    seed_default_sequences,
)
from instate.core.ledger import record_event
from instate.core.policy import seed_default_policy
from tests.conftest import make_merchant_id, now_utc


async def test_seed_sequences_idempotent(session: AsyncSession):
    first = await seed_default_sequences(session)
    second = await seed_default_sequences(session)
    await session.commit()
    assert first > 0
    assert second == 0


async def _touch(session, merchant, entity_id, i):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="CustomerContacted",
        occurred_at=now_utc(),
        payload={"channel": "email"},
        source_event_id=f"{entity_id}_t{i}",
    )


async def test_step_advances_with_touches(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_sequences(session)
    await session.commit()

    step0 = await next_sequence_step(
        session, root_cause="insufficient_funds", merchant_id=merchant, entity_id="sub_dun"
    )
    assert step0 is not None
    assert step0["step_index"] == 0
    assert step0["action"] == "SEND_PAYMENT_LINK"

    await _touch(session, merchant, "sub_dun", 0)
    await session.commit()
    step1 = await next_sequence_step(
        session, root_cause="insufficient_funds", merchant_id=merchant, entity_id="sub_dun"
    )
    assert step1 is not None
    assert step1["step_index"] == 1
    assert step1["channel"] == "email"
    assert step1["delay_hours"] == 72


async def test_past_last_step_returns_none(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_sequences(session)
    await session.commit()

    for i in range(5):
        await _touch(session, merchant, "sub_done", i)
    await session.commit()

    assert (
        await next_sequence_step(
            session, root_cause="insufficient_funds", merchant_id=merchant, entity_id="sub_done"
        )
        is None
    )


async def test_unknown_root_cause_has_no_sequence(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_sequences(session)
    await session.commit()

    assert (
        await next_sequence_step(
            session, root_cause="fraud_block", merchant_id=merchant, entity_id="sub_x"
        )
        is None
    )


async def test_digest_carries_dunning_step(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    await seed_default_sequences(session)
    await session.commit()

    ctx = await build_context(
        session,
        merchant_id=merchant,
        entity_id="sub_dig",
        entity_type="subscription",
        root_cause="insufficient_funds",
        policy_version=1,
    )
    assert ctx["dunning_step"] is not None
    assert ctx["dunning_step"]["step_index"] == 0
    assert ctx["dunning_step"]["action"] == "SEND_PAYMENT_LINK"


async def test_card_expired_sequence_leads_with_whatsapp(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_sequences(session)
    await session.commit()

    await _touch(session, merchant, "sub_wa", 0)
    await session.commit()
    step = await next_sequence_step(
        session, root_cause="card_expired", merchant_id=merchant, entity_id="sub_wa"
    )
    assert step is not None
    assert step["channel"] == "whatsapp"
    assert step["action"] == "REQUEST_PAYMENT_METHOD"
