"""Counterfactual replay under overridden policy."""

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.core.gate import evaluate
from instate.core.ledger import record_event
from instate.core.models import Decision
from instate.core.policy import get_rules, seed_default_policy
from instate.replay.counterfactual import replay_with_policy
from tests.conftest import make_merchant_id, now_utc


class FakeGateway:
    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        return GatewayResponse("completed", provider_ref="r", amount_minor=49900)

    async def lookup(self, key):
        return None


class FakeReasoner:
    model_name = "fake"
    last_usage = (900, 60)

    async def propose(self, context):
        return {"action": "RETRY_NOW", "timing": "IMMEDIATE", "rationale": "n", "confidence": 0.9}


async def seed_all(session):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def test_replay_stricter_ceiling_flags_history(session: AsyncSession):
    """Pins 3->2 ceiling: 2 prior retries flips ALLOW to DENY."""
    merchant = make_merchant_id()
    await seed_all(session)

    entity = "cf_a"
    for i in range(2):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() - timedelta(days=2 - i * 0.5),
            source_event_id=f"{entity}_r{i}",
        )
    await session.commit()

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id=entity,
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    decision = await session.get(Decision, g1.decision_id)
    decision.executed_action = "RETRY_NOW"
    await session.commit()

    report = await replay_with_policy(
        session, overrides={"retry_ceiling_7d": 2}, merchant_id=merchant
    )
    await session.commit()

    assert report.policy_version_from == 1
    assert report.policy_version_to == 2
    assert report.decisions_replayed >= 1
    assert report.stricter >= 1
    assert any("ALLOW→" in e for e in report.examples)


async def test_replay_shadow_policy_does_not_disturb_v1(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)

    await replay_with_policy(session, overrides={"retry_ceiling_7d": 2}, merchant_id=merchant)
    await session.commit()

    v1 = {r.rule_id: r for r in await get_rules(session, "subscription", 1)}
    v2 = {r.rule_id: r for r in await get_rules(session, "subscription", 2)}
    assert v1["retry_ceiling_7d"].limit_value == 3
    assert v2["retry_ceiling_7d"].limit_value == 2


async def test_replay_money_projection(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)

    entity = "cf_money"
    # 2 retries spaced >24h (clear of spacing rule).
    for i, back in enumerate([3.0, 2.0]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() - timedelta(days=back),
            source_event_id=f"{entity}_r{i}",
        )
    await session.commit()

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id=entity,
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    decision = await session.get(Decision, g1.decision_id)
    decision.executed_action = "RETRY_NOW"
    await session.commit()

    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity,
        entity_type="subscription",
        event_type="RetrySucceeded",
        occurred_at=now_utc(),
        payload={"amount_minor": 49900},
        source_event_id=f"{entity}_ok",
        decision_id=decision.id,
    )
    await session.commit()

    report = await replay_with_policy(
        session, overrides={"retry_ceiling_7d": 2}, merchant_id=merchant
    )
    await session.commit()

    assert report.projected_recovered_lost_minor == 49900


async def test_replay_unseeded_policy_raises(session: AsyncSession):
    with pytest.raises(LookupError):
        await replay_with_policy(session, overrides={"retry_ceiling_7d": 2})


async def test_replay_loosened_denied_decision_is_reported_not_scored(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)

    for i in range(4):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="cf_denied",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() - timedelta(days=6 + i * 0.1),
            source_event_id=f"cf_d_r{i}",
        )
    await session.commit()
    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="cf_denied",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    assert g1.verdict == "DENY"
    await session.commit()

    report = await replay_with_policy(
        session, overrides={"retry_ceiling_7d": 5}, merchant_id=merchant
    )
    await session.commit()
    assert report.looser == 1
    assert report.projected_recovered_lost_minor == 0
