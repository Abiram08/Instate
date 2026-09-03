"""Tests for the counterfactual replay (§9, build item 14).

The policy simulator: re-decide history under an overridden policy,
at the original decision times, and project the impact — the question
every collections team has and none can currently answer.
"""

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
    """retry_ceiling_7d 3→2: a historical decision made with 2 prior
    retries would now be DENY — the simulator must see it."""
    merchant = make_merchant_id()
    await seed_all(session)

    entity = "cf_a"
    # History: 2 retries (under the OLD ceiling of 3, over the NEW of 2)
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

    # A decision made at that time (as the pipeline would have recorded)
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
    """The simulator creates v2 shadow rows — the in-force policy v1 is
    untouched (read-only replay, never a rewrite of the present)."""
    merchant = make_merchant_id()
    await seed_all(session)

    await replay_with_policy(session, overrides={"retry_ceiling_7d": 2}, merchant_id=merchant)
    await session.commit()

    v1 = {r.rule_id: r for r in await get_rules(session, "subscription", 1)}
    v2 = {r.rule_id: r for r in await get_rules(session, "subscription", 2)}
    assert v1["retry_ceiling_7d"].limit_value == 3
    assert v2["retry_ceiling_7d"].limit_value == 2


async def test_replay_money_projection(session: AsyncSession):
    """A decision that actually recovered money, denied by the new policy
    → projected recovered-lost. And a doomed attempt → avoided."""
    merchant = make_merchant_id()
    await seed_all(session)

    entity = "cf_money"
    # 2 prior retries, spaced >24h apart (clear of the spacing rule) —
    # the OLD ceiling of 3 allowed a 3rd; the NEW ceiling of 2 denies it
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

    # The third retry recovered money (linked to this decision)
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
    """Loosening a policy over a decision that was DENIED (and never
    executed) IS a verdict change worth reporting — but it contributes no
    money projection, because nothing ever executed."""
    merchant = make_merchant_id()
    await seed_all(session)

    # 4 retries → DENY at the old ceiling
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
    assert g1.verdict == "DENY"  # the decision existed, denied
    await session.commit()

    report = await replay_with_policy(
        session, overrides={"retry_ceiling_7d": 5}, merchant_id=merchant
    )
    await session.commit()
    assert report.looser == 1  # DENY→ALLOW, reported
    assert report.projected_recovered_lost_minor == 0  # nothing executed → nothing lost
