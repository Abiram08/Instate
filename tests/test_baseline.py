"""Stateless baseline behavior without memory."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.core.ledger import record_event
from instate.core.models import Decision, Event
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from instate.replay.baseline import StatelessBaselineAgent
from tests.conftest import days_ago, make_merchant_id, now_utc


class FakeReasoner:
    model_name = "fake"
    last_usage = (900, 60)

    def __init__(self, proposal):
        self.proposal = proposal
        self.contexts = []

    async def propose(self, context):
        self.contexts.append(context)
        return self.proposal


class FakeGateway:
    def __init__(self, status="completed"):
        self.status = status
        self.calls = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse(self.status, provider_ref="r", amount_minor=49900)

    async def lookup(self, key):
        return None


NAIVE = {"action": "RETRY_NOW", "timing": "IMMEDIATE", "rationale": "n", "confidence": 0.9}


async def seed_all(session):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def events_of(session, merchant, entity_id):
    result = await session.execute(
        select(Event.event_type)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .order_by(Event.id.asc())
    )
    return [r[0] for r in result.all()]


async def at_ceiling_event(session, merchant, entity_id):
    """Seed 3 retries in the last week (at ceiling)."""
    for i in range(3):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(i + 1),
            source_event_id=f"{entity_id}_r{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()
    return await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds", "amount_minor": 99900},
        source_event_id=f"{entity_id}_wh",
    )


async def test_baseline_retries_past_the_ceiling(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    event = await at_ceiling_event(session, merchant, "base_ceil")

    agent = StatelessBaselineAgent(FakeReasoner(NAIVE), FakeGateway())
    result = await agent.process_failure(session, event=event)
    await session.commit()

    assert result.executed_action == "RETRY_NOW"
    types = await events_of(session, merchant, "base_ceil")
    assert types.count("RetryAttempted") == 4


async def test_baseline_calls_the_model_every_time(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    event = await at_ceiling_event(session, merchant, "base_model")

    reasoner = FakeReasoner(NAIVE)
    agent = StatelessBaselineAgent(reasoner, FakeGateway())
    await agent.process_failure(session, event=event)
    await session.commit()

    assert len(reasoner.contexts) == 1

    event2 = await record_event(
        session,
        merchant_id=merchant,
        entity_id="base_fraud",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "FRAUD_DETECTED"},
        source_event_id="base_fraud_wh",
    )
    await session.commit()
    result = await agent.process_failure(session, event=event2)
    await session.commit()
    assert result.llm_called is True


async def test_baseline_context_is_unbounded(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)

    for i in range(12):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="base_ctx",
            entity_type="subscription",
            event_type="CustomerContacted",
            occurred_at=days_ago(12 - i),
            payload={"channel": "email"},
            source_event_id=f"base_ctx_c{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()
    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="base_ctx",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id="base_ctx_wh",
    )
    await session.commit()

    reasoner = FakeReasoner(NAIVE)
    agent = StatelessBaselineAgent(reasoner, FakeGateway())
    result = await agent.process_failure(session, event=event)
    await session.commit()

    assert result.context_chars > 800
    sent = reasoner.contexts[0]
    assert len(sent["history"]) == 13


async def test_baseline_has_no_gate_evidence(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    event = await at_ceiling_event(session, merchant, "base_nogate")

    agent = StatelessBaselineAgent(FakeReasoner(NAIVE), FakeGateway())
    await agent.process_failure(session, event=event)
    await session.commit()

    result = await session.execute(select(Decision).where(Decision.entity_id == "base_nogate"))
    decision = result.scalar_one()
    assert decision.gate1 is None and decision.gate2 is None
    assert decision.prompt_text is not None
    assert len(decision.prompt_text) > 400


async def test_baseline_still_uses_the_outbox(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    event = await at_ceiling_event(session, merchant, "base_outbox")

    agent = StatelessBaselineAgent(FakeReasoner(NAIVE), FakeGateway())
    await agent.process_failure(session, event=event)
    await session.commit()

    types = await events_of(session, merchant, "base_outbox")
    assert "ActionIntended" in types
    assert "ActionCompleted" in types
