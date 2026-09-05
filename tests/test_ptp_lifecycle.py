"""Promise-to-pay lifecycle from PromiseMade to recovery."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.decide import process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.adapters.razorpay import GatewayResponse
from instate.core.ledger import record_event
from instate.core.models import EntityState
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from instate.core.watchers import check_watchers, seed_default_watchers
from instate.replay.metrics import money_flow
from tests.conftest import make_merchant_id, now_utc


class FakeReasoner:
    model_name = "fake"
    last_usage = (900, 60)

    def __init__(self, proposal):
        self.proposal = proposal

    async def propose(self, context):
        return self.proposal


class FakeGateway:
    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        return GatewayResponse("completed", provider_ref="ref")

    async def lookup(self, key):
        return None


class RecordingNotifier:
    def __init__(self):
        self.sent = []

    async def send(self, url, payload, signature):
        self.sent.append((url, payload))
        return True


async def seed_all(session: AsyncSession):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def test_promise_lifecycle_recovers_money(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    base = now_utc()

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_ptp",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=base,
        payload={"failure_code": "customer_cancelled", "amount_minor": 99900},
        source_event_id="sub_ptp_wh",
    )
    await session.commit()

    reasoner = FakeReasoner(
        {
            "action": "AWAIT_PROMISE",
            "timing": "IMMEDIATE",
            "rationale": "customer owns timing",
            "confidence": 0.9,
        }
    )
    result = await process_failure(
        session, event=event, reasoner=reasoner, gateway=FakeGateway(), now=base
    )
    await session.commit()
    assert result.executed_action == "AWAIT_PROMISE"

    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_ptp",
        entity_type="subscription",
        event_type="PromiseMade",
        occurred_at=base,
        payload={"due_at": (base + timedelta(days=3)).isoformat()},
        source_event_id="sub_ptp_pm",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_ptp"))
    assert state.status == "AWAITING_PROMISE"
    assert state.open_ptp_due_at is not None

    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_ptp",
        entity_type="subscription",
        event_type="PromiseHonored",
        occurred_at=base + timedelta(days=1),
        payload={"amount_minor": 99900},
        source_event_id="sub_ptp_ph",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_ptp"))
    assert state.status == "RECOVERED"
    assert state.open_ptp_due_at is None

    gross, reversed_ = await money_flow(session, merchant_id=merchant)
    assert gross == 99900
    assert reversed_ == 0


async def test_broken_promise_routes_back_to_diagnosed(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    base = now_utc()

    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_pb",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=base,
        payload={"failure_code": "customer_cancelled"},
        source_event_id="sub_pb_wh",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_pb",
        entity_type="subscription",
        event_type="PromiseMade",
        occurred_at=base,
        payload={"due_at": base.isoformat()},
        source_event_id="sub_pb_pm",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_pb",
        entity_type="subscription",
        event_type="PromiseBroken",
        occurred_at=base,
        payload={},
        source_event_id="sub_pb_pb",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_pb"))
    assert state.status == "DIAGNOSED"
    assert state.open_ptp_due_at is None


async def test_overdue_promise_fires_watcher(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_all(session)
    base = now_utc()

    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_od",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=base - timedelta(days=5),
        payload={"failure_code": "customer_cancelled"},
        source_event_id="sub_od_wh",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_od",
        entity_type="subscription",
        event_type="PromiseMade",
        occurred_at=base - timedelta(days=4),
        payload={"due_at": (base - timedelta(days=1)).isoformat()},
        source_event_id="sub_od_pm",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    await session.commit()

    notifier = RecordingNotifier()
    fired = await check_watchers(session, notifier=notifier, now=base)
    assert fired >= 1
    assert any(p["entity_id"] == "sub_od" for _, p in notifier.sent)
