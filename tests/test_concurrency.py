"""Concurrency (TOCTOU): two pipelines, one entity at 2/3 retries.
First to complete takes the 3rd attempt; second must DENY, never a 4th RetryAttempted.
Without the per-entity lock, both pass Gate-1 on SQLite (FOR UPDATE is a no-op) and double-act."""

import asyncio
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from instate.adapters.razorpay import GatewayResponse
from instate.agent.decide import process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.core.gate import evaluate
from instate.core.ledger import record_event
from instate.core.locks import get_entity_lock
from instate.core.models import Base, Event
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from tests.conftest import make_merchant_id, now_utc


class FakeReasoner:
    model_name = "fake-reasoner"

    async def propose(self, context: dict) -> dict | None:
        return {
            "action": "RETRY_NOW",
            "timing": "IMMEDIATE",
            "rationale": "transient timeout, retry immediately",
            "confidence": 0.9,
        }


class FakeGateway:
    def __init__(self):
        self.calls: list[dict] = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse("completed", provider_ref="ref", detail="")

    async def lookup(self, idempotency_key: str):
        return GatewayResponse("completed", provider_ref="ref")


async def _file_db(tmp_path):
    """One shared file DB — two sessions on it are truly concurrent."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/conc.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return engine, factory


async def _seed_below_ceiling(session, merchant, entity_id, *, now):
    """Policy/map/taxonomy; 2/3 retries in-window but outside 24h spacing."""
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    for i, back in enumerate([timedelta(days=3), timedelta(days=2)]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now - back,
            source_event_id=f"seed_retry_{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()


async def _retry_count(session, merchant, entity_id) -> int:
    result = await session.execute(
        select(func.count(Event.id)).where(
            Event.merchant_id == merchant,
            Event.entity_id == entity_id,
            Event.event_type == "RetryAttempted",
        )
    )
    return result.scalar_one()


async def test_concurrent_pipelines_serialize_on_one_entity(tmp_path):
    """Two concurrent failures at 2/3 → one executes, one DENY; never 4 attempts."""
    engine, factory = await _file_db(tmp_path)
    merchant = make_merchant_id()
    entity_id = "sub_race"
    now = now_utc()

    async with factory() as setup:
        await _seed_below_ceiling(setup, merchant, entity_id, now=now)
        trigger_a = await record_event(
            setup, merchant_id=merchant, entity_id=entity_id,
            entity_type="subscription", event_type="PaymentFailed",
            occurred_at=now, payload={"failure_code": "network_timeout"},
            source_event_id="race_a",
        )
        trigger_b = await record_event(
            setup, merchant_id=merchant, entity_id=entity_id,
            entity_type="subscription", event_type="PaymentFailed",
            occurred_at=now, payload={"failure_code": "network_timeout"},
            source_event_id="race_b",
        )
        await setup.commit()
        trigger_a_id, trigger_b_id = trigger_a.id, trigger_b.id

    reasoner = FakeReasoner()
    gateway = FakeGateway()

    async def run_one(trigger_id: int):
        async with factory() as s:
            event = await s.get(Event, trigger_id)
            return await process_failure(
                s, event=event, reasoner=reasoner, gateway=gateway, now=now,
            )

    results = await asyncio.gather(run_one(trigger_a_id), run_one(trigger_b_id))

    async with factory() as check:
        assert await _retry_count(check, merchant, entity_id) == 3

    paths = sorted(r.path for r in results)
    assert paths == ["gate1_deny", "llm"], f"expected one execution + one deny, got {paths}"
    assert len(gateway.calls) == 1

    await engine.dispose()


async def test_gates_alone_cannot_serialize_on_sqlite(tmp_path):
    """Negative control: bare evaluate() calls both ALLOW on SQLite; gates alone cannot serialize."""
    engine, factory = await _file_db(tmp_path)
    merchant = make_merchant_id()
    entity_id = "sub_bare"
    now = now_utc()

    async with factory() as setup:
        await _seed_below_ceiling(setup, merchant, entity_id, now=now)

    async def run_eval():
        async with factory() as s:
            return await evaluate(
                s, merchant_id=merchant, entity_id=entity_id,
                entity_type="subscription", action_class="RETRY_NOW",
                root_cause="network_timeout", now=now, record=False,
            )

    verdicts = await asyncio.gather(run_eval(), run_eval())
    assert [v.verdict for v in verdicts] == ["ALLOW", "ALLOW"]

    await engine.dispose()


async def test_locks_are_per_entity(tmp_path):
    """Locks are per-entity, not global."""
    merchant = make_merchant_id()
    assert get_entity_lock(merchant, "sub_x") is get_entity_lock(merchant, "sub_x")
    assert get_entity_lock(merchant, "sub_x") is not get_entity_lock(merchant, "sub_y")
    other_merchant = make_merchant_id()
    assert get_entity_lock(merchant, "sub_x") is not get_entity_lock(other_merchant, "sub_x")


async def test_concurrent_pipelines_on_different_entities_both_execute(tmp_path):
    """Different entities do not block each other."""
    engine, factory = await _file_db(tmp_path)
    merchant = make_merchant_id()
    now = now_utc()

    async with factory() as setup:
        await seed_default_policy(setup)
        await seed_default_diagnosis(setup)
        await seed_default_taxonomy(setup)
        for entity_id in ("sub_p", "sub_q"):
            for i, back in enumerate([timedelta(days=3), timedelta(days=2)]):
                await record_event(
                    setup, merchant_id=merchant, entity_id=entity_id,
                    entity_type="subscription", event_type="RetryAttempted",
                    occurred_at=now - back, source_event_id=f"{entity_id}_r{i}",
                )
            await record_event(
                setup, merchant_id=merchant, entity_id=entity_id,
                entity_type="subscription", event_type="PaymentFailed",
                occurred_at=now, payload={"failure_code": "network_timeout"},
                source_event_id=f"{entity_id}_trig",
            )
        await setup.commit()
        await fold_events(setup)
        await setup.commit()
        triggers = await setup.execute(
            select(Event).where(
                Event.merchant_id == merchant, Event.event_type == "PaymentFailed"
            )
        )
        trigger_ids = [e.id for e in triggers.scalars()]

    reasoner = FakeReasoner()
    gateway = FakeGateway()

    async def run_one(trigger_id: int):
        async with factory() as s:
            event = await s.get(Event, trigger_id)
            return await process_failure(
                s, event=event, reasoner=reasoner, gateway=gateway, now=now,
            )

    results = await asyncio.gather(*(run_one(t) for t in trigger_ids))

    assert sorted(r.path for r in results) == ["llm", "llm"]
    async with factory() as check:
        for entity_id in ("sub_p", "sub_q"):
            assert await _retry_count(check, merchant, entity_id) == 3

    await engine.dispose()


class BlockingGateway:
    """Slow gateway: blocks in execute() with intent committed but outcome unwritten."""

    def __init__(self):
        self.calls: list[dict] = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse("completed", provider_ref="ref", detail="")

    async def lookup(self, idempotency_key: str):
        return None


async def test_slow_gateway_cannot_reopen_the_race(tmp_path):
    """Slow gateway + concurrent pipelines still serializes via the app lock."""
    engine, factory = await _file_db(tmp_path)
    merchant = make_merchant_id()
    entity_id = "sub_slow"
    now = now_utc()

    async with factory() as setup:
        await _seed_below_ceiling(setup, merchant, entity_id, now=now)
        trigger_ids = []
        for tag in ("a", "b"):
            event = await record_event(
                setup, merchant_id=merchant, entity_id=entity_id,
                entity_type="subscription", event_type="PaymentFailed",
                occurred_at=now, payload={"failure_code": "network_timeout"},
                source_event_id=f"slow_{tag}",
            )
            trigger_ids.append(event.id)
        await setup.commit()

    reasoner = FakeReasoner()
    gateway = BlockingGateway()

    orig_execute = BlockingGateway.execute

    async def blocking_execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        await release.wait()
        return GatewayResponse("completed", provider_ref="ref", detail="")

    release = asyncio.Event()
    BlockingGateway.execute = blocking_execute
    try:
        async def run_one(trigger_id: int):
            async with factory() as s:
                event = await s.get(Event, trigger_id)
                return await process_failure(
                    s, event=event, reasoner=reasoner, gateway=gateway, now=now,
                )

        tasks = [asyncio.create_task(run_one(t)) for t in trigger_ids]
        # Second pipeline stays parked outside Gate-1 while the first is blocked.
        for _ in range(200):
            if len(gateway.calls) >= 1:
                break
            await asyncio.sleep(0.01)
        assert len(gateway.calls) == 1, (
            f"second pipeline entered execute while first was blocked: {gateway.calls}"
        )
        gateway.calls.clear()
        release.set()
        results = await asyncio.gather(*tasks)
    finally:
        BlockingGateway.execute = orig_execute

    async with factory() as check:
        assert await _retry_count(check, merchant, entity_id) == 3

    paths = sorted(r.path for r in results)
    assert paths == ["gate1_deny", "llm"], f"expected one execution + one deny, got {paths}"

    await engine.dispose()
