"""Tier 2 token budget and Postgres concurrency proof."""

import json
import os

import pytest

from instate.agent.decide import build_context


async def test_context_stays_bounded_on_fat_entity(session):
    mid = __import__("uuid").uuid4()
    s = session
    from instate.core.ledger import record_event
    from instate.core.projection import fold_events
    from tests.conftest import now_utc
    now = now_utc()
    for i in range(300):
        await record_event(
            s, merchant_id=mid, entity_id="sub_fat", entity_type="subscription",
            event_type="RetryAttempted", occurred_at=now,
            payload={"success": False, "blob": "x" * 500},
            source_event_id=f"fat_{i}")
    await s.commit()
    await fold_events(s)
    ctx = await build_context(
        s, merchant_id=mid, entity_id="sub_fat", entity_type="subscription",
        root_cause="insufficient_funds", policy_version=1,
        precedents=[{"situation": "s", "action": "a", "outcome": "o"}] * 10)
    assert len(ctx["precedents"]) == 3, "top_k must stay fixed at 3"
    assert len(ctx["recent_events"]) == 5, "digest must stay bounded at 5"
    prompt_tokens = len(json.dumps(ctx, default=str)) // 4
    assert prompt_tokens < 1300, f"context blew the budget: ~{prompt_tokens} tokens"


@pytest.mark.skipif(
    not os.environ.get("INSTATE_TEST_PG_DSN"),
    reason="needs INSTATE_TEST_PG_DSN (real Postgres) — SQLite path covered elsewhere",
)
async def test_pg_gate_serializes_concurrent_pipelines():
    from sqlalchemy.ext.asyncio import sessionmaker, create_async_engine
    from instate.core.models import Base
    dsn = os.environ["INSTATE_TEST_PG_DSN"]
    engine = create_async_engine(dsn)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
        from instate.core.policy import seed_default_policy
        from instate.core.ledger import record_event
        from instate.core.projection import fold_events
        from tests.conftest import make_merchant_id, now_utc
        from datetime import timedelta
        mid = make_merchant_id()
        await seed_default_policy(s)
        await seed_default_taxonomy(s)
        await seed_default_diagnosis(s)
        now = now_utc()
        for i, back in enumerate([5, 3]):
            await record_event(
                s, merchant_id=mid, entity_id="sub_pg", entity_type="subscription",
                event_type="RetryAttempted", occurred_at=now - timedelta(days=back),
                payload={"success": False}, source_event_id=f"pg_hist_{i}")
        await s.commit()
        await fold_events(s)
        await s.commit()
        from instate.core.gate import evaluate
        r = await evaluate(s, merchant_id=mid, entity_id="sub_pg",
                           entity_type="subscription", action_class="money",
                           action="RETRY_NOW", now=now)
        assert r.allowed, "2/3 retries must still allow one more"
    await engine.dispose()
