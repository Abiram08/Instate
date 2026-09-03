"""Tests for the console — the read-only memory wall.

A console that cannot act is the point: GET routes only, entity states,
windowed counters, and every decision's reason chain rendered.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from instate.core.models import Base, new_merchant_id
from instate.core.policy import seed_default_policy
from instate.surfaces.console import create_console_app


@pytest.fixture()
def console_client(tmp_path):
    from datetime import UTC, datetime

    from instate.core.gate import evaluate
    from instate.core.ledger import record_event
    from instate.core.projection import fold_events

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'console.db'}", poolclass=NullPool
    )

    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            merchant = new_merchant_id()
            await seed_default_policy(session)
            await record_event(
                session,
                merchant_id=merchant,
                entity_id="sub_wall",
                entity_type="subscription",
                event_type="PaymentFailed",
                occurred_at=datetime.now(UTC),
                payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
                source_event_id="c_wh_1",
            )
            await session.commit()
            await fold_events(session)
            # one decision so the wall has a chain to render
            await evaluate(
                session,
                merchant_id=merchant,
                entity_id="sub_wall",
                entity_type="subscription",
                action_class="RETRY_SCHEDULED",
                root_cause="insufficient_funds",
            )
            await session.commit()
        return factory, merchant

    import asyncio

    factory, merchant = asyncio.run(_setup())
    client = TestClient(create_console_app(factory))
    yield client, merchant
    asyncio.run(engine.dispose())


def test_index_lists_entities_with_counters(console_client):
    client, _ = console_client
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text
    assert "sub_wall" in html  # the entity link
    assert "retries · 7d" in html  # the counters the gates enforce
    assert "memory wall" in html


def test_index_has_no_forms_or_posts(console_client):
    """Read-only by construction: no forms in the markup, no POST routes."""
    client, _ = console_client
    html = client.get("/").text
    assert "<form" not in html
    post = client.post("/", content=b"x")
    assert post.status_code == 405  # Method Not Allowed


def test_entity_page_renders_chain_and_decision(console_client):
    client, merchant = console_client
    resp = client.get(f"/entity/{merchant}/sub_wall")
    assert resp.status_code == 200
    html = resp.text
    assert "PaymentFailed" in html  # the timeline
    assert "retry_ceiling_7d" in html  # the gate-1 chain
    assert "decision #" in html


def test_unknown_entity_is_404(console_client):
    client, merchant = console_client
    resp = client.get(f"/entity/{merchant}/ghost")
    assert resp.status_code == 404
