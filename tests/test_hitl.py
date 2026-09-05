"""HITL queue orphan and phantom-resolution guards."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.hitl import claim_task, enqueue_escalation, resolve_task, sla_breaches
from instate.core.ledger import record_event
from instate.core.models import Event, HitlTask
from instate.core.projection import fold_events
from tests.conftest import make_merchant_id, now_utc


async def _failed_entity(session: AsyncSession, merchant, entity_id: str):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id=f"{entity_id}_wh",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()


async def test_enqueue_is_refused_for_recovered_entity(session: AsyncSession):
    merchant = make_merchant_id()
    await _failed_entity(session, merchant, "sub_done")
    await record_event(
        session, merchant_id=merchant, entity_id="sub_done",
        entity_type="subscription", event_type="RetrySucceeded",
        occurred_at=now_utc(), payload={"amount_minor": 49900},
        source_event_id="sub_done_ok",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    task = await enqueue_escalation(
        session, merchant_id=merchant, entity_id="sub_done", reason="stale_page"
    )
    assert task is None

    rows = await session.execute(select(HitlTask))
    assert list(rows.scalars().all()) == []


async def test_enqueue_works_for_open_entity(session: AsyncSession):
    merchant = make_merchant_id()
    await _failed_entity(session, merchant, "sub_open")

    task = await enqueue_escalation(
        session, merchant_id=merchant, entity_id="sub_open", reason="needs_human"
    )
    await session.commit()

    assert task is not None
    assert task.status == "open"
    assert task.sla_due_at is not None


async def test_resolve_after_recovery_is_noop_without_phantom_event(session: AsyncSession):
    merchant = make_merchant_id()
    await _failed_entity(session, merchant, "sub_race")

    task = await enqueue_escalation(
        session, merchant_id=merchant, entity_id="sub_race", reason="needs_human"
    )
    await session.commit()
    await claim_task(session, task.id, assignee="ops-ada")
    await session.commit()

    await record_event(
        session, merchant_id=merchant, entity_id="sub_race",
        entity_type="subscription", event_type="RetrySucceeded",
        occurred_at=now_utc(), payload={"amount_minor": 49900},
        source_event_id="sub_race_ok",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    resolved = await resolve_task(session, task.id, "recovered", payload={"by": "ops-ada"})
    await session.commit()

    assert resolved is not None
    assert resolved.status == "resolved"
    assert resolved.resolution_action == "noop_already_recovered"

    events = await session.execute(
        select(Event.event_type).where(
            Event.merchant_id == merchant, Event.entity_id == "sub_race"
        )
    )
    types = [r[0] for r in events.all()]
    assert types.count("HumanResolved") == 0
    assert types.count("RetrySucceeded") == 1


async def test_normal_resolve_writes_back_to_ledger(session: AsyncSession):
    merchant = make_merchant_id()
    await _failed_entity(session, merchant, "sub_fix")

    task = await enqueue_escalation(
        session, merchant_id=merchant, entity_id="sub_fix", reason="needs_human"
    )
    await session.commit()

    resolved = await resolve_task(session, task.id, "recovered", payload={"by": "ops-ada"})
    await session.commit()

    assert resolved.status == "resolved"
    assert resolved.resolution_action == "recovered"
    events = await session.execute(
        select(Event.event_type).where(
            Event.merchant_id == merchant, Event.entity_id == "sub_fix"
        )
    )
    assert "HumanResolved" in [r[0] for r in events.all()]


async def test_sla_breach_lists_only_open_tasks(session: AsyncSession):
    from datetime import timedelta

    merchant = make_merchant_id()
    await _failed_entity(session, merchant, "sub_sla")

    task = await enqueue_escalation(
        session, merchant_id=merchant, entity_id="sub_sla",
        reason="slow", sla_hours=0,
    )
    await session.commit()

    breaches = await sla_breaches(session, now=now_utc() + timedelta(seconds=1))
    assert [t.id for t in breaches] == [task.id]

    await resolve_task(session, task.id, "recovered")
    await session.commit()
    assert await sla_breaches(session, now=now_utc() + timedelta(seconds=1)) == []
