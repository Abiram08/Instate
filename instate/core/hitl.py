"""HITL queue — assignment, SLA, resolution writes back to L0 (§15).

Escalations are not dead-ends: a human resolves, the resolution event
feeds L3, so the memory learns from people.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import (
    STATUS_RECOVERED,
    STATUS_WRITTEN_OFF,
    EntityState,
    HitlTask,
)
from instate.core.projection import fold_events

TERMINAL_STATUSES = {STATUS_RECOVERED, STATUS_WRITTEN_OFF}


async def _current_status(
    session: AsyncSession, merchant_id, entity_id: str
) -> str | None:
    state = await session.get(EntityState, (merchant_id, entity_id))
    return state.status if state else None


async def enqueue_escalation(
    session: AsyncSession,
    *,
    merchant_id,
    entity_id: str,
    entity_type: str = "subscription",
    decision_id: int | None = None,
    reason: str,
    sla_hours: int = 24,
) -> HitlTask | None:
    """Enqueue a human task — unless the entity is already terminal.

    A RECOVERED (or WRITTEN_OFF) entity has nothing to escalate: without
    this guard the task would sit `open` forever, breach SLA, and a later
    resolve would write a phantom second HumanResolved. Returns None
    when there is nothing to escalate.
    """
    if await _current_status(session, merchant_id, entity_id) in TERMINAL_STATUSES:
        return None
    task = HitlTask(
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision_id=decision_id,
        reason=reason,
        sla_due_at=datetime.now(UTC) + timedelta(hours=sla_hours),
    )
    session.add(task)
    await session.flush()
    return task


async def claim_task(session: AsyncSession, task_id: int, assignee: str) -> HitlTask | None:
    task = await session.get(HitlTask, task_id)
    if task is None or task.status != "open":
        return None
    task.assignee = assignee
    task.status = "claimed"
    task.updated_at = datetime.now(UTC)
    await session.flush()
    return task


async def resolve_task(
    session: AsyncSession,
    task_id: int,
    action: str,
    payload: dict | None = None,
) -> HitlTask | None:
    """Human resolves: write back to L0 (HumanResolved / PromiseMade etc.)
    so L3 precedent captures it.

    Race-safe: if the entity reached a terminal status between enqueue
    and resolve (e.g. a scheduled retry landed first), the task still
    closes — but as an explicit no-op, with NO duplicate ledger event.
    """
    task = await session.get(HitlTask, task_id)
    if task is None or task.status not in ("open", "claimed"):
        return None
    terminal = await _current_status(session, task.merchant_id, task.entity_id)
    if terminal in TERMINAL_STATUSES:
        task.status = "resolved"
        task.resolution_action = f"noop_already_{terminal.lower()}"
        task.resolution_payload = payload or {}
        task.updated_at = datetime.now(UTC)
        await session.flush()
        return task
    task.status = "resolved"
    task.resolution_action = action
    task.resolution_payload = payload or {}
    task.updated_at = datetime.now(UTC)

    # Write-back: the human outcome becomes a ledger event
    event_type = {"recovered": "HumanResolved", "promise": "PromiseMade"}.get(action, "HumanResolved")
    await record_event(
        session,
        merchant_id=task.merchant_id,
        entity_id=task.entity_id,
        entity_type=task.entity_type,
        event_type=event_type,
        occurred_at=datetime.now(UTC),
        payload={"hitl_task_id": task.id, "action": action, **(payload or {})},
        source_event_id=f"hitl_{task.id}_resolved",
        decision_id=task.decision_id,
    )
    # fold to update L1
    await session.flush()
    await fold_events(session)
    await session.flush()
    return task


async def sla_breaches(session: AsyncSession, now: datetime | None = None) -> list[HitlTask]:
    now = now or datetime.now(UTC)
    q = select(HitlTask).where(HitlTask.status.in_(["open", "claimed"]), HitlTask.sla_due_at < now)
    return list((await session.execute(q)).scalars().all())
