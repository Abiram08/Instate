"""Boot-time outbox reconciliation.
Dangling ActionIntended rows are resolved via gateway lookup by idempotency key.
Completed/failed → write receipt; unknown → re-execute with the same key.
"""

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import PaymentGateway
from instate.agent.execute import commit_outcome
from instate.core.ledger import record_event
from instate.core.locks import get_entity_lock
from instate.core.models import Decision, Event
from instate.core.projection import fold_events


@dataclass
class ReconciledIntent:
    entity_id: str
    action: str
    via: str  # "lookup" | "re-executed" | "sentinel"
    status: str  # gateway status, or "completed" for the sentinel receipt


async def find_dangling_intents(session: AsyncSession) -> list[Event]:
    """Intents with no matching ActionCompleted/ActionFailed receipt."""
    intents = await session.execute(
        select(Event).where(Event.event_type == "ActionIntended").order_by(Event.id.asc())
    )
    intent_events = list(intents.scalars().all())
    if not intent_events:
        return []

    completions = await session.execute(
        select(Event.payload).where(Event.event_type.in_(["ActionCompleted", "ActionFailed"]))
    )
    resolved_keys: set[str] = set()
    for (payload,) in completions.all():
        if payload and payload.get("idempotency_key"):
            resolved_keys.add(payload["idempotency_key"])

    return [
        i
        for i in intent_events
        if (i.payload or {}).get("idempotency_key")
        and (i.payload or {}).get("idempotency_key") not in resolved_keys
    ]


async def reconcile_one(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    intent: Event,
) -> ReconciledIntent:
    """Resolve one dangling intent under the per-entity lock."""
    key = (intent.payload or {}).get("idempotency_key", "")
    async with get_entity_lock(intent.merchant_id, intent.entity_id):
        return await _reconcile_intent(session, gateway=gateway, intent=intent, key=key)


async def reconcile_pending(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    report: list[ReconciledIntent] | None = None,
) -> int:
    """Reconcile every dangling intent. Returns count resolved.

    A gateway that *raises* (instead of returning unknown) never kills the
    boot: the intent is left dangling for the next run and reported with
    via="deferred". Intents are never lost to a gateway explosion.
    """
    resolved = 0
    for intent in await find_dangling_intents(session):
        # Snapshot immutable event data first: a rollback below expires the
        # ORM object, and touching expired attrs after that is a greenlet trap.
        entity_id, action = intent.entity_id, (intent.payload or {}).get("action", "UNKNOWN")
        try:
            detail = await reconcile_one(session, gateway=gateway, intent=intent)
        except Exception:  # noqa: BLE001 — gateways lie; the ledger must not
            await session.rollback()
            detail = ReconciledIntent(entity_id, action, "deferred", "unknown")
            if report is not None:
                report.append(detail)
            continue
        if report is not None:
            report.append(detail)
        resolved += 1

    return resolved


async def _reconcile_intent(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    intent: Event,
    key: str,
) -> ReconciledIntent:
    payload = intent.payload or {}
    action = payload.get("action", "UNKNOWN")
    decision = (
        await session.get(Decision, intent.decision_id)
        if intent.decision_id is not None
        else None
    )
    if decision is None:
        # Missing decision: write sentinel completion so it never re-flags.
        await record_event(
            session,
            merchant_id=intent.merchant_id,
            entity_id=intent.entity_id,
            entity_type=intent.entity_type,
            event_type="ActionCompleted",
            occurred_at=intent.occurred_at,
            payload={
                "idempotency_key": key,
                "action": action,
                "detail": "reconciled: decision row missing",
            },
            source_event_id=f"{key}:done",
            decision_id=None,
        )
        await session.commit()
        return ReconciledIntent(intent.entity_id, action, "sentinel", "completed")

    remote = await gateway.lookup(key)

    if remote is None:
        # Never landed — re-execute with the same key.
        response = await gateway.execute(
            action,
            entity_id=intent.entity_id,
            idempotency_key=key,
        )
        via = "re-executed"
    else:
        response = remote
        via = "lookup"

    await commit_outcome(
        session,
        merchant_id=intent.merchant_id,
        entity_id=intent.entity_id,
        entity_type=intent.entity_type,
        decision=decision,
        action=action,
        key=key,
        response=response,
        now=intent.occurred_at,
    )
    await fold_events(session)
    return ReconciledIntent(intent.entity_id, action, via, response.status)
