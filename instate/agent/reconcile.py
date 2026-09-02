"""Instate reconcile — the boot-time failure demo (§6 step 5, §10).

On boot, any `ActionIntended` without a matching `ActionCompleted` is
reconciled by querying Razorpay with the stored idempotency key:

  - gateway says "completed" → write ActionCompleted (the work happened;
    the crash only lost the receipt)
  - gateway says "failed"    → write ActionFailed
  - gateway knows nothing    → the action never landed; safe to re-execute
    with the SAME key (exactly-once semantics)

Kill the process mid-action on stage, restart it, watch it reconcile.
That is the "one failure handled gracefully" demo, and it is a genuinely
correct pattern rather than a stunt.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import PaymentGateway
from instate.agent.execute import commit_outcome
from instate.core.ledger import record_event
from instate.core.locks import get_entity_lock
from instate.core.models import Decision, Event
from instate.core.projection import fold_events


async def reconcile_pending(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
) -> int:
    """Reconcile every dangling intent. Returns how many were resolved.

    Matching is by `payload.idempotency_key` computed in Python (demo
    scale: scanning the intents is sub-ms; production adds an index or a
    `status` projection — the pattern is unchanged).
    """
    intents = await session.execute(
        select(Event).where(Event.event_type == "ActionIntended").order_by(Event.id.asc())
    )
    intent_events = list(intents.scalars().all())
    if not intent_events:
        return 0

    completions = await session.execute(
        select(Event.payload).where(Event.event_type.in_(["ActionCompleted", "ActionFailed"]))
    )
    resolved_keys: set[str] = set()
    for (payload,) in completions.all():
        if payload and payload.get("idempotency_key"):
            resolved_keys.add(payload["idempotency_key"])

    resolved = 0
    for intent in intent_events:
        payload = intent.payload or {}
        key = payload.get("idempotency_key")
        if not key or key in resolved_keys:
            continue

        # Same per-entity serialization as every other gate→intent span.
        async with get_entity_lock(intent.merchant_id, intent.entity_id):
            if await _reconcile_intent(session, gateway=gateway, intent=intent, key=key):
                resolved += 1

    return resolved


async def _reconcile_intent(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    intent: Event,
    key: str,
) -> bool:
    """Resolve one dangling intent. Returns True if it was resolved."""
    payload = intent.payload or {}
    action = payload.get("action", "UNKNOWN")
    decision = (
        await session.get(Decision, intent.decision_id)
        if intent.decision_id is not None
        else None
    )
    if decision is None:
        # An intent whose decision row is gone: write a sentinel
        # completion so it never re-flags (auditability over silence)
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
        return True

    # Ask Razorpay what actually happened, by OUR key
    remote = await gateway.lookup(key)

    if remote is None:
        # Never landed — re-execute with the SAME key (exactly-once)
        response = await gateway.execute(
            action,
            entity_id=intent.entity_id,
            idempotency_key=key,
        )
    else:
        response = remote

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
    return True
