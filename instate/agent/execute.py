"""Instate execute — the outbox pattern (§6 step 5) and scheduling.

Intent → Execute → Commit:
  1. Write `ActionIntended` (with the idempotency key) and COMMIT — the
     intent must be durable BEFORE anything touches Razorpay.
  2. Call the gateway (timeout, ≤3 retries — the adapter's problem).
  3. Write `ActionCompleted` / `ActionFailed` + the outcome events, COMMIT.

On boot, any `ActionIntended` without a matching `ActionCompleted` is
reconciled (reconcile.py): the gateway is queried by the stored key —
found → completed, not found → safe to re-execute with the SAME key.
Kill the process mid-action on stage, restart it, watch it reconcile.

The idempotency key is deterministic (derived from the decision), and it
doubles as the intent event's `source_event_id` — so the ledger's UNIQUE
constraint itself prevents a double-intent.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse, PaymentGateway
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_AWAIT_PROMISE,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    Decision,
    HARD_DECLINE_ROOT_CAUSES,
    ScheduledAction,
)
from instate.core.projection import has_new_method_since_last_failure
from instate.core.locks import get_entity_lock


# ---------------------------------------------------------------------------
# Event mapping — action → the outcome events the fold understands
# ---------------------------------------------------------------------------

# The primary outcome event per action (what the projection folds)
OUTCOME_EVENT: dict[str, str] = {
    ACTION_RETRY_NOW: "RetryAttempted",
    ACTION_RETRY_SCHEDULED: "RetryScheduled",
    ACTION_SEND_PAYMENT_LINK: "PaymentLinkSent",
    ACTION_REQUEST_PAYMENT_METHOD: "PaymentLinkSent",
    ACTION_UPDATE_MANDATE: "RecoveryActionSent",
    ACTION_AWAIT_PROMISE: "RecoveryActionSent",
}

# Channel recorded on CustomerContacted (contact-frequency caps key on it)
ACTION_CHANNEL: dict[str, str] = {
    ACTION_SEND_PAYMENT_LINK: "payment_link",
    ACTION_REQUEST_PAYMENT_METHOD: "payment_link",
    ACTION_UPDATE_MANDATE: "mandate_update",
    ACTION_AWAIT_PROMISE: "message",
}


def make_idempotency_key(merchant_id: UUID, entity_id: str, decision_id: int | None) -> str:
    """Deterministic key — derived from the decision, so a crash+reconcile
    lands on the same key without needing to remember anything."""
    return f"{merchant_id}:{entity_id}:d{decision_id or 0}"


# ---------------------------------------------------------------------------
# Timing — proposal timing → due date for RETRY_SCHEDULED
# ---------------------------------------------------------------------------


def parse_timing(timing: str | None, now: datetime) -> timedelta:
    """`T_PLUS_48H` / `T_PLUS_2D` / `IMMEDIATE` → timedelta.

    Unrecognized timings fall back to T_PLUS_24H — scheduling a retry is
    never blocked by a wording surprise. `NEXT_PAYDAY` is a Stage-4
    refinement (learned from prior payment timestamps).
    """
    if not timing:
        return timedelta(hours=24)
    t = timing.strip().upper()
    if t == "IMMEDIATE":
        return timedelta(0)
    if t.startswith("T_PLUS_") and t.endswith("H"):
        try:
            return timedelta(hours=int(t[7:-1]))
        except ValueError:
            pass
    if t.startswith("T_PLUS_") and t.endswith("D"):
        try:
            return timedelta(days=int(t[7:-1]))
        except ValueError:
            pass
    return timedelta(hours=24)


# ---------------------------------------------------------------------------
# The outbox
# ---------------------------------------------------------------------------


async def write_intent(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    occurred_at: datetime | None = None,
) -> str:
    """Step 1 — write the intent, durably, BEFORE the gateway call.

    Returns the idempotency key. The intent event's source_event_id IS
    the key, so the ledger's UNIQUE constraint makes a double-intent
    impossible. Commits — this is the whole point of the pattern.
    """
    occurred_at = occurred_at or datetime.now(UTC)
    key = make_idempotency_key(merchant_id, entity_id, decision.id)

    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="ActionIntended",
        occurred_at=occurred_at,
        payload={"action": action, "idempotency_key": key, "decision_id": decision.id},
        source_event_id=key,  # UNIQUE → double-intent is impossible
        decision_id=decision.id,
    )
    await session.commit()  # durable BEFORE the gateway call
    return key


async def execute_action(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    proposal: dict | None = None,
    now: datetime | None = None,
) -> GatewayResponse:
    """Intent → execute → commit, for actions that hit the gateway NOW.

    The outcome events (fold-visible) and the decision's executed_action
    are written after the call. Gateway "unknown" (timeout/5xx) still
    writes ActionCompleted-less intent — reconciliation owns it.
    """
    now = now or datetime.now(UTC)
    key = await write_intent(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision=decision,
        action=action,
        occurred_at=now,
    )

    response = await gateway.execute(
        action,
        entity_id=entity_id,
        idempotency_key=key,
        payload=(proposal or {}).get("payload"),
    )

    await commit_outcome(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        decision=decision,
        action=action,
        key=key,
        response=response,
        now=now,
    )
    return response


async def commit_outcome(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    action: str,
    key: str,
    response: GatewayResponse,
    now: datetime | None = None,
) -> None:
    """Step 3 — write the completion/failed event and the outcome events.

    "completed"/"failed" → ActionCompleted/ActionFailed + outcome events.
    "unknown" → nothing here; the intent stands and reconciliation
    resolves it (exactly-once semantics, §10).
    """
    now = now or datetime.now(UTC)

    if response.status == "completed":
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="ActionCompleted",
            occurred_at=now,
            payload={
                "idempotency_key": key,
                "action": action,
                "provider_ref": response.provider_ref,
            },
            source_event_id=f"{key}:done",
            decision_id=decision.id,
        )
        await _write_outcome_events(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            action=action,
            decision=decision,
            occurred_at=now,
            success=True,
            amount_minor=response.amount_minor,
        )
    elif response.status == "failed":
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="ActionFailed",
            occurred_at=now,
            payload={
                "idempotency_key": key,
                "action": action,
                "detail": response.detail,
            },
            source_event_id=f"{key}:failed",
            decision_id=decision.id,
        )
        await _write_outcome_events(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            action=action,
            decision=decision,
            occurred_at=now,
            success=False,
        )
    # status == "unknown": the intent stands; reconciliation resolves it

    decision.executed_action = action
    await session.commit()


async def _write_outcome_events(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    action: str,
    decision: Decision,
    occurred_at: datetime,
    success: bool,
    amount_minor: int | None = None,
) -> None:
    """The fold-visible outcome events, linked to the decision."""
    key = make_idempotency_key(merchant_id, entity_id, decision.id)
    outcome_type = OUTCOME_EVENT.get(action)
    if outcome_type is not None:
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type=outcome_type,
            occurred_at=occurred_at,
            payload={"action": action, "success": success,
                     "decision_id": decision.id},
            source_event_id=f"{key}:{outcome_type}",
            decision_id=decision.id,
        )
        # A successful retry is RECOVERED MONEY (§11) — the amount rides
        # on RetrySucceeded so the net-money metric can net it against
        # RecoveryReversed later.
        if success and action == ACTION_RETRY_NOW and amount_minor is not None:
            await record_event(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                event_type="RetrySucceeded",
                occurred_at=occurred_at,
                payload={"amount_minor": amount_minor,
                         "decision_id": decision.id},
                source_event_id=f"{key}:recovered",
                decision_id=decision.id,
            )

    channel = ACTION_CHANNEL.get(action)
    if channel is not None:
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="CustomerContacted",
            occurred_at=occurred_at,
            payload={"channel": channel, "action": action, "decision_id": decision.id},
            source_event_id=f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:contact",
            decision_id=decision.id,
        )


# ---------------------------------------------------------------------------
# Escalation — the deterministic stop (no gateway, zero tokens)
# ---------------------------------------------------------------------------


async def escalate_to_human(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    reason: str,
    now: datetime | None = None,
) -> None:
    """Write EscalatedToHuman and mark the decision. The stop-path for
    Gate-1 DENY, Gate-2 DENY/REQUIRE_HUMAN, and fixed-action routes."""
    now = now or datetime.now(UTC)
    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="EscalatedToHuman",
        occurred_at=now,
        payload={"reason": reason, "decision_id": decision.id},
        source_event_id=f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:esc",
        decision_id=decision.id,
    )
    decision.executed_action = "ESCALATE_HUMAN"
    await session.commit()


# ---------------------------------------------------------------------------
# Scheduling — RETRY_SCHEDULED lands in the durable queue (§14)
# ---------------------------------------------------------------------------


async def schedule_retry(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    decision: Decision,
    timing: str | None,
    root_cause: str | None = None,
    now: datetime | None = None,
) -> ScheduledAction:
    """The only action that doesn't touch Razorpay at decision time:
    it lands in `scheduled_actions`, and the tick loop executes it when
    due (as a RETRY_NOW through the same outbox). `root_cause` rides
    along so the due scan can defer hard-decline retries (§6)."""
    now = now or datetime.now(UTC)
    due_at = now + parse_timing(timing, now)
    key = f"{make_idempotency_key(merchant_id, entity_id, decision.id)}:sched"

    # Uniqueness is on idempotency_key — look up by key, never re-schedule
    result = await session.execute(
        select(ScheduledAction).where(ScheduledAction.idempotency_key == key)
    )
    existing = result.scalar_one_or_none()
    if existing is not None:
        return existing

    scheduled = ScheduledAction(
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        action=ACTION_RETRY_NOW,  # a due scheduled retry IS a retry now
        due_at=due_at,
        idempotency_key=key,
        decision_id=decision.id,
        payload={"timing": timing, "root_cause": root_cause},
    )
    session.add(scheduled)
    await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="RetryScheduled",
        occurred_at=now,
        payload={"due_at": due_at.isoformat(), "timing": timing, "decision_id": decision.id},
        source_event_id=f"{key}:schedevent",
        decision_id=decision.id,
    )
    decision.executed_action = "RETRY_SCHEDULED"
    await session.commit()
    return scheduled


async def run_due_scheduled(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    now: datetime | None = None,
) -> int:
    """Tick-loop half: execute every due scheduled action through the
    outbox (as RETRY_NOW), marking the queue row so it never re-fires."""
    now = now or datetime.now(UTC)
    result = await session.execute(
        select(ScheduledAction).where(
            ScheduledAction.due_at <= now,
            ScheduledAction.executed_at.is_(None),
        )
    )
    due = list(result.scalars().all())

    executed = 0
    for scheduled in due:
        # Serialized per entity like every other gate→intent span: a
        # concurrent pipeline for this entity waits here instead of
        # racing the outbox.
        async with get_entity_lock(scheduled.merchant_id, scheduled.entity_id):
            if await _run_one_scheduled(session, gateway=gateway, scheduled=scheduled, now=now):
                executed += 1
    return executed


async def _run_one_scheduled(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    scheduled: ScheduledAction,
    now: datetime,
) -> bool:
    """Execute one due row. Returns True if it fired (vs deferred/missing)."""
    decision = (
        await session.get(Decision, scheduled.decision_id)
        if scheduled.decision_id is not None
        else None
    )
    if decision is None:
        scheduled.executed_at = now
        await session.commit()
        return False

    # Hard-decline deferral (§6, Stripe lesson): a scheduled retry for a
    # hard-declined method stays queued until a PaymentMethodChanged
    # event unblocks it. Unexecuted retries create no charge.
    root_cause = (scheduled.payload or {}).get("root_cause")
    if root_cause in HARD_DECLINE_ROOT_CAUSES and not await (
        has_new_method_since_last_failure(
            session,
            merchant_id=scheduled.merchant_id,
            entity_id=scheduled.entity_id,
        )
    ):
        return False  # stays pending — the next tick re-checks

    await execute_action(
        session,
        gateway=gateway,
        merchant_id=scheduled.merchant_id,
        entity_id=scheduled.entity_id,
        entity_type=scheduled.entity_type,
        decision=decision,
        action=scheduled.action,  # RETRY_NOW
        proposal={"payload": scheduled.payload},
        now=now,
    )
    scheduled.executed_at = now
    await session.commit()
    return True
