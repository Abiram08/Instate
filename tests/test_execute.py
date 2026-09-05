"""Outbox and boot reconciler tests: intent-before-call ordering and exactly-once recovery."""

from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.agent.execute import (
    escalate_to_human,
    execute_action,
    make_idempotency_key,
    parse_timing,
    write_intent,
)
from instate.agent.reconcile import reconcile_pending
from instate.core.ledger import DuplicateEventError, record_event
from instate.core.models import Decision, Event
from instate.core.policy import seed_default_policy
from tests.conftest import make_merchant_id, now_utc


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeGateway:
    def __init__(self, status: str = "completed", known: dict | None = None):
        self.status = status
        self.known = known or {}  # idempotency_key → GatewayResponse
        self.calls: list[dict] = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append(
            {"action": action, "entity_id": entity_id, "idempotency_key": idempotency_key}
        )
        return GatewayResponse(self.status, provider_ref=f"ref_{len(self.calls)}")

    async def lookup(self, idempotency_key: str):
        return self.known.get(idempotency_key)


async def new_decision(session: AsyncSession, merchant, entity_id: str) -> Decision:
    """A decision row (as Gate-1 would have created it)."""
    await seed_default_policy(session)
    await session.commit()
    decision = Decision(merchant_id=merchant, entity_id=entity_id, root_cause="x")
    session.add(decision)
    await session.flush()
    return decision


async def event_types_of(session, merchant, entity_id) -> list[str]:
    result = await session.execute(
        select(Event.event_type)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .order_by(Event.id.asc())
    )
    return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# parse_timing
# ---------------------------------------------------------------------------


def test_parse_timing():
    now = now_utc()
    assert parse_timing("IMMEDIATE", now) == timedelta(0)
    assert parse_timing("T_PLUS_48H", now) == timedelta(hours=48)
    assert parse_timing("t_plus_2d", now) == timedelta(days=2)


def test_parse_timing_fallback():
    """A wording surprise never blocks scheduling: unknown → 24h."""
    assert parse_timing("WHENEVER", now_utc()) == timedelta(hours=24)
    assert parse_timing(None, now_utc()) == timedelta(hours=24)


def test_make_idempotency_key_is_deterministic():
    merchant = make_merchant_id()
    a = make_idempotency_key(merchant, "sub_1", 42)
    b = make_idempotency_key(merchant, "sub_1", 42)
    assert a == b
    assert str(merchant) in a and "d42" in a


# ---------------------------------------------------------------------------
# Outbox
# ---------------------------------------------------------------------------


async def test_intent_is_written_and_committed_before_gateway(session: AsyncSession):
    """Intent is committed before the gateway call, once with deterministic key."""
    merchant = make_merchant_id()
    gateway = FakeGateway(status="completed")
    decision = await new_decision(session, merchant, "sub_outbox")

    response = await execute_action(
        session,
        gateway=gateway,
        merchant_id=merchant,
        entity_id="sub_outbox",
        entity_type="subscription",
        decision=decision,
        action="RETRY_NOW",
    )

    assert response.status == "completed"
    assert len(gateway.calls) == 1
    key = gateway.calls[0]["idempotency_key"]
    assert key == make_idempotency_key(merchant, "sub_outbox", decision.id)

    types = await event_types_of(session, merchant, "sub_outbox")
    assert types == ["ActionIntended", "ActionCompleted", "RetryAttempted"]

    intent = await session.execute(select(Event).where(Event.event_type == "ActionIntended"))
    intent_event = intent.scalar_one()
    assert intent_event.source_event_id == key
    assert intent_event.decision_id == decision.id


async def test_gateway_unknown_leaves_intent_dangling(session: AsyncSession):
    """Unknown gateway status leaves intent dangling for reconciliation."""
    merchant = make_merchant_id()
    gateway = FakeGateway(status="unknown")
    decision = await new_decision(session, merchant, "sub_unknown")

    response = await execute_action(
        session,
        gateway=gateway,
        merchant_id=merchant,
        entity_id="sub_unknown",
        entity_type="subscription",
        decision=decision,
        action="SEND_PAYMENT_LINK",
    )

    assert response.status == "unknown"
    types = await event_types_of(session, merchant, "sub_unknown")
    assert "ActionIntended" in types
    assert "ActionCompleted" not in types


async def test_contact_actions_write_customer_contacted(session: AsyncSession):
    """Link/mandate actions also write CustomerContacted with channel."""
    merchant = make_merchant_id()
    gateway = FakeGateway(status="completed")
    decision = await new_decision(session, merchant, "sub_link")

    await execute_action(
        session,
        gateway=gateway,
        merchant_id=merchant,
        entity_id="sub_link",
        entity_type="subscription",
        decision=decision,
        action="SEND_PAYMENT_LINK",
    )

    types = await event_types_of(session, merchant, "sub_link")
    assert "PaymentLinkSent" in types
    assert "CustomerContacted" in types


async def test_write_intent_is_idempotent(session: AsyncSession):
    """Second intent with same key hits UNIQUE constraint."""
    merchant = make_merchant_id()
    decision = await new_decision(session, merchant, "sub_dup")

    await write_intent(
        session,
        merchant_id=merchant,
        entity_id="sub_dup",
        entity_type="subscription",
        decision=decision,
        action="RETRY_NOW",
    )

    with pytest.raises(DuplicateEventError):
        await write_intent(
            session,
            merchant_id=merchant,
            entity_id="sub_dup",
            entity_type="subscription",
            decision=decision,
            action="RETRY_NOW",
        )


async def test_escalation_writes_event_and_marks_decision(session: AsyncSession):
    merchant = make_merchant_id()
    decision = await new_decision(session, merchant, "sub_esc")

    await escalate_to_human(
        session,
        merchant_id=merchant,
        entity_id="sub_esc",
        entity_type="subscription",
        decision=decision,
        reason="gate1_deny",
    )

    assert decision.executed_action == "ESCALATE_HUMAN"
    types = await event_types_of(session, merchant, "sub_esc")
    assert types == ["EscalatedToHuman"]


# ---------------------------------------------------------------------------
# Boot reconciler
# ---------------------------------------------------------------------------


async def _dangling_intent(session, merchant, entity_id: str, action="SEND_PAYMENT_LINK"):
    """An intent whose process died before the outcome was written."""
    decision = await new_decision(session, merchant, entity_id)
    key = await write_intent(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        decision=decision,
        action=action,
    )
    return decision, key


async def test_reconciler_completes_known_work(session: AsyncSession):
    """Crash after accept, before receipt → reconcile completes without re-calling."""
    merchant = make_merchant_id()
    decision, key = await _dangling_intent(session, merchant, "sub_recon")
    gateway = FakeGateway(known={key: GatewayResponse("completed", provider_ref="rp_123")})

    resolved = await reconcile_pending(session, gateway=gateway)

    assert resolved == 1
    assert gateway.calls == []  # never re-executed — the work was already done
    types = await event_types_of(session, merchant, "sub_recon")
    assert "ActionCompleted" in types
    assert "PaymentLinkSent" in types


async def test_reconciler_reexecutes_unknown_work_with_same_key(session: AsyncSession):
    """Crash before accept → re-execute with same idempotency key."""
    merchant = make_merchant_id()
    decision, key = await _dangling_intent(session, merchant, "sub_recon2")
    gateway = FakeGateway(known={})  # Razorpay knows nothing about it

    resolved = await reconcile_pending(session, gateway=gateway)

    assert resolved == 1
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["idempotency_key"] == key  # the SAME key
    types = await event_types_of(session, merchant, "sub_recon2")
    assert "ActionCompleted" in types


async def test_reconciler_ignores_resolved_intents(session: AsyncSession):
    """Intents that already have their receipt are never touched."""
    merchant = make_merchant_id()
    decision, key = await _dangling_intent(session, merchant, "sub_done")

    # The receipt exists (the process died AFTER writing it)
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_done",
        entity_type="subscription",
        event_type="ActionCompleted",
        occurred_at=now_utc(),
        payload={"idempotency_key": key},
        source_event_id=f"{key}:done",
        decision_id=decision.id,
    )
    await session.commit()

    gateway = FakeGateway()
    resolved = await reconcile_pending(session, gateway=gateway)

    assert resolved == 0
    assert gateway.calls == []


async def test_reconciler_noop_on_clean_boot(session: AsyncSession):
    merchant = make_merchant_id()
    await new_decision(session, merchant, "sub_clean")
    resolved = await reconcile_pending(session, gateway=FakeGateway())
    assert resolved == 0


async def test_full_crash_cycle_end_to_end(session: AsyncSession):
    """Crash cycle: unknown → reboot → reconcile completes exactly once."""
    merchant = make_merchant_id()
    decision = await new_decision(session, merchant, "sub_crash")

    crash_gateway = FakeGateway(status="unknown")
    await execute_action(
        session,
        gateway=crash_gateway,
        merchant_id=merchant,
        entity_id="sub_crash",
        entity_type="subscription",
        decision=decision,
        action="SEND_PAYMENT_LINK",
    )
    types = await event_types_of(session, merchant, "sub_crash")
    assert "ActionCompleted" not in types

    key = make_idempotency_key(merchant, "sub_crash", decision.id)
    boot_gateway = FakeGateway(known={key: GatewayResponse("completed", provider_ref="rp_9")})
    resolved = await reconcile_pending(session, gateway=boot_gateway)

    assert resolved == 1
    types = await event_types_of(session, merchant, "sub_crash")
    assert types.count("ActionCompleted") == 1
    assert types.count("PaymentLinkSent") == 1
    assert types.count("CustomerContacted") == 1
