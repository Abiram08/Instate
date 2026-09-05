"""Paused status and backup/method-check actions."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.agent.execute import execute_action
from instate.core.gate import LEGAL_ACTIONS_BY_STATUS, check_proposal, evaluate
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_CHECK_METHOD_UPDATED,
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_BACKUP_METHOD,
    ACTION_RETRY_NOW,
    LEGAL_ACTIONS,
    STATUS_PAUSED,
    Decision,
    Event,
)
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from tests.conftest import make_merchant_id, now_utc


class FakeGateway:
    def __init__(self, status="completed", data=None):
        self.status = status
        self.data = data or {}
        self.calls = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse(self.status, provider_ref="ref", data=dict(self.data))

    async def lookup(self, idempotency_key: str):
        return None


async def seed_policy(session: AsyncSession):
    await seed_default_policy(session)
    await session.commit()


async def new_decision(session: AsyncSession, merchant, entity_id: str) -> Decision:
    decision = Decision(merchant_id=merchant, entity_id=entity_id, root_cause="x")
    session.add(decision)
    await session.flush()
    return decision


async def pause_entity(session, merchant, entity_id: str):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="SubscriptionPaused",
        occurred_at=now_utc(),
        source_event_id=f"{entity_id}_pause",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()


# PAUSED


async def test_paused_entity_reaches_paused_status(session: AsyncSession):
    from instate.core.models import EntityState

    merchant = make_merchant_id()
    await pause_entity(session, merchant, "sub_pause")
    state = await session.get(EntityState, (merchant, "sub_pause"))
    assert state.status == STATUS_PAUSED


async def test_paused_money_attempt_denied_at_gate1(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    await pause_entity(session, merchant, "sub_parked")

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_parked",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    await session.commit()

    assert result.verdict == "DENY"
    assert any(e["rule_id"] == "paused_no_autoretry" for e in result.reason_chain)


async def test_paused_contact_action_still_allowed(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    await pause_entity(session, merchant, "sub_parked2")

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_parked2",
        entity_type="subscription",
        action_class="SEND_PAYMENT_LINK",
        root_cause="card_expired",
    )
    await session.commit()

    assert result.verdict == "ALLOW"


async def test_resume_reopens_money_path(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    await pause_entity(session, merchant, "sub_resume")
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_resume",
        entity_type="subscription",
        event_type="SubscriptionResumed",
        occurred_at=now_utc(),
        source_event_id="sub_resume_back",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_resume",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
    )
    await session.commit()

    assert result.verdict == "ALLOW"
    assert not any(e["rule_id"] == "paused_no_autoretry" for e in result.reason_chain)


async def test_paused_legality_map(session: AsyncSession):
    allowed = LEGAL_ACTIONS_BY_STATUS["PAUSED"]
    assert ACTION_ESCALATE_HUMAN in allowed
    assert ACTION_RETRY_NOW not in allowed
    assert ACTION_RETRY_BACKUP_METHOD not in allowed


# New actions


async def test_new_actions_are_legal_everywhere_old_ones_are(session: AsyncSession):
    assert ACTION_RETRY_BACKUP_METHOD in LEGAL_ACTIONS
    assert ACTION_CHECK_METHOD_UPDATED in LEGAL_ACTIONS
    assert ACTION_RETRY_BACKUP_METHOD in LEGAL_ACTIONS_BY_STATUS["DIAGNOSED"]
    assert ACTION_CHECK_METHOD_UPDATED in LEGAL_ACTIONS_BY_STATUS["DIAGNOSED"]


async def test_backup_retry_bypasses_hard_decline_but_counts_to_budget(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_bak",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "CARD_EXPIRED"},
        source_event_id="sub_bak_wh",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_bak",
        entity_type="subscription",
        action_class="RETRY_BACKUP_METHOD",
        root_cause="card_expired",
    )
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_bak",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": "RETRY_BACKUP_METHOD", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()

    assert g2.verdict == "ALLOW"
    assert not any(e["rule_id"] == "hard_decline_requires_new_method" for e in g2.reason_chain)

    g1b = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_bak",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="card_expired",
    )
    await session.commit()
    g2b = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_bak",
        entity_type="subscription",
        decision_id=g1b.decision_id,
        proposal={"action": "RETRY_NOW", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()
    assert g2b.verdict == "DENY"


async def test_method_check_writes_payment_changed_on_update(session: AsyncSession):
    """Pins read-only check: no RetryAttempted burned."""
    merchant = make_merchant_id()
    await seed_policy(session)
    decision = await new_decision(session, merchant, "sub_probe")
    await session.commit()

    gw = FakeGateway(data={"method_updated": True})
    await execute_action(
        session,
        gateway=gw,
        merchant_id=merchant,
        entity_id="sub_probe",
        entity_type="subscription",
        decision=decision,
        action="CHECK_METHOD_UPDATED",
        now=now_utc(),
    )
    await session.commit()

    result = await session.execute(
        select(Event.event_type).where(
            Event.merchant_id == merchant, Event.entity_id == "sub_probe"
        )
    )
    types = [r[0] for r in result.all()]
    assert "MethodCheckCompleted" in types
    assert "PaymentMethodChanged" in types
    assert "RetryAttempted" not in types


async def test_method_check_negative_writes_no_unblock(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)
    decision = await new_decision(session, merchant, "sub_probe2")
    await session.commit()

    gw = FakeGateway(data={"method_updated": False})
    await execute_action(
        session,
        gateway=gw,
        merchant_id=merchant,
        entity_id="sub_probe2",
        entity_type="subscription",
        decision=decision,
        action="CHECK_METHOD_UPDATED",
        now=now_utc(),
    )
    await session.commit()

    result = await session.execute(
        select(Event.event_type).where(
            Event.merchant_id == merchant, Event.entity_id == "sub_probe2"
        )
    )
    types = [r[0] for r in result.all()]
    assert "MethodCheckCompleted" in types
    assert "PaymentMethodChanged" not in types
