"""Tests for the Stage-4 domain hardening (build item 11, §6 Stripe lessons).

The hard-decline method gate is the centerpiece: a money attempt on a
hard-declined method is denied until a PaymentMethodChanged event lands —
at Gate-2 for immediate proposals, and at the due-scan for scheduled
retries. Same rule, both doors.
"""

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.agent.decide import process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.agent.execute import run_due_scheduled, schedule_retry
from instate.core.gate import check_proposal, evaluate
from instate.core.ledger import record_event
from instate.core.models import Decision, Event
from instate.core.policy import seed_default_policy
from instate.core.projection import has_new_method_since_last_failure
from tests.conftest import days_ago, make_merchant_id, now_utc


class FakeGateway:
    def __init__(self, status="completed"):
        self.status = status
        self.calls = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse(self.status, provider_ref="r", amount_minor=49900)

    async def lookup(self, key):
        return None


async def seed_all(session):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def events_of(session, merchant, entity_id):
    result = await session.execute(
        select(Event.event_type)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .order_by(Event.id.asc())
    )
    return [r[0] for r in result.all()]


# ---------------------------------------------------------------------------
# has_new_method_since_last_failure
# ---------------------------------------------------------------------------


async def test_no_method_evidence_blocks_by_default(session: AsyncSession):
    """The hard-decline unblock requires EVIDENCE: with no
    PaymentMethodChanged on file at all, the block stands — even for an
    entity with no prior failure row."""
    merchant = make_merchant_id()
    assert (
        await has_new_method_since_last_failure(session, merchant_id=merchant, entity_id="e_none")
        is False
    )


async def test_method_change_without_failure_counts_as_fresh(session: AsyncSession):
    """A method change with no failure on record = a fresh method on file."""
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_fresh",
        entity_type="subscription",
        event_type="PaymentMethodChanged",
        occurred_at=days_ago(1),
        source_event_id="m1",
    )
    await session.commit()

    assert (
        await has_new_method_since_last_failure(session, merchant_id=merchant, entity_id="e_fresh")
        is True
    )


async def test_method_change_after_failure_unblocks(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_meth",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=days_ago(2),
        source_event_id="f1",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_meth",
        entity_type="subscription",
        event_type="PaymentMethodChanged",
        occurred_at=days_ago(1),
        source_event_id="m1",
    )
    await session.commit()

    assert (
        await has_new_method_since_last_failure(session, merchant_id=merchant, entity_id="e_meth")
        is True
    )


async def test_no_method_change_since_failure_blocks(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_block",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=days_ago(2),
        source_event_id="f1",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_block",
        entity_type="subscription",
        event_type="PaymentMethodChanged",
        occurred_at=days_ago(3),  # BEFORE the failure
        source_event_id="m1",
    )
    await session.commit()

    assert (
        await has_new_method_since_last_failure(session, merchant_id=merchant, entity_id="e_block")
        is False
    )


# ---------------------------------------------------------------------------
# Gate-2 — the hard-decline rule
# ---------------------------------------------------------------------------


async def test_gate2_denies_retry_on_hard_decline(session: AsyncSession):
    """card_expired + model says RETRY_NOW → the hard-decline rule vetoes
    it with a reason-chain entry naming the mechanism."""
    merchant = make_merchant_id()
    await seed_all(session)

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="e_hd",
        entity_type="subscription",
        action_class="REQUEST_PAYMENT_METHOD",
        root_cause="card_expired",
    )
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="e_hd",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": "RETRY_NOW", "confidence": 0.95},
        root_cause="card_expired",
    )
    await session.commit()

    assert g2.verdict == "DENY"
    entry = next(e for e in g2.reason_chain if e["rule_id"] == "hard_decline_requires_new_method")
    assert "hard" in entry["detail"]


async def test_gate2_allows_contact_on_hard_decline(session: AsyncSession):
    """The rule gates MONEY attempts — a payment link (method update) is
    exactly what a hard decline needs, and passes."""
    merchant = make_merchant_id()
    await seed_all(session)

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="e_link",
        entity_type="subscription",
        action_class="REQUEST_PAYMENT_METHOD",
        root_cause="card_expired",
    )
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="e_link",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": "SEND_PAYMENT_LINK", "confidence": 0.9},
        root_cause="card_expired",
    )
    await session.commit()

    assert g2.verdict == "ALLOW"


async def test_gate2_hard_decline_not_triggered_for_other_causes(session: AsyncSession):
    """Precision: insufficient_funds retries are not collateral damage."""
    merchant = make_merchant_id()
    await seed_all(session)

    g1 = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="e_if",
        entity_type="subscription",
        action_class="RETRY_SCHEDULED",
        root_cause="insufficient_funds",
    )
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="e_if",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": "RETRY_NOW", "confidence": 0.9},
        root_cause="insufficient_funds",
    )
    await session.commit()

    assert g2.verdict == "ALLOW"
    assert not any(e["rule_id"] == "hard_decline_requires_new_method" for e in g2.reason_chain)


# ---------------------------------------------------------------------------
# The pipeline — a bad model cannot retry a dead card
# ---------------------------------------------------------------------------


async def test_pipeline_blocks_bad_model_retry_on_card_expired(session: AsyncSession):
    """The model naively proposes RETRY_NOW on card_expired → Gate-2 vetoes
    → escalate. The hallucination cannot reach Razorpay."""
    merchant = make_merchant_id()
    await seed_all(session)

    class NaiveModel:
        model_name = "naive"
        last_usage = (900, 60)

        async def propose(self, context):
            return {
                "action": "RETRY_NOW",
                "timing": "IMMEDIATE",
                "rationale": "just retry",
                "confidence": 0.95,
            }

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="pay_dead",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "CARD_EXPIRED", "amount_minor": 99900},
        source_event_id="wh_dead",
    )
    await session.commit()

    gateway = FakeGateway()
    result = await process_failure(session, event=event, reasoner=NaiveModel(), gateway=gateway)
    await session.commit()

    assert result.executed_action == "ESCALATE_HUMAN"
    assert gateway.calls == []  # nothing touched money
    types = await events_of(session, merchant, "pay_dead")
    assert "EscalatedToHuman" in types


async def test_prompt_text_persisted_for_reproducibility(session: AsyncSession):
    """§5/§11: the decision stores the exact rendered context —
    'reproducible' becomes literal, not just an inputs_hash."""
    merchant = make_merchant_id()
    await seed_all(session)

    class DecentModel:
        model_name = "decent"
        last_usage = (900, 60)

        async def propose(self, context):
            return {
                "action": "RETRY_NOW",
                "timing": "IMMEDIATE",
                "rationale": "transient",
                "confidence": 0.9,
            }

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="pay_pt",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "GATEWAY_TIMEOUT"},
        source_event_id="wh_pt",
    )
    await session.commit()

    await process_failure(session, event=event, reasoner=DecentModel(), gateway=FakeGateway())
    await session.commit()

    result = await session.execute(select(Decision).where(Decision.entity_id == "pay_pt"))
    decision = result.scalar_one()
    assert decision.prompt_text is not None
    assert "root_cause" in decision.prompt_text  # the digest, rendered
    assert decision.tokens_in > 0  # input cost = the rendered digest


# ---------------------------------------------------------------------------
# The due-scan — scheduled retries wait for the method
# ---------------------------------------------------------------------------


async def test_scheduled_hard_decline_retry_defers_then_unblocks(session: AsyncSession):
    """Stripe's model, mechanically: the scheduled retry stays queued
    (executes nothing) until PaymentMethodChanged lands — then the next
    tick executes it through the same outbox."""
    merchant = make_merchant_id()
    await seed_all(session)

    decision = Decision(merchant_id=merchant, entity_id="e_sched", root_cause="card_expired")
    session.add(decision)
    await session.flush()

    await schedule_retry(
        session,
        merchant_id=merchant,
        entity_id="e_sched",
        entity_type="subscription",
        decision=decision,
        timing="T_PLUS_1H",
        root_cause="card_expired",
    )
    await session.commit()

    gateway = FakeGateway()

    # T+2h: due, but no new method → deferred, gateway never called
    fired = await run_due_scheduled(session, gateway=gateway, now=now_utc() + timedelta(hours=2))
    await session.commit()
    assert fired == 0
    assert gateway.calls == []

    # The customer updates their method
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="e_sched",
        entity_type="subscription",
        event_type="PaymentMethodChanged",
        occurred_at=now_utc() + timedelta(hours=3),
        source_event_id="mc_1",
    )
    await session.commit()

    # T+4h: unblocked → executes
    fired = await run_due_scheduled(session, gateway=gateway, now=now_utc() + timedelta(hours=4))
    await session.commit()
    assert fired == 1
    assert len(gateway.calls) == 1

    # And never re-fires
    again = await run_due_scheduled(session, gateway=gateway, now=now_utc() + timedelta(hours=5))
    assert again == 0
