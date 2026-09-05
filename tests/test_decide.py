"""Agent pipeline tests with FakeReasoner/FakeGateway.
Covers zero-LLM routes, gated LLM path, outbox linkage, and drain idempotence."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.decide import drain_pending, process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.agent.execute import run_due_scheduled
from instate.adapters.razorpay import GatewayResponse
from instate.core.ledger import record_event
from instate.core.models import Decision, EntityState, Event
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from tests.conftest import make_merchant_id, now_utc


# ---------------------------------------------------------------------------
# Fakes — the agent's dependencies, satisfied without any SDK
# ---------------------------------------------------------------------------


class FakeReasoner:
    """Returns a canned proposal; records the contexts it saw."""

    model_name = "fake-reasoner"

    def __init__(self, proposal: dict | None, fail: bool = False):
        self.proposal = proposal
        self.fail = fail  # simulate SDK/timeout/refusal failure
        self.calls: list[dict] = []

    async def propose(self, context: dict) -> dict | None:
        self.calls.append(context)
        if self.fail:
            return None
        return self.proposal


class FakeGateway:
    """Records calls; returns canned responses; supports lookup()."""

    def __init__(self, status: str = "completed", fail_lookup: bool = False):
        self.status = status
        self.fail_lookup = fail_lookup
        self.calls: list[dict] = []
        self.completed_keys: set[str] = set()

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append(
            {
                "action": action,
                "entity_id": entity_id,
                "idempotency_key": idempotency_key,
                "payload": payload,
            }
        )
        if self.status == "completed":
            self.completed_keys.add(idempotency_key)
        return GatewayResponse(self.status, provider_ref=f"ref_{len(self.calls)}", detail="")

    async def lookup(self, idempotency_key: str):
        if self.fail_lookup:
            return None
        if idempotency_key in self.completed_keys:
            return GatewayResponse("completed", provider_ref="ref_lookup")
        return None


GOOD_PROPOSAL = {
    "action": "RETRY_NOW",
    "timing": "IMMEDIATE",
    "rationale": "transient timeout, retry immediately",
    "confidence": 0.9,
}


async def seed_stage3(session: AsyncSession):
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def failed_payment_event(
    session: AsyncSession,
    merchant,
    entity_id: str,
    failure_code: str = "network_timeout",
    entity_type: str = "subscription",
) -> Event:
    """A webhook-captured failure event (as the receiver would append it)."""
    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": failure_code, "amount_minor": 499900},
        source_event_id=f"wh_{entity_id}",
    )
    await session.commit()
    return event


def event_types_of(types: list[str], name: str) -> bool:
    return name in types


async def query_events(session, merchant, entity_id) -> list[str]:
    from sqlalchemy import select

    result = await session.execute(
        select(Event.event_type)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .order_by(Event.id.asc())
    )
    return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Deterministic routes
# ---------------------------------------------------------------------------


async def test_fraud_block_is_deterministic_zero_llm(session: AsyncSession):
    """fraud_block → ESCALATE_HUMAN with zero LLM."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_fraud", "FRAUD_DETECTED")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path in ("deterministic", "gate1_deny")
    assert result.zero_llm is True
    assert result.executed_action == "ESCALATE_HUMAN"
    assert reasoner.calls == []
    types = await query_events(session, merchant, "pay_fraud")
    assert "EscalatedToHuman" in types

    decision = await session.get(Decision, result.decision_id)
    assert decision.gate1  # the evidence for WHY, persisted


async def test_mandate_inactive_is_deterministic_zero_llm(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_mand", "MANDATE_INACTIVE")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path in ("deterministic", "gate1_deny")
    assert result.zero_llm is True
    assert result.executed_action == "ESCALATE_HUMAN"
    types = await query_events(session, merchant, "pay_mand")
    assert "EscalatedToHuman" in types
    state = await session.get(EntityState, (merchant, "pay_mand"))
    assert state.status == "ESCALATED"


async def test_unknown_code_escalates_safely(session: AsyncSession):
    """Unmapped code → UNKNOWN → deterministic escalate."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_odd", "SOMETHING_NOVEL")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.root_cause == "UNKNOWN"
    assert result.path == "deterministic"
    assert result.zero_llm is True


async def test_gate1_deny_stops_before_the_model(session: AsyncSession):
    """At ceiling → Gate-1 DENY without invoking the model."""
    merchant = make_merchant_id()
    await seed_stage3(session)

    # 3 retries in the last 7 days → at the ceiling
    for i in range(3):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="pay_ceiling",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=now_utc() - timedelta(days=i),
            source_event_id=f"r_{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()

    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway()
    event = await failed_payment_event(session, merchant, "pay_ceiling")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "gate1_deny"
    assert result.zero_llm is True
    assert reasoner.calls == []
    types = await query_events(session, merchant, "pay_ceiling")
    assert "EscalatedToHuman" in types


# ---------------------------------------------------------------------------
# LLM path
# ---------------------------------------------------------------------------


async def test_happy_path_llm_proposal_executes(session: AsyncSession):
    """Legal proposal → gate-2 ALLOW → gateway called, outcome linked."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway(status="completed")

    event = await failed_payment_event(session, merchant, "pay_ok", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "llm"
    assert result.llm_called is True
    assert result.gateway_called is True
    assert result.executed_action == "RETRY_NOW"
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["action"] == "RETRY_NOW"

    types = await query_events(session, merchant, "pay_ok")
    assert "FailureDiagnosed" in types
    assert "ActionIntended" in types
    assert "ActionCompleted" in types
    assert "RetryAttempted" in types

    decision = await session.get(Decision, result.decision_id)
    assert decision.executed_action == "RETRY_NOW"
    assert decision.proposal["action"] == "RETRY_NOW"
    assert decision.model == "fake-reasoner"
    assert decision.inputs_hash is not None  # reproducible, not just logged
    assert decision.gate1 and decision.gate2  # both chains on one row


async def test_invalid_llm_output_falls_back_to_policy_default(session: AsyncSession):
    """Illegal model output → deterministic policy default."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner({"action": "CHARGE_THEM_TWICE", "confidence": 0.99})
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_bad", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "policy_default"
    assert result.llm_called is False
    assert result.executed_action == "RETRY_NOW"
    assert len(gateway.calls) == 1


async def test_llm_failure_falls_back_to_policy_default(session: AsyncSession):
    """Model failure (None) → policy default, still gated."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(None, fail=True)
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_down", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "policy_default"
    assert result.llm_called is False
    assert result.executed_action == "RETRY_NOW"

    decision = await session.get(Decision, result.decision_id)
    assert decision.proposal["rationale"].startswith("policy default")


async def test_insufficient_funds_defaults_to_scheduling(session: AsyncSession):
    """insufficient_funds → RETRY_SCHEDULED without touching the gateway."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(
        {
            "action": "RETRY_SCHEDULED",
            "timing": "T_PLUS_48H",
            "rationale": "payday-aligned",
            "confidence": 0.85,
        }
    )
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_payday", "insufficient_funds")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.executed_action == "RETRY_SCHEDULED"
    assert gateway.calls == []

    types = await query_events(session, merchant, "pay_payday")
    assert "RetryScheduled" in types


async def test_gate2_dnc_stops_a_contact_action(session: AsyncSession):
    """DNC context at gate-2 → contact proposal denied → escalate."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(
        {
            "action": "SEND_PAYMENT_LINK",
            "timing": "IMMEDIATE",
            "rationale": "update the method",
            "confidence": 0.9,
        }
    )
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_dnc", "card_expired")
    result = await process_failure(
        session,
        event=event,
        reasoner=reasoner,
        gateway=gateway,
        context={"dnc": True},
    )
    await session.commit()

    assert result.path == "gate2_stop"
    assert result.llm_called is True
    assert gateway.calls == []
    types = await query_events(session, merchant, "pay_dnc")
    assert "EscalatedToHuman" in types


async def test_low_confidence_routes_to_human(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(
        {"action": "RETRY_NOW", "timing": "IMMEDIATE", "rationale": "unsure", "confidence": 0.3}
    )
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_lowconf", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "gate2_stop"
    assert result.executed_action == "ESCALATE_HUMAN"
    assert gateway.calls == []


# ---------------------------------------------------------------------------
# Drain
# ---------------------------------------------------------------------------


async def test_drain_processes_pending_and_only_once(session: AsyncSession):
    """Drain processes pending failures exactly once."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner(GOOD_PROPOSAL)
    gateway = FakeGateway()

    await failed_payment_event(session, merchant, "pay_a", "GATEWAY_TIMEOUT")
    await failed_payment_event(session, merchant, "pay_b", "GATEWAY_TIMEOUT")
    # One already-diagnosed failure (as a previous drain would have left it)
    processed = await failed_payment_event(session, merchant, "pay_done", "GATEWAY_TIMEOUT")
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="pay_done",
        entity_type="subscription",
        event_type="FailureDiagnosed",
        occurred_at=now_utc(),
        payload={"root_cause": "network_timeout", "trigger_event_id": processed.id},
        source_event_id=f"{processed.id}:diag",
    )
    await session.commit()

    results = await drain_pending(session, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert len(results) == 2
    assert {r.entity_id for r in results} == {"pay_a", "pay_b"}

    again = await drain_pending(session, reasoner=reasoner, gateway=gateway)
    await session.commit()
    assert again == []


async def test_drain_empty_ledger_is_noop(session: AsyncSession):
    await seed_stage3(session)
    results = await drain_pending(session, reasoner=FakeReasoner(None), gateway=FakeGateway())
    assert results == []


# ---------------------------------------------------------------------------
# Scheduled retries
# ---------------------------------------------------------------------------


async def test_scheduled_retry_fires_only_when_due(session: AsyncSession):
    """T+48H retry fires only when due, once."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    gateway = FakeGateway()
    reasoner = FakeReasoner(
        {
            "action": "RETRY_SCHEDULED",
            "timing": "T_PLUS_48H",
            "rationale": "payday-aligned",
            "confidence": 0.85,
        }
    )

    event = await failed_payment_event(session, merchant, "pay_sched", "insufficient_funds")
    await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()
    assert gateway.calls == []

    # 24h later: not yet due (T_PLUS_48H)
    fired = await run_due_scheduled(session, gateway=gateway, now=now_utc() + timedelta(hours=24))
    await session.commit()
    assert fired == 0

    # 49h later: due — executes as RETRY_NOW through the outbox
    fired = await run_due_scheduled(session, gateway=gateway, now=now_utc() + timedelta(hours=49))
    await session.commit()
    assert fired == 1
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["action"] == "RETRY_NOW"

    # The queue row is marked — it never double-fires
    fired_again = await run_due_scheduled(
        session, gateway=gateway, now=now_utc() + timedelta(hours=50)
    )
    await session.commit()
    assert fired_again == 0


# ---------------------------------------------------------------------------
# Failure execution
# ---------------------------------------------------------------------------


async def test_gateway_failure_writes_action_failed(session: AsyncSession):
    """Gateway failure writes ActionFailed + outcome to the ledger."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    gateway = FakeGateway(status="failed")
    reasoner = FakeReasoner(GOOD_PROPOSAL)

    event = await failed_payment_event(session, merchant, "pay_fail", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    types = await query_events(session, merchant, "pay_fail")
    assert "ActionFailed" in types
    assert "RetryAttempted" in types
    assert result.notes == ["gateway:failed"]
