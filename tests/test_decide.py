"""Tests for the agent pipeline — the thin workflow, end to end.

Every path is testable WITHOUT a model (that's the point of a workflow,
not an autonomous loop): the tests inject a FakeReasoner and a
FakeGateway. What they prove:

- the majority-of-events-never-reach-the-model claim: gate-1 deny,
  fixed-action routes, and UNKNOWN all resolve at zero tokens
- the one LLM call's output is validated and gated before execution
- LLM failure → the deterministic policy default (no drama)
- the outbox: intent before gateway, outcome after, everything linked
- the drain: webhooks appended but never processed get processed once
"""

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
# Deterministic routes — zero tokens
# ---------------------------------------------------------------------------


async def test_fraud_block_is_deterministic_zero_llm(session: AsyncSession):
    """fraud_block → fixed ESCALATE_HUMAN route, zero tokens. In practice
    BOTH zero-token mechanisms fire: the fraud context rule at Gate-1
    (REQUIRE_HUMAN, with the policy citation in the reason chain) and the
    deterministic taxonomy route. Either way: no model, human owns it."""
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
    """An unmapped failure code still has a branch: UNKNOWN → deterministic
    escalate. The pipeline cannot dead-end on novelty."""
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
    """An entity at its retry ceiling → Gate-1 DENY → escalate, and the
    reasoner is never invoked (the zero-token headline path)."""
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
# The one LLM call — validated, gated, then executed
# ---------------------------------------------------------------------------


async def test_happy_path_llm_proposal_executes(session: AsyncSession):
    """network_timeout + legal proposal → gate-2 allows → gateway called
    → outcome events written, linked to the decision."""
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
    """The model proposes an illegal action → validate_proposal returns
    None → the deterministic policy default takes over — no retries,
    no drama (§7 failure path)."""
    merchant = make_merchant_id()
    await seed_stage3(session)
    reasoner = FakeReasoner({"action": "CHARGE_THEM_TWICE", "confidence": 0.99})
    gateway = FakeGateway()

    event = await failed_payment_event(session, merchant, "pay_bad", "GATEWAY_TIMEOUT")
    result = await process_failure(session, event=event, reasoner=reasoner, gateway=gateway)
    await session.commit()

    assert result.path == "policy_default"
    assert result.llm_called is False  # an invalid output is no output
    # network_timeout's policy default is RETRY_NOW — executed directly
    assert result.executed_action == "RETRY_NOW"
    assert len(gateway.calls) == 1


async def test_llm_failure_falls_back_to_policy_default(session: AsyncSession):
    """The SDK throws / refuses → None → policy default. network_timeout
    → RETRY_NOW via the default, still gated, still audited."""
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
    """insufficient_funds → RETRY_SCHEDULED: a scheduled retry never hits
    the gateway at decision time; it lands in scheduled_actions."""
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
    assert gateway.calls == []  # nothing touches money now

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

    # card_expired's default is REQUEST_PAYMENT_METHOD (non-deterministic),
    # so the model runs — and gate-2 denies the contact under DNC.
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
# The drain — ledger-first webhooks, processed exactly once
# ---------------------------------------------------------------------------


async def test_drain_processes_pending_and_only_once(session: AsyncSession):
    """Two fresh failures + one already-diagnosed failure → the drain
    processes exactly the two pending ones. Idempotent by construction."""
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

    # A second drain is a no-op — everything is diagnosed now
    again = await drain_pending(session, reasoner=reasoner, gateway=gateway)
    await session.commit()
    assert again == []


async def test_drain_empty_ledger_is_noop(session: AsyncSession):
    await seed_stage3(session)
    results = await drain_pending(session, reasoner=FakeReasoner(None), gateway=FakeGateway())
    assert results == []


# ---------------------------------------------------------------------------
# Scheduled retries — the durable queue fires when due
# ---------------------------------------------------------------------------


async def test_scheduled_retry_fires_only_when_due(session: AsyncSession):
    """A T_PLUS_48H scheduled retry: not executed before due, executed as
    RETRY_NOW after, through the same outbox."""
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
# Failure execution — a decline is a fact, written to the ledger
# ---------------------------------------------------------------------------


async def test_gateway_failure_writes_action_failed(session: AsyncSession):
    """A gateway 'failed' response is a real outcome: ActionFailed + the
    outcome event land in the ledger (the stateless baseline would have
    just retried again)."""
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
