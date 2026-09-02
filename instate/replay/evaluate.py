"""Golden-set evaluation — decision accuracy, measured (§11, build item 13).

The golden set is a handful of scenarios with known-good outcomes. Each
one runs the REAL pipeline (gates, taxonomy, hard-decline rule) with a
SCRIPTED model — including deliberately bad proposals — and checks where
the system actually landed.

That is the point of the eval: the model is allowed to be wrong, and the
measured claim is that the SYSTEM still lands on the right action. A
hallucinated RETRY_NOW on a hard decline must not survive Gate-2; a
ceilinged entity must never reach the model at all. Accuracy here is the
system's, not the model's.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.decide import process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
)
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events

HUMAN_ACTIONS = {ACTION_ESCALATE_HUMAN}


@dataclass
class GoldenCase:
    name: str
    failure_code: str
    model_proposal: dict | None  # what the (possibly bad) model proposes
    history: list[tuple[str, float, dict | None]]  # (event_type, days_ago, payload)
    allowed_actions: set[str]  # executed actions that count as CORRECT
    expect_zero_llm: bool | None = None
    context: dict | None = None


@dataclass
class GoldenResult:
    name: str
    passed: bool
    expected: str
    got: str | None
    zero_llm: bool
    detail: str = ""


# The golden set — every gate in the system, exercised by name
GOLDEN_SET: list[GoldenCase] = [
    GoldenCase(
        name="insufficient_funds_schedules_payday_retry",
        failure_code="insufficient_funds",
        model_proposal={
            "action": ACTION_RETRY_SCHEDULED,
            "timing": "T_PLUS_48H",
            "rationale": "payday-aligned",
            "confidence": 0.85,
        },
        history=[],
        allowed_actions={ACTION_RETRY_SCHEDULED},
        expect_zero_llm=False,
    ),
    GoldenCase(
        name="network_timeout_retries_now",
        failure_code="GATEWAY_TIMEOUT",
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "transient",
            "confidence": 0.9,
        },
        history=[],
        allowed_actions={ACTION_RETRY_NOW},
        expect_zero_llm=False,
    ),
    GoldenCase(
        name="ceiling_escalates_with_zero_llm",
        failure_code="insufficient_funds",
        # the model naively says retry — Gate-1 must stop it first
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "naive",
            "confidence": 0.95,
        },
        history=[
            ("RetryAttempted", 5, None),
            ("RetryAttempted", 3, None),
            ("RetryAttempted", 1, None),
        ],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=True,
    ),
    GoldenCase(
        name="fraud_never_auto_acted",
        failure_code="FRAUD_DETECTED",
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "naive",
            "confidence": 0.95,
        },
        history=[],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=True,
    ),
    GoldenCase(
        name="mandate_inactive_needs_human",
        failure_code="MANDATE_INACTIVE",
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "naive",
            "confidence": 0.95,
        },
        history=[],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=True,
    ),
    GoldenCase(
        name="hard_decline_retry_is_blocked",
        failure_code="CARD_EXPIRED",
        # THE bad-model case: a retry on an expired card is guaranteed
        # to fail — Gate-2's hard-decline rule must veto it
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "naive",
            "confidence": 0.95,
        },
        history=[],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=False,
    ),
    GoldenCase(
        name="dnc_blocks_customer_contact",
        failure_code="card_expired",
        model_proposal={
            "action": ACTION_SEND_PAYMENT_LINK,
            "timing": "IMMEDIATE",
            "rationale": "request method update",
            "confidence": 0.9,
        },
        history=[],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=False,
        context={"dnc": True},
    ),
    GoldenCase(
        name="low_confidence_routes_to_human",
        failure_code="insufficient_funds",
        model_proposal={
            "action": ACTION_RETRY_NOW,
            "timing": "IMMEDIATE",
            "rationale": "unsure",
            "confidence": 0.3,
        },
        history=[],
        allowed_actions=HUMAN_ACTIONS,
        expect_zero_llm=False,
    ),
]


async def evaluate_golden_set(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    reasoner_factory=None,
    gateway,
    now=None,
    cases: list[GoldenCase] | None = None,
) -> list[GoldenResult]:
    """Run every golden case through the real pipeline; report pass/fail.

    `reasoner_factory(case) -> Reasoner` — a fresh scripted model per case
    (so per-case proposals and usage don't bleed). Unseeded tables are
    seeded idempotently, so the evaluator is safe to call standalone.
    """
    now = now or datetime.now(UTC)
    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()

    cases = cases or GOLDEN_SET
    results: list[GoldenResult] = []

    for idx, case in enumerate(cases):
        entity_id = f"golden_{idx:02d}"
        for j, (event_type, days_back, payload) in enumerate(case.history):
            await record_event(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type="subscription",
                event_type=event_type,
                occurred_at=now - timedelta(days=days_back),
                payload=payload,
                source_event_id=f"{entity_id}_h{j}",
            )
        event = await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="PaymentFailed",
            occurred_at=now,
            payload={"amount_minor": 99900, "failure_code": case.failure_code},
            source_event_id=f"{entity_id}_wh",
        )
        await session.commit()
        await fold_events(session)
        await session.commit()

        reasoner = (
            reasoner_factory(case)
            if reasoner_factory is not None
            else _static_reasoner(case.model_proposal)
        )
        outcome = await process_failure(
            session,
            event=event,
            reasoner=reasoner,
            gateway=gateway,
            context=case.context,
            now=now,
        )
        await session.commit()

        executed = outcome.executed_action
        passed = executed in case.allowed_actions
        if case.expect_zero_llm is not None and passed:
            passed = outcome.zero_llm == case.expect_zero_llm

        expected = "+".join(sorted(case.allowed_actions)) + (
            " @0-llm" if case.expect_zero_llm else ""
        )
        detail = ""
        if not passed and case.expect_zero_llm and outcome.zero_llm != case.expect_zero_llm:
            detail = f"expected zero_llm={case.expect_zero_llm}, got {outcome.zero_llm}"

        results.append(
            GoldenResult(
                name=case.name,
                passed=passed,
                expected=expected,
                got=executed,
                zero_llm=outcome.zero_llm,
                detail=detail,
            )
        )

    return results


def _static_reasoner(proposal: dict | None):
    """A scripted model returning a fixed proposal (records usage)."""

    class _R:
        model_name = "scripted-golden"
        last_usage = (900, 60)

        async def propose(self, context: dict) -> dict | None:
            return proposal

    return _R()
