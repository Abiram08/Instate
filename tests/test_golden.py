"""Tests for the golden-set evaluation (build item 13, §11).

The measured claim: the SYSTEM lands on the right action even when the
model is wrong. The golden set runs the real pipeline with scripted
models — including deliberately naive ones — and the accuracy number
comes from the gates, the taxonomy, and the hard-decline rule.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.razorpay import GatewayResponse
from instate.replay.evaluate import evaluate_golden_set
from tests.conftest import make_merchant_id, now_utc


class FakeGateway:
    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        return GatewayResponse("completed", provider_ref="r", amount_minor=49900)

    async def lookup(self, key):
        return None


def reasoner_factory(case):
    class _R:
        model_name = "scripted"
        last_usage = (900, 60)

        async def propose(self, context):
            return case.model_proposal

    return _R()


async def test_golden_set_all_pass(session: AsyncSession):
    """Every gate in the system, exercised by name — accuracy 100% with
    the naive model deliberately trying to retry dead cards and
    ceilinged entities."""
    merchant = make_merchant_id()
    results = await evaluate_golden_set(
        session,
        merchant_id=merchant,
        reasoner_factory=reasoner_factory,
        gateway=FakeGateway(),
        now=now_utc(),
    )
    await session.commit()

    failed = [r for r in results if not r.passed]
    for r in failed:
        print(f"FAILED: {r.name}: expected {r.expected}, got {r.got} — {r.detail}")

    assert results, "golden set must not be empty"
    assert not failed, f"{len(failed)} golden cases failed"
    accuracy = sum(r.passed for r in results) / len(results)
    assert accuracy == 1.0


async def test_golden_zero_llm_cases_stay_zero(session: AsyncSession):
    """The ceiling/fraud/mandate cases must resolve without the model —
    checked as part of the pass criteria."""
    merchant = make_merchant_id()
    results = await evaluate_golden_set(
        session,
        merchant_id=merchant,
        reasoner_factory=reasoner_factory,
        gateway=FakeGateway(),
        now=now_utc(),
    )
    await session.commit()

    zero_cases = [r for r in results if r.zero_llm]
    assert {
        "ceiling_escalates_with_zero_llm",
        "fraud_never_auto_acted",
        "mandate_inactive_needs_human",
    }.issubset({r.name for r in zero_cases})


async def test_golden_hard_decline_case_vetoed_the_retry(session: AsyncSession):
    """The flagship: the model said RETRY_NOW on an expired card, the
    system escalated instead — the veto is visible in the result."""
    merchant = make_merchant_id()
    results = await evaluate_golden_set(
        session,
        merchant_id=merchant,
        reasoner_factory=reasoner_factory,
        gateway=FakeGateway(),
        now=now_utc(),
    )
    await session.commit()

    case = next(r for r in results if r.name == "hard_decline_retry_is_blocked")
    assert case.passed
    assert case.got == "ESCALATE_HUMAN"
    assert case.zero_llm is False  # the model was consulted, then overridden
