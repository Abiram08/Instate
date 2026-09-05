"""Golden-set evaluation of pipeline accuracy."""

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
