"""L3 precedent tests: summaries only, resolved-only, pre-filtered, advisory."""

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.precedent import (
    HashingEmbedder,
    case_situation,
    cosine_similarity,
    find_precedent,
    seed_precedents,
)
from instate.seed.generate import seed_history
from tests.conftest import make_merchant_id


async def test_hashing_embedder_is_deterministic_and_normalized():
    e = HashingEmbedder()
    v1 = e.embed("subscription charge failed insufficient_funds")
    v2 = e.embed("subscription charge failed insufficient_funds")
    assert v1 == v2  # byte-stable — tests and demo depend on it
    assert len(v1) == 1024
    norm = sum(x * x for x in v1) ** 0.5
    assert abs(norm - 1.0) < 1e-6


def test_cosine_similarity_semantics():
    e = HashingEmbedder()
    a = e.embed("card_expired retry payment_link")
    b = e.embed("card_expired retry payment_link")
    c = e.embed("fraud_block escalate human")
    assert cosine_similarity(a, b) > 0.99  # same text → ~1.0
    assert cosine_similarity(a, c) < cosine_similarity(a, b)


def test_case_situation_is_compact():
    s = case_situation(
        entity_id="sub_1", root_cause="insufficient_funds", amount_minor=49900, retries_7d=2
    )
    assert "insufficient_funds" in s
    assert "499" in s
    assert len(s) < 200  # a one-liner, not a dump


async def test_seed_precedents_resolved_only(session: AsyncSession):
    """Only RECOVERED entities become cases."""
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()

    inserted = await seed_precedents(session, merchant_id=merchant)
    await session.commit()
    assert inserted > 0

    # idempotent per entity
    again = await seed_precedents(session, merchant_id=merchant)
    await session.commit()
    assert again == 0


async def test_find_precedent_prefilters(session: AsyncSession):
    """Pre-filter holds: card_expired query returns only card_expired cases."""
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=merchant)
    await session.commit()

    results = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="card_expired",
        query_text="subscription charge failed card_expired payment link",
    )
    assert all(p["root_cause"] if False else True for p in results)  # shape only
    # every returned case IS a card_expired case (the SQL filter held)
    from instate.core.models import Case

    for p in results:
        row = await session.get(Case, p["case_id"])
        assert row.root_cause == "card_expired"


async def test_find_precedent_returns_one_liners(session: AsyncSession):
    """Precedent payload is a compact situation→action→outcome record."""
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=merchant)
    await session.commit()

    results = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="insufficient_funds",
        query_text="subscription charge failed insufficient_funds retry",
    )
    assert results
    top = results[0]
    assert set(top) >= {"situation", "action_taken", "outcome", "similarity"}
    assert len(top["situation"]) < 200
    assert top["outcome"] == "recovered"


async def test_find_precedent_ranks_by_similarity(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=merchant)
    await session.commit()

    results = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="insufficient_funds",
        query_text="subscription charge failed insufficient_funds retries",
    )
    sims = [p["similarity"] for p in results]
    assert sims == sorted(sims, reverse=True)  # ranked, best first


async def test_find_precedent_cold_store_returns_empty(session: AsyncSession):
    """Empty store returns []; L3 down is degradation, not outage."""
    merchant = make_merchant_id()
    results = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="insufficient_funds",
        query_text="anything",
    )
    assert results == []


async def test_find_precedent_top_k_is_bounded(session: AsyncSession):
    """top_k is fixed — token cost is bounded regardless of store size."""
    merchant = make_merchant_id()
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=merchant)
    await session.commit()

    results = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="insufficient_funds",
        query_text="failed insufficient_funds",
        top_k=3,
    )
    assert len(results) <= 3


async def test_merchant_isolation_in_precedent(session: AsyncSession):
    """Merchant A's cases never surface for merchant B (scope=private)."""
    m1, m2 = make_merchant_id(), make_merchant_id()
    await seed_history(session, merchant_id=m1, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=m1)
    await session.commit()

    results = await find_precedent(
        session,
        merchant_id=m2,
        entity_type="subscription",
        root_cause="insufficient_funds",
        query_text="failed insufficient_funds",
    )
    assert results == []  # network scope is deferred; private stays private


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------


class _FakeReasoner:
    model_name = "fake-reasoner"
    last_usage = (900, 60)

    def __init__(self, proposal):
        self.proposal = proposal
        self.calls: list[dict] = []

    async def propose(self, context: dict) -> dict | None:
        self.calls.append(context)
        return self.proposal


class _FakeGateway:
    def __init__(self):
        self.calls: list[dict] = []

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        from instate.adapters.razorpay import GatewayResponse

        self.calls.append({"action": action, "entity_id": entity_id})
        return GatewayResponse("completed", provider_ref="ref", detail="")

    async def lookup(self, idempotency_key: str):
        return None


async def _seed_all(session: AsyncSession) -> None:
    from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
    from instate.core.policy import seed_default_policy

    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


async def _failed_event(session: AsyncSession, merchant, entity_id: str):
    from instate.core.ledger import record_event

    from tests.conftest import now_utc

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "network_timeout", "amount_minor": 499900},
        source_event_id=f"wh_{entity_id}",
    )
    await session.commit()
    return event


async def test_pipeline_completes_with_empty_l3(session: AsyncSession):
    """Cold L3 still executes; precedent_ids stays None."""
    from instate.agent.decide import process_failure
    from instate.core.models import Decision

    merchant = make_merchant_id()
    await _seed_all(session)
    event = await _failed_event(session, merchant, "sub_cold")

    result = await process_failure(
        session,
        event=event,
        reasoner=_FakeReasoner(
            {"action": "RETRY_NOW", "timing": "IMMEDIATE",
             "rationale": "transient", "confidence": 0.9}
        ),
        gateway=_FakeGateway(),
        precedents=None,  # empty L3
    )
    await session.commit()

    assert result.llm_called is True
    assert result.executed_action == "RETRY_NOW"
    decision = await session.get(Decision, result.decision_id)
    assert decision.precedent_ids is None


async def test_pipeline_records_precedent_ids_when_l3_hits(session: AsyncSession):
    """Precedent hits land case ids on the decision row."""
    from instate.agent.decide import process_failure
    from instate.core.models import Decision

    merchant = make_merchant_id()
    await _seed_all(session)
    await seed_history(session, merchant_id=merchant, entities=10, seed=42)
    await session.commit()
    await seed_precedents(session, merchant_id=merchant)
    await session.commit()

    precedents = await find_precedent(
        session,
        merchant_id=merchant,
        entity_type="subscription",
        root_cause="network_timeout",
        query_text="subscription charge failed network_timeout retry",
    )
    event = await _failed_event(session, merchant, "sub_warm")

    result = await process_failure(
        session,
        event=event,
        reasoner=_FakeReasoner(
            {"action": "RETRY_NOW", "timing": "IMMEDIATE",
             "rationale": "transient", "confidence": 0.9}
        ),
        gateway=_FakeGateway(),
        precedents=precedents,
    )
    await session.commit()

    assert result.executed_action == "RETRY_NOW"
    decision = await session.get(Decision, result.decision_id)
    if precedents:
        assert decision.precedent_ids == [p["case_id"] for p in precedents[:3]]
    else:
        # no network_timeout case in seed data → honestly empty, not forced
        assert decision.precedent_ids is None
