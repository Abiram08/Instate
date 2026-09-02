"""Tests for the diagnosis map and action taxonomy (build item 5).

The map and taxonomy are versioned DATA, not code — so the tests cover
seeding idempotence, version isolation, and the never-empty guarantee:
an unmapped code diagnoses as UNKNOWN, whose taxonomy row is a
deterministic route to ESCALATE_HUMAN.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.diagnose import (
    diagnose,
    seed_default_diagnosis,
    seed_default_taxonomy,
    taxonomy_for,
)
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    DiagnosisRule,
    ROOT_CAUSE_UNKNOWN,
)


async def test_seed_diagnosis_is_idempotent(session: AsyncSession):
    first = await seed_default_diagnosis(session)
    second = await seed_default_diagnosis(session)
    await session.commit()

    assert first > 0
    assert second == 0


async def test_seed_taxonomy_is_idempotent(session: AsyncSession):
    first = await seed_default_taxonomy(session)
    second = await seed_default_taxonomy(session)
    await session.commit()

    assert first > 0
    assert second == 0


# ---------------------------------------------------------------------------
# diagnose — the deterministic map
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("insufficient_funds", "insufficient_funds"),
        ("INSUFFICIENT_FUNDS", "insufficient_funds"),
        ("CARD_EXPIRED", "card_expired"),
        ("MANDATE_INACTIVE", "mandate_inactive"),
        ("GATEWAY_TIMEOUT", "network_timeout"),
        ("FRAUD_DETECTED", "fraud_block"),
        ("CANCELLED_BY_USER", "customer_initiated"),
    ],
)
async def test_diagnose_known_codes(session: AsyncSession, code, expected):
    """Every seeded code resolves to its root cause — the map, not an LLM."""
    await seed_default_diagnosis(session)
    await session.commit()

    assert await diagnose(session, failure_code=code) == expected


async def test_diagnose_unknown_code_is_explicit(session: AsyncSession):
    """An unmapped code is UNKNOWN — explicit, never silent."""
    await seed_default_diagnosis(session)
    await session.commit()

    assert await diagnose(session, failure_code="SOMETHING_NOVEL") == ROOT_CAUSE_UNKNOWN


async def test_diagnose_missing_code_is_unknown(session: AsyncSession):
    """A webhook with no code at all still has a branch — the never-empty
    guarantee covers absence, not just novelty."""
    await seed_default_diagnosis(session)
    await session.commit()

    assert await diagnose(session, failure_code=None) == ROOT_CAUSE_UNKNOWN


async def test_diagnosis_versions_do_not_bleed(session: AsyncSession):
    """v2 can remap a code; v1 answers stay exactly what they were."""
    await seed_default_diagnosis(session, version=1)
    session.add(
        DiagnosisRule(
            version=2,
            failure_code="insufficient_funds",
            root_cause="card_expired",  # a (silly) v2 reclassification
            source="v2 test remap",
        )
    )
    await session.commit()

    v1 = await diagnose(session, failure_code="insufficient_funds", version=1)
    v2 = await diagnose(session, failure_code="insufficient_funds", version=2)
    assert v1 == "insufficient_funds"
    assert v2 == "card_expired"


# ---------------------------------------------------------------------------
# taxonomy — root cause → action
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("root_cause", "expected_action", "deterministic"),
    [
        ("insufficient_funds", ACTION_RETRY_SCHEDULED, False),
        ("network_timeout", ACTION_RETRY_NOW, False),
        ("card_expired", ACTION_REQUEST_PAYMENT_METHOD, False),
        ("mandate_inactive", ACTION_ESCALATE_HUMAN, True),
        ("fraud_block", ACTION_ESCALATE_HUMAN, True),
        (ROOT_CAUSE_UNKNOWN, ACTION_ESCALATE_HUMAN, True),
    ],
)
async def test_taxonomy_rows(session: AsyncSession, root_cause, expected_action, deterministic):
    """The §6 action taxonomy, keyed to decline reason — deterministic
    routes (fraud, mandate, UNKNOWN) skip the model entirely."""
    await seed_default_taxonomy(session)
    await session.commit()

    rule = await taxonomy_for(session, root_cause)
    assert rule.default_action == expected_action
    assert rule.deterministic == deterministic


async def test_taxonomy_unknown_root_cause_hits_fallback(session: AsyncSession):
    """Even a root cause with no taxonomy row resolves — to the UNKNOWN
    fallback (deterministic escalate). The pipeline cannot dead-end."""
    await seed_default_taxonomy(session)
    await session.commit()

    rule = await taxonomy_for(session, "brand_new_cause")
    assert rule.default_action == ACTION_ESCALATE_HUMAN
    assert rule.deterministic is True


async def test_customer_initiated_routes_to_link(session: AsyncSession):
    """customer_initiated → contact, don't charge."""
    await seed_default_taxonomy(session)
    await session.commit()

    rule = await taxonomy_for(session, "customer_initiated")
    assert rule.default_action == ACTION_SEND_PAYMENT_LINK
    assert rule.deterministic is False


async def test_taxonomy_unseeded_raises(session: AsyncSession):
    """No taxonomy and no UNKNOWN fallback is a hard error — an unseeded
    system must never behave as an implicit allow-all."""
    with pytest.raises(LookupError):
        await taxonomy_for(session, "anything")
