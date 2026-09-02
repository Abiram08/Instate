"""Tests for L2 policy — versioned rules, applies_when matching, seeding.

Policy is rows, not code (§3 of architecture.md). These tests verify the
rule mechanics: seeding is idempotent, versions never bleed into each
other, and applies_when matching is strict subset semantics (a missing
context key never matches — silence is not consent).
"""


import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Policy, VERDICT_REQUIRE_HUMAN
from instate.core.policy import (
    DEFAULT_POLICY_RULES,
    active_policy_version,
    get_rules,
    metric_event_types,
    rule_applies_to,
    seed_default_policy,
)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


async def test_seed_default_policy_inserts_rules(session: AsyncSession):
    """Seeding inserts the full default rule set for the entity type."""
    inserted = await seed_default_policy(session)
    await session.commit()

    assert inserted == len(DEFAULT_POLICY_RULES)
    rules = await get_rules(session, "subscription", 1)
    assert len(rules) == len(DEFAULT_POLICY_RULES)
    rule_ids = {r.rule_id for r in rules}
    assert "retry_ceiling_7d" in rule_ids
    assert "contact_freq_24h" in rule_ids
    assert "fraud_block_require_human" in rule_ids


async def test_seed_default_policy_is_idempotent(session: AsyncSession):
    """Seeding twice inserts nothing the second time (safe on every startup)."""
    first = await seed_default_policy(session)
    await session.commit()
    second = await seed_default_policy(session)
    await session.commit()

    assert first == len(DEFAULT_POLICY_RULES)
    assert second == 0
    rules = await get_rules(session, "subscription", 1)
    assert len(rules) == len(DEFAULT_POLICY_RULES)


async def test_seed_is_scoped_per_entity_type(session: AsyncSession):
    """Rules for 'subscription' don't leak into 'invoice'."""
    await seed_default_policy(session)
    invoice_inserted = await seed_default_policy(session, entity_type="invoice")
    await session.commit()

    assert invoice_inserted == len(DEFAULT_POLICY_RULES)
    sub_rules = await get_rules(session, "subscription", 1)
    inv_rules = await get_rules(session, "invoice", 1)
    assert len(sub_rules) == len(DEFAULT_POLICY_RULES)
    assert len(inv_rules) == len(DEFAULT_POLICY_RULES)


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


async def test_active_policy_version(session: AsyncSession):
    """Active version is the max version in force."""
    await seed_default_policy(session, version=1)
    await seed_default_policy(session, version=2)
    await session.commit()

    assert await active_policy_version(session, "subscription") == 2


async def test_active_policy_version_raises_when_unseeded(session: AsyncSession):
    """Evaluating against an unseeded entity type is a hard error, not an
    implicit allow-all — no policy means no gate, and no gate is a bug."""
    with pytest.raises(LookupError):
        await active_policy_version(session, "subscription")


async def test_policy_versions_do_not_bleed(session: AsyncSession):
    """A v2 rule change never rewrites v1 — decisions pin their version."""
    await seed_default_policy(session, version=1)
    await session.commit()

    # Tighten the retry ceiling in v2 (3 → 2)
    v1_rules = await get_rules(session, "subscription", 1)
    for r in v1_rules:
        if r.rule_id == "retry_ceiling_7d":
            session.add(
                Policy(
                    version=2,
                    entity_type=r.entity_type,
                    rule_id=r.rule_id,
                    metric=r.metric,
                    limit_value=2,
                    window_seconds=r.window_seconds,
                    verdict=r.verdict,
                    applies_when=r.applies_when,
                    source="v2: tightened retry ceiling",
                )
            )
    await session.commit()

    v1 = {r.rule_id: r for r in await get_rules(session, "subscription", 1)}
    v2 = {r.rule_id: r for r in await get_rules(session, "subscription", 2)}
    assert v1["retry_ceiling_7d"].limit_value == 3  # history intact
    assert v2["retry_ceiling_7d"].limit_value == 2  # future decisions
    # v2 only contains the overridden rule + none of the untouched v1 rows
    assert set(v2) == {"retry_ceiling_7d"}


# ---------------------------------------------------------------------------
# applies_when matching — strict subset semantics
# ---------------------------------------------------------------------------


def _rule(applies_when):
    return Policy(
        version=1,
        entity_type="subscription",
        rule_id="test_rule",
        metric=None,
        limit_value=0,
        window_seconds=None,
        verdict=VERDICT_REQUIRE_HUMAN,
        applies_when=applies_when,
        source="test",
    )


def test_rule_applies_to_null_applies_when_matches_everything():
    """A rule without applies_when is a universal rule."""
    assert rule_applies_to(_rule(None), {"root_cause": "anything"}) is True
    assert rule_applies_to(_rule(None), None) is True
    assert rule_applies_to(_rule(None), {}) is True


def test_rule_applies_to_exact_match():
    rule = _rule({"root_cause": "fraud_block"})
    assert rule_applies_to(rule, {"root_cause": "fraud_block"}) is True


def test_rule_applies_to_subset_match():
    """Extra context keys are fine — applies_when is subset semantics."""
    rule = _rule({"root_cause": "card_expired"})
    context = {"root_cause": "card_expired", "issuer_country": "IN", "amount": 500}
    assert rule_applies_to(rule, context) is True


def test_rule_applies_to_missing_key_never_matches():
    """Silence is not consent: a missing context key does not match."""
    rule = _rule({"issuer_country": "IN"})
    assert rule_applies_to(rule, {"root_cause": "card_expired"}) is False
    assert rule_applies_to(rule, {}) is False
    assert rule_applies_to(rule, None) is False


def test_rule_applies_to_value_mismatch():
    rule = _rule({"root_cause": "fraud_block"})
    assert rule_applies_to(rule, {"root_cause": "card_expired"}) is False


# ---------------------------------------------------------------------------
# Metric registry
# ---------------------------------------------------------------------------


def test_metric_event_types_known_metrics():
    """Known metrics resolve to their registered event-type sets."""
    retry_types = metric_event_types("retry_count_7d")
    assert "RetryAttempted" in retry_types
    contact_types = metric_event_types("contacts_24h")
    assert "CustomerContacted" in contact_types


def test_metric_event_types_unknown_metric_is_event_type():
    """Unknown metrics degrade to raw event-type names — new counters can
    be introduced by policy row alone (data, not code)."""
    assert metric_event_types("PromiseMade") == {"PromiseMade"}


# ---------------------------------------------------------------------------
# Multi-merchant isolation at the policy layer
# ---------------------------------------------------------------------------


async def test_rules_are_merchant_agnostic_global(session: AsyncSession):
    """Policy is per entity_type+version, shared across merchants —
    but evaluation is per merchant (gate tests cover the isolation)."""
    await seed_default_policy(session)
    await session.commit()
    rules = await get_rules(session, "subscription", 1)
    assert all(r.entity_type == "subscription" for r in rules)
    # No merchant column on the table at all:
    assert not any("merchant" in c.name for c in Policy.__table__.columns)
