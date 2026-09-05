"""Versioned policy rules, matching, and seeding."""


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


# Seeding


async def test_seed_default_policy_inserts_rules(session: AsyncSession):
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
    first = await seed_default_policy(session)
    await session.commit()
    second = await seed_default_policy(session)
    await session.commit()

    assert first == len(DEFAULT_POLICY_RULES)
    assert second == 0
    rules = await get_rules(session, "subscription", 1)
    assert len(rules) == len(DEFAULT_POLICY_RULES)


async def test_seed_is_scoped_per_entity_type(session: AsyncSession):
    await seed_default_policy(session)
    invoice_inserted = await seed_default_policy(session, entity_type="invoice")
    await session.commit()

    assert invoice_inserted == len(DEFAULT_POLICY_RULES)
    sub_rules = await get_rules(session, "subscription", 1)
    inv_rules = await get_rules(session, "invoice", 1)
    assert len(sub_rules) == len(DEFAULT_POLICY_RULES)
    assert len(inv_rules) == len(DEFAULT_POLICY_RULES)


# Versioning


async def test_active_policy_version(session: AsyncSession):
    await seed_default_policy(session, version=1)
    await seed_default_policy(session, version=2)
    await session.commit()

    assert await active_policy_version(session, "subscription") == 2


async def test_active_policy_version_raises_when_unseeded(session: AsyncSession):
    with pytest.raises(LookupError):
        await active_policy_version(session, "subscription")


async def test_policy_versions_do_not_bleed(session: AsyncSession):
    await seed_default_policy(session, version=1)
    await session.commit()

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
    assert v1["retry_ceiling_7d"].limit_value == 3
    assert v2["retry_ceiling_7d"].limit_value == 2
    assert set(v2) == {"retry_ceiling_7d"}


# applies_when matching


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
    assert rule_applies_to(_rule(None), {"root_cause": "anything"}) is True
    assert rule_applies_to(_rule(None), None) is True
    assert rule_applies_to(_rule(None), {}) is True


def test_rule_applies_to_exact_match():
    rule = _rule({"root_cause": "fraud_block"})
    assert rule_applies_to(rule, {"root_cause": "fraud_block"}) is True


def test_rule_applies_to_subset_match():
    rule = _rule({"root_cause": "card_expired"})
    context = {"root_cause": "card_expired", "issuer_country": "IN", "amount": 500}
    assert rule_applies_to(rule, context) is True


def test_rule_applies_to_missing_key_never_matches():
    rule = _rule({"issuer_country": "IN"})
    assert rule_applies_to(rule, {"root_cause": "card_expired"}) is False
    assert rule_applies_to(rule, {}) is False
    assert rule_applies_to(rule, None) is False


def test_rule_applies_to_value_mismatch():
    rule = _rule({"root_cause": "fraud_block"})
    assert rule_applies_to(rule, {"root_cause": "card_expired"}) is False


# Metric registry


def test_metric_event_types_known_metrics():
    retry_types = metric_event_types("retry_count_7d")
    assert "RetryAttempted" in retry_types
    contact_types = metric_event_types("contacts_24h")
    assert "CustomerContacted" in contact_types


def test_metric_event_types_unknown_metric_is_event_type():
    assert metric_event_types("PromiseMade") == {"PromiseMade"}


# Multi-merchant isolation


async def test_rules_are_merchant_agnostic_global(session: AsyncSession):
    await seed_default_policy(session)
    await session.commit()
    rules = await get_rules(session, "subscription", 1)
    assert all(r.entity_type == "subscription" for r in rules)
    assert not any("merchant" in c.name for c in Policy.__table__.columns)
