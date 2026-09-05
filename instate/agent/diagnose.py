"""Failure code → root cause as versioned data.
Map and taxonomy live in tables, updatable without a deploy.
Unmapped codes diagnose as UNKNOWN, routed deterministically to ESCALATE_HUMAN.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    DiagnosisRule,
    ROOT_CAUSE_UNKNOWN,
    TaxonomyRule,
)


# ---------------------------------------------------------------------------
# Seed data — decline-code map; verify strings against Razorpay docs
# ---------------------------------------------------------------------------

DEFAULT_DIAGNOSIS_RULES: list[dict] = [
    {"failure_code": "insufficient_funds", "root_cause": "insufficient_funds"},
    {"failure_code": "INSUFFICIENT_FUNDS", "root_cause": "insufficient_funds"},
    {"failure_code": "SG_00039", "root_cause": "insufficient_funds"},
    {"failure_code": "card_expired", "root_cause": "card_expired"},
    {"failure_code": "CARD_EXPIRED", "root_cause": "card_expired"},
    {"failure_code": "expired_card", "root_cause": "card_expired"},
    {"failure_code": "TOKEN_STALE", "root_cause": "card_expired"},
    {"failure_code": "mandate_inactive", "root_cause": "mandate_inactive"},
    {"failure_code": "MANDATE_INACTIVE", "root_cause": "mandate_inactive"},
    {"failure_code": "MANDATE_ACTIVE_STATUS_INVALID", "root_cause": "mandate_inactive"},
    {"failure_code": "network_timeout", "root_cause": "network_timeout"},
    {"failure_code": "GATEWAY_TIMEOUT", "root_cause": "network_timeout"},
    {"failure_code": "NETWORK_ERROR", "root_cause": "network_timeout"},
    {"failure_code": "GATEWAY_ERROR", "root_cause": "network_timeout"},
    {"failure_code": "fraud_suspected", "root_cause": "fraud_block"},
    {"failure_code": "FRAUD_DETECTED", "root_cause": "fraud_block"},
    {"failure_code": "payment_blocked_by_fraud", "root_cause": "fraud_block"},
    {"failure_code": "customer_cancelled", "root_cause": "customer_initiated"},
    {"failure_code": "CANCELLED_BY_USER", "root_cause": "customer_initiated"},
    {"failure_code": "customer_stopped_payment", "root_cause": "customer_initiated"},
    {"failure_code": "WRONG_UPI_PIN", "root_cause": "customer_error"},
    {"failure_code": "wrong_upi_pin", "root_cause": "customer_error"},
    {"failure_code": "INCORRECT_CVV", "root_cause": "customer_error"},
    {"failure_code": "incorrect_cvv", "root_cause": "customer_error"},
    {"failure_code": "INVALID_CARD_NUMBER", "root_cause": "customer_error"},
    {"failure_code": "invalid_card_number", "root_cause": "customer_error"},
]

DEFAULT_DIAGNOSIS_SOURCE = "Razorpay error codes (test-mode observed set) — verify current strings"


DEFAULT_TAXONOMY_RULES: list[dict] = [
    {
        "root_cause": "insufficient_funds",
        "default_action": ACTION_RETRY_SCHEDULED,  # retrying now fails by definition
        "deterministic": False,
        "source": "§6 taxonomy: timing is the judgment call",
    },
    {
        "root_cause": "network_timeout",
        "default_action": ACTION_RETRY_NOW,  # transient failure
        "deterministic": False,
        "source": "§6 taxonomy: transient failure",
    },
    {
        "root_cause": "card_expired",
        "default_action": ACTION_REQUEST_PAYMENT_METHOD,  # never retry without a new method
        "deterministic": False,
        "source": "§6 taxonomy + Stripe lesson: hard declines are payment-method situations",
    },
    {
        "root_cause": "mandate_inactive",
        "default_action": ACTION_ESCALATE_HUMAN,  # deterministic route
        "deterministic": True,
        "source": "§6 taxonomy: needs re-authorisation, not collection",
    },
    {
        "root_cause": "fraud_block",
        "default_action": ACTION_ESCALATE_HUMAN,  # deterministic route
        "deterministic": True,
        "source": "§6 taxonomy: never auto-retry",
    },
    {
        "root_cause": "customer_initiated",
        "default_action": ACTION_SEND_PAYMENT_LINK,  # contact, don't charge
        "deterministic": False,
        "source": "§6 taxonomy: customer owns the timing",
    },
    {
        "root_cause": "customer_error",
        "default_action": ACTION_SEND_PAYMENT_LINK,  # fix-it link; input errors are correctable
        "deterministic": False,
        "source": "§6 taxonomy: >50% of Indian failures are customer-error/network — recoverable fast",
    },
    {
        "root_cause": ROOT_CAUSE_UNKNOWN,
        "default_action": ACTION_ESCALATE_HUMAN,  # never-empty default
        "deterministic": True,
        "source": "§6 taxonomy: every input must have a branch",
    },
]


# ---------------------------------------------------------------------------
# Seeding — idempotent
# ---------------------------------------------------------------------------


async def seed_default_diagnosis(
    session: AsyncSession,
    *,
    version: int = 1,
) -> int:
    inserted = 0
    for rule in DEFAULT_DIAGNOSIS_RULES:
        exists = await session.get(DiagnosisRule, (version, rule["failure_code"]))
        if exists is not None:
            continue
        session.add(
            DiagnosisRule(
                version=version,
                failure_code=rule["failure_code"],
                root_cause=rule["root_cause"],
                source=DEFAULT_DIAGNOSIS_SOURCE,
            )
        )
        inserted += 1
    await session.flush()
    return inserted


async def seed_default_taxonomy(
    session: AsyncSession,
    *,
    version: int = 1,
) -> int:
    inserted = 0
    for rule in DEFAULT_TAXONOMY_RULES:
        exists = await session.get(TaxonomyRule, (version, rule["root_cause"]))
        if exists is not None:
            continue
        session.add(
            TaxonomyRule(
                version=version,
                root_cause=rule["root_cause"],
                default_action=rule["default_action"],
                deterministic=rule["deterministic"],
                source=rule["source"],
            )
        )
        inserted += 1
    await session.flush()
    return inserted


# ---------------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------------


async def diagnose(
    session: AsyncSession,
    *,
    failure_code: str | None,
    version: int = 1,
) -> str:
    """Resolve a failure code to a root cause. Unmapped codes return UNKNOWN."""
    if not failure_code:
        return ROOT_CAUSE_UNKNOWN

    result = await session.execute(
        select(DiagnosisRule).where(
            DiagnosisRule.version == version,
            DiagnosisRule.failure_code == failure_code,
        )
    )
    rule = result.scalar_one_or_none()
    return rule.root_cause if rule is not None else ROOT_CAUSE_UNKNOWN


# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------


async def taxonomy_for(
    session: AsyncSession,
    root_cause: str,
    version: int = 1,
) -> TaxonomyRule:
    """Return the taxonomy row; fall back to UNKNOWN (deterministic escalate)."""
    result = await session.execute(
        select(TaxonomyRule).where(
            TaxonomyRule.version == version,
            TaxonomyRule.root_cause == root_cause,
        )
    )
    rule = result.scalar_one_or_none()
    if rule is not None:
        return rule
    result = await session.execute(
        select(TaxonomyRule).where(
            TaxonomyRule.version == version,
            TaxonomyRule.root_cause == ROOT_CAUSE_UNKNOWN,
        )
    )
    fallback = result.scalar_one_or_none()
    if fallback is None:
        raise LookupError(
            f"no taxonomy row for {root_cause!r} and no UNKNOWN fallback — "
            f"seed with seed_default_taxonomy() first"
        )
    return fallback
