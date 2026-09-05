"""L2 policy: versioned counter and context rules (§3).

Counter rules fire when an indexed L0 count >= limit; context rules fire
on applies_when match. Decisions record the policy_version in force.
"""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    Policy,
    VERDICT_DENY,
    VERDICT_REQUIRE_HUMAN,
)
from instate.core.projection import RETRY_EVENT_TYPES, CONTACT_EVENT_TYPES


# ---------------------------------------------------------------------------
# Metric registry — metric name → the L0 event types it counts
# ---------------------------------------------------------------------------

# The window itself lives on the policy row (window_seconds), so the same
# metric can be capped over different windows by different rules.
METRIC_EVENT_TYPES: dict[str, set[str]] = {
    "retry_count_7d": RETRY_EVENT_TYPES,
    "retry_count_72h": RETRY_EVENT_TYPES,
    "retry_count_24h": RETRY_EVENT_TYPES,
    "contacts_24h": CONTACT_EVENT_TYPES,
    "contacts_1h": CONTACT_EVENT_TYPES,
}


# ---------------------------------------------------------------------------
# Default rules — the L2 seed, keyed on real decline reasons (§6 taxonomy)
# ---------------------------------------------------------------------------

DEFAULT_POLICY_RULES: list[dict] = [
    {
        "rule_id": "retry_ceiling_7d",
        "metric": "retry_count_7d",
        "limit_value": 3,
        "window_seconds": 7 * 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": None,
        "source": "Internal collections policy: retry budget — max 3 attempts per 7 days",
    },
    {
        "rule_id": "retry_spacing_24h",
        "metric": "retry_count_24h",
        "limit_value": 1,
        "window_seconds": 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": None,
        "source": "Internal collections policy: max 1 retry attempt per 24h per entity",
    },
    {
        "rule_id": "contact_freq_24h",
        "metric": "contacts_24h",
        "limit_value": 2,
        "window_seconds": 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": None,
        "source": "TRAI DND / customer preference: max 2 customer contacts per 24h",
    },
    {
        "rule_id": "mandate_inactive_require_human",
        "metric": None,
        "limit_value": 0,
        "window_seconds": None,
        "verdict": VERDICT_REQUIRE_HUMAN,
        "applies_when": {"root_cause": "mandate_inactive"},
        "source": "RBI e-mandate framework: re-authorisation is human-assisted, not collected",
    },
    {
        "rule_id": "fraud_block_require_human",
        "metric": None,
        "limit_value": 0,
        "window_seconds": None,
        "verdict": VERDICT_REQUIRE_HUMAN,
        "applies_when": {"root_cause": "fraud_block"},
        "source": "Fraud policy: never auto-act on fraud-flagged payments",
    },
    {
        "rule_id": "india_emandate_no_autoretry",
        "metric": None,
        "limit_value": 0,
        "window_seconds": None,
        "verdict": VERDICT_DENY,
        "applies_when": {"issuer_country": "IN"},
        "source": "RBI e-mandate: India-issued instruments need pre-debit authorisation; auto-retry not permitted",
    },
    # --- Razorpay's real retry model, as rows (docs: Subscriptions > Payment Retries)
    {
        "rule_id": "upi_daily_retry_cap",
        "metric": "retry_count_24h",
        "limit_value": 1,
        "window_seconds": 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": {"method": "upi"},
        "source": "Razorpay retry model: UPI re-attempted once daily (T+1, T+2, T+3), then halted",
    },
    {
        "rule_id": "card_daily_retry_cap",
        "metric": "retry_count_24h",
        "limit_value": 1,
        "window_seconds": 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": {"method": "card"},
        "source": "Razorpay retry model: cards re-attempted once daily (T+1, T+2, T+3), then halted",
    },
    {
        "rule_id": "emandate_retry_spacing_72h",
        "metric": "retry_count_72h",
        "limit_value": 1,
        "window_seconds": 72 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": {"method": "emandate"},
        "source": "Razorpay retry model: e-mandate retries only on confirmation/rejection (24h+)",
    },
    {
        "rule_id": "emandate_require_confirmation",
        "metric": None,
        "limit_value": 0,
        "window_seconds": None,
        "verdict": VERDICT_REQUIRE_HUMAN,
        "applies_when": {"method": "emandate", "confirmed": False},
        "source": "Razorpay retry model: never retry an unconfirmed mandate debit without review",
    },
    # --- Jurisdiction-keyed caps (regulators scrutinize per-jurisdiction frequency)
    {
        "rule_id": "contact_freq_24h_TRAI",
        "metric": "contacts_24h",
        "limit_value": 1,
        "window_seconds": 24 * 3600,
        "verdict": VERDICT_DENY,
        "applies_when": {"jurisdiction": "IN"},
        "source": "TRAI regime: 1 customer contact per 24h for Indian recipients (stricter than generic cap)",
    },
]


# ---------------------------------------------------------------------------
# Seeding — idempotent
# ---------------------------------------------------------------------------


async def seed_default_policy(
    session: AsyncSession,
    *,
    entity_type: str = "subscription",
    version: int = 1,
) -> int:
    """Insert default rows for an entity type at a version; idempotent."""
    inserted = 0
    for rule in DEFAULT_POLICY_RULES:
        existing = await session.get(Policy, (version, entity_type, rule["rule_id"]))
        if existing is not None:
            continue
        session.add(
            Policy(
                version=version,
                entity_type=entity_type,
                rule_id=rule["rule_id"],
                metric=rule["metric"],
                limit_value=rule["limit_value"],
                window_seconds=rule["window_seconds"],
                verdict=rule["verdict"],
                applies_when=rule["applies_when"],
                source=rule["source"],
            )
        )
        inserted += 1
    await session.flush()
    return inserted


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


async def active_policy_version(session: AsyncSession, entity_type: str) -> int:
    """The highest policy version in force for an entity type."""
    result = await session.execute(
        select(func.max(Policy.version)).where(Policy.entity_type == entity_type)
    )
    version = result.scalar_one_or_none()
    if version is None:
        raise LookupError(
            f"no policy rows for entity_type={entity_type!r} — "
            f"seed with seed_default_policy() first"
        )
    return version


async def get_rules(
    session: AsyncSession,
    entity_type: str,
    version: int,
) -> list[Policy]:
    """All rules in force for an entity type at a version (stable order)."""
    result = await session.execute(
        select(Policy)
        .where(Policy.version == version, Policy.entity_type == entity_type)
        .order_by(Policy.rule_id.asc())
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# applies_when matching
# ---------------------------------------------------------------------------


def rule_applies_to(rule: Policy, context: dict | None) -> bool:
    """True if applies_when is a subset of context; empty applies always."""
    if not rule.applies_when:
        return True
    if not context:
        return False
    return all(context.get(key) == value for key, value in rule.applies_when.items())


def metric_event_types(metric: str) -> set[str]:
    """Resolve a metric name to the L0 event types it counts.

    Unknown metrics are treated as raw event-type names, so new counters
    can be introduced by policy row alone (data, not code).
    """
    if metric in METRIC_EVENT_TYPES:
        return METRIC_EVENT_TYPES[metric]
    return {metric}
