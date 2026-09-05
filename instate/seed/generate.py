"""Deterministic synthetic history and failure batches (seeded RNG).
Same seed → identical history, so baseline and agent run over identical batches.
Covers recovery, promise, escalation, and ceiling patterns.
"""

import random
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import Event
from instate.core.projection import fold_events

AMOUNTS = [49900, 99900, 149900, 249900, 499900]  # minor units

# Failure codes exercised by the batch (covers every root cause + UNKNOWN)
BATCH_CODES = [
    "insufficient_funds",
    "GATEWAY_TIMEOUT",
    "CARD_EXPIRED",
    "insufficient_funds",
    "customer_cancelled",
    "FRAUD_DETECTED",
    "MANDATE_INACTIVE",
    "SOMETHING_NOVEL",  # UNKNOWN — the never-empty default must be exercised
    "insufficient_funds",
    "NETWORK_ERROR",
]


def _days_ago(base: datetime, days: float) -> datetime:
    return base - timedelta(days=days)


async def _emit(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    event_type: str,
    occurred_at: datetime,
    payload: dict | None = None,
    source_event_id: str | None = None,
    channel: str | None = None,
) -> Event:
    event = await record_event(
        session,
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type=event_type,
        occurred_at=occurred_at,
        payload=payload,
        source_event_id=source_event_id,
        channel=channel,
    )
    return event


async def _mark_diagnosed(
    session: AsyncSession,
    merchant_id: UUID,
    trigger: Event,
    root_cause: str,
    at: datetime,
) -> None:
    """Append FailureDiagnosed marker; drain skips marked failures via trigger_event_id."""
    await _emit(
        session,
        merchant_id,
        trigger.entity_id,
        trigger.entity_type,
        "FailureDiagnosed",
        at,
        {
            "root_cause": root_cause,
            "failure_code": (trigger.payload or {}).get("failure_code"),
            "trigger_event_id": trigger.id,
        },
        source_event_id=f"{trigger.id}:diag",
    )


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

ARCHETYPE_CYCLE = [
    "recovered_retry",
    "promise_keeper",
    "method_updater",
    "escalated",
    "at_ceiling",
    "promise_breaker",
    "still_open",
    "backup_routed",  # backup instrument recovery, zero customer action
    "whatsapp_recovered",  # WhatsApp-first contact → recovery (demo-visible)
    "recovered_retry",  # weighted: recoveries should be common
    "still_open",
    "escalated",
]


async def seed_history(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entities: int = 30,
    seed: int = 42,
    now: datetime | None = None,
) -> dict:
    """Write `entities` synthetic histories and fold; deterministic per seed.
    Returns {entities, events, checkouts}; history is marked diagnosed so the drain skips it.
    """
    now = now or datetime.now(UTC)
    rng = random.Random(seed)
    amount = lambda: rng.choice(AMOUNTS)  # noqa: E731 — tiny local helper
    tag = merchant_id.hex[:8]  # scope source ids per run — dedupe is global

    event_count = 0
    checkout_count = 0
    recovered_seen = 0

    async def start_case(entity_id: str, code: str, days_back: float) -> Event:
        nonlocal event_count
        failed = await _emit(
            session,
            merchant_id,
            entity_id,
            "subscription",
            "PaymentFailed",
            _days_ago(now, days_back),
            {"amount_minor": amount(), "failure_code": code},
            source_event_id=f"{tag}_{entity_id}_fail",
        )
        await _mark_diagnosed(
            session,
            merchant_id,
            failed,
            {
                "insufficient_funds": "insufficient_funds",
                "GATEWAY_TIMEOUT": "network_timeout",
                "CARD_EXPIRED": "card_expired",
                "FRAUD_DETECTED": "fraud_block",
                "MANDATE_INACTIVE": "mandate_inactive",
                "customer_cancelled": "customer_initiated",
            }.get(code, "UNKNOWN"),
            failed.occurred_at,
        )
        event_count += 2
        return failed

    for i in range(entities):
        archetype = ARCHETYPE_CYCLE[i % len(ARCHETYPE_CYCLE)]
        entity_id = f"sub_{i:03d}"
        n = "subscription"

        if archetype == "recovered_retry":
            recovered_seen += 1
            amt = amount()
            await start_case(entity_id, "insufficient_funds", 21)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 18),
                {"success": False},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 14),
                {"success": False},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 10),
                {"success": True},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetrySucceeded",
                _days_ago(now, 10),
                {"amount_minor": amt},
            )
            event_count += 4
            if recovered_seen >= 2:  # every recovery after the first gets charged back
                await _emit(
                    session,
                    merchant_id,
                    entity_id,
                    n,
                    "RecoveryReversed",
                    _days_ago(now, 6),
                    {"amount_minor": amt, "reason": "chargeback"},
                )
                event_count += 1

        elif archetype == "promise_keeper":
            amt = amount()
            await start_case(entity_id, "customer_cancelled", 15)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "CustomerContacted",
                _days_ago(now, 14),
                {"channel": "email"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "PromiseMade",
                _days_ago(now, 13),
                {"due_at": _days_ago(now, 5).isoformat()},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "PromiseHonored",
                _days_ago(now, 5),
                {"amount_minor": amt},
            )
            event_count += 3

        elif archetype == "promise_breaker":
            await start_case(entity_id, "customer_cancelled", 20)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "PromiseMade",
                _days_ago(now, 19),
                {"due_at": _days_ago(now, 10).isoformat()},
            )
            await _emit(session, merchant_id, entity_id, n, "PromiseBroken", _days_ago(now, 10), {})
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "EscalatedToHuman",
                _days_ago(now, 9),
                {"reason": "promise_broken"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "HumanResolved",
                _days_ago(now, 3),
                {"amount_minor": amount()},
            )
            event_count += 4

        elif archetype == "method_updater":
            amt = amount()
            await start_case(entity_id, "CARD_EXPIRED", 18)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "PaymentLinkSent",
                _days_ago(now, 17),
                {"channel": "payment_link"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "CustomerContacted",
                _days_ago(now, 17),
                {"channel": "payment_link"},
            )
            await _emit(
                session, merchant_id, entity_id, n, "PaymentMethodChanged", _days_ago(now, 6), {}
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 5),
                {"success": True},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetrySucceeded",
                _days_ago(now, 5),
                {"amount_minor": amt},
            )
            event_count += 5

        elif archetype == "escalated":
            await start_case(entity_id, "FRAUD_DETECTED", 16)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "EscalatedToHuman",
                _days_ago(now, 16),
                {"reason": "fraud_block"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "HumanResolved",
                _days_ago(now, 2),
                {"amount_minor": amount()},
            )
            event_count += 2

        elif archetype == "at_ceiling":
            await start_case(entity_id, "insufficient_funds", 6)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 5),
                {"success": False},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 3),
                {"success": False},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RetryAttempted",
                _days_ago(now, 1),
                {"success": False},
            )
            event_count += 3

        elif archetype == "backup_routed":
            amt = amount()
            await start_case(entity_id, "CARD_EXPIRED", 12)
            await _emit(
                session, merchant_id, entity_id, n, "PaymentMethodChanged",
                _days_ago(now, 11), {"via": "backup_on_file"},
            )
            await _emit(
                session, merchant_id, entity_id, n, "RetryAttempted",
                _days_ago(now, 10), {"success": True, "via": "backup", "zero_customer_action": True},
            )
            await _emit(
                session, merchant_id, entity_id, n, "RetrySucceeded",
                _days_ago(now, 10), {"amount_minor": amt, "via": "backup"},
            )
            event_count += 3

        elif archetype == "whatsapp_recovered":
            amt = amount()
            await start_case(entity_id, "insufficient_funds", 9)
            await _emit(
                session, merchant_id, entity_id, n, "CustomerContacted",
                _days_ago(now, 8), {"channel": "whatsapp"},
                source_event_id=f"{tag}_{entity_id}_wa", channel="whatsapp",
            )
            await _emit(
                session, merchant_id, entity_id, n, "PaymentLinkSent",
                _days_ago(now, 8), {"channel": "whatsapp"},
                source_event_id=f"{tag}_{entity_id}_walink", channel="whatsapp",
            )
            await _emit(
                session, merchant_id, entity_id, n, "RetrySucceeded",
                _days_ago(now, 6), {"amount_minor": amt},
            )
            event_count += 3

        else:  # still_open
            await start_case(entity_id, "insufficient_funds", 4)
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "RecoveryActionSent",
                _days_ago(now, 3),
                {"channel": "sms"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "CustomerContacted",
                _days_ago(now, 2.5),
                {"channel": "sms"},
            )
            await _emit(
                session,
                merchant_id,
                entity_id,
                n,
                "CustomerContacted",
                _days_ago(now, 2),
                {"channel": "email"},
            )
            event_count += 3

        # Every 5th entity also gets a thin checkout consumer
        if i % 5 == 0:
            chk = f"chk_{i:03d}"
            await _emit(
                session,
                merchant_id,
                chk,
                "checkout",
                "CheckoutAbandoned",
                _days_ago(now, rng.randint(2, 9)),
                {"amount_minor": amount()},
            )
            await _emit(
                session,
                merchant_id,
                chk,
                "checkout",
                "PaymentLinkSent",
                _days_ago(now, rng.randint(1, 2)),
                {"channel": "payment_link"},
            )
            event_count += 2
            checkout_count += 1

    await session.commit()
    await fold_events(session)
    await session.commit()
    return {
        "entities": entities,
        "events": event_count,
        "checkouts": checkout_count,
    }


# ---------------------------------------------------------------------------
# Fresh batch
# ---------------------------------------------------------------------------


async def generate_failure_batch(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    count: int | None = None,
    seed: int = 99,
    now: datetime | None = None,
    prefix: str = "batch",
    entity_ids: list[str] | None = None,
    codes: list[str] | None = None,
):
    """Append PaymentFailed events and return them, in order.
    Defaults to 10 fresh entities; `entity_ids`/`codes` override
    (count defaults to len(entity_ids)). Deterministic per seed.
    """
    now = now or datetime.now(UTC)
    rng = random.Random(seed)
    n = len(entity_ids) if (entity_ids is not None and count is None) else (count or 10)
    events = []
    for i in range(n):
        code = codes[i] if codes and i < len(codes) else BATCH_CODES[i % len(BATCH_CODES)]
        entity_id = entity_ids[i] if entity_ids and i < len(entity_ids) else f"{prefix}_{i:03d}"
        event = await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="PaymentFailed",
            occurred_at=now,
            payload={"amount_minor": rng.choice(AMOUNTS), "failure_code": code},
            source_event_id=f"wh_{prefix}_{i:03d}",
        )
        events.append(event)
    await session.commit()
    return events
