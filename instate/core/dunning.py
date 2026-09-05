"""Dunning outreach cadences as versioned data (§6). Advisory to the model; caps still come from L2."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_SEND_PAYMENT_LINK,
    DunningSequence,
    Event,
)

# Outreach touch types: the step counter. Money attempts are not steps.
SEQUENCE_TOUCH_TYPES = {"CustomerContacted", "PaymentLinkSent", "RecoveryActionSent"}

DEFAULT_SEQUENCES: list[dict] = [
    # insufficient_funds: link day 1, reminder day 3, SMS nudge day 7
    {
        "root_cause": "insufficient_funds",
        "step_index": 0,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "payment_link",
        "delay_hours": 24,
        "source": "dunning cadence: first touch carries the link",
    },
    {
        "root_cause": "insufficient_funds",
        "step_index": 1,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "email",
        "delay_hours": 72,
        "source": "dunning cadence: reminder before payday window",
    },
    {
        "root_cause": "insufficient_funds",
        "step_index": 2,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "sms",
        "delay_hours": 168,
        "source": "dunning cadence: SMS nudge as last outreach before pause",
    },
    # card_expired: link immediately, one reminder — then the method path owns it
    {
        "root_cause": "card_expired",
        "step_index": 0,
        "action": ACTION_REQUEST_PAYMENT_METHOD,
        "channel": "payment_link",
        "delay_hours": 1,
        "source": "dunning cadence: method update first, always",
    },
    {
        "root_cause": "card_expired",
        "step_index": 1,
        "action": ACTION_REQUEST_PAYMENT_METHOD,
        "channel": "whatsapp",
        "delay_hours": 48,
        "source": "dunning cadence: WhatsApp follow-up (Razorpay's lead channel in India)",
    },
    # network_timeout: one immediate retry, then a link if still failing
    {
        "root_cause": "network_timeout",
        "step_index": 0,
        "action": ACTION_RETRY_NOW,
        "channel": None,
        "delay_hours": 0,
        "source": "dunning cadence: transient — retry now, talk later",
    },
    {
        "root_cause": "network_timeout",
        "step_index": 1,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "payment_link",
        "delay_hours": 2,
        "source": "dunning cadence: still failing means customer action needed",
    },
    # customer_initiated: contact, never charge — link then human
    {
        "root_cause": "customer_initiated",
        "step_index": 0,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "email",
        "delay_hours": 24,
        "source": "dunning cadence: contact, don't charge",
    },
    {
        "root_cause": "customer_initiated",
        "step_index": 1,
        "action": ACTION_ESCALATE_HUMAN,
        "channel": None,
        "delay_hours": 72,
        "source": "dunning cadence: silence after contact means a person, not more mail",
    },
    # customer_error: fix-it link immediately (wrong PIN/CVV is correctable in minutes)
    {
        "root_cause": "customer_error",
        "step_index": 0,
        "action": ACTION_SEND_PAYMENT_LINK,
        "channel": "payment_link",
        "delay_hours": 1,
        "source": "dunning cadence: input errors are correctable in minutes",
    },
]


async def seed_default_sequences(
    session: AsyncSession,
    *,
    version: int = 1,
) -> int:
    """Seed the outreach cadences (idempotent)."""
    inserted = 0
    for row in DEFAULT_SEQUENCES:
        exists = await session.get(DunningSequence, (version, row["root_cause"], row["step_index"]))
        if exists is not None:
            continue
        session.add(DunningSequence(version=version, **row))
        inserted += 1
    await session.flush()
    return inserted


async def outreach_touch_count(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
) -> int:
    """All-time outreach touches for an entity — the sequence step index."""
    result = await session.execute(
        select(func.count(Event.id)).where(
            Event.merchant_id == merchant_id,
            Event.entity_id == entity_id,
            Event.event_type.in_(SEQUENCE_TOUCH_TYPES),
        )
    )
    return result.scalar_one()


async def next_sequence_step(
    session: AsyncSession,
    *,
    root_cause: str,
    merchant_id: UUID,
    entity_id: str,
    version: int = 1,
) -> dict | None:
    """Next dunning step for this entity, or None past the last step. Advisory only."""
    touches = await outreach_touch_count(session, merchant_id=merchant_id, entity_id=entity_id)
    result = await session.execute(
        select(DunningSequence).where(
            DunningSequence.version == version,
            DunningSequence.root_cause == root_cause,
            DunningSequence.step_index == touches,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return {
        "step_index": row.step_index,
        "action": row.action,
        "channel": row.channel,
        "delay_hours": row.delay_hours,
    }
