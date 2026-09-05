"""Append-only L0 event ledger with per-entity hash chains (§1).
Idempotent by source_event_id; payload_hash survives payload redaction.
"""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import ArchiveAnchor, Event


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------


def compute_payload_hash(payload: dict[str, Any] | None) -> bytes:
    """Deterministic payload hash (sorted keys).
    None hashes as b"null" so the chain verifies after redaction.
    """
    if payload is None:
        return hashlib.sha256(b"null").digest()
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).digest()


def compute_event_hash(
    prev_hash: bytes | None,
    merchant_id: UUID | str,
    entity_id: str,
    event_type: str,
    occurred_at: datetime,
    payload_hash: bytes,
) -> bytes:
    """Per-entity chain hash over prev_hash, merchant, entity, type, time, payload_hash.

    None prev_hash = genesis event.
    """
    h = hashlib.sha256()
    h.update(prev_hash if prev_hash else b"")
    h.update(str(merchant_id).encode("utf-8"))
    h.update(entity_id.encode("utf-8"))
    h.update(event_type.encode("utf-8"))
    # Normalize timestamp to ISO 8601 UTC for stable hashing
    ts = occurred_at if occurred_at.tzinfo else occurred_at.replace(tzinfo=UTC)
    h.update(ts.astimezone(UTC).isoformat().encode("utf-8"))
    h.update(payload_hash)
    return h.digest()


# ---------------------------------------------------------------------------
# record_event — the ONLY way events enter the ledger
# ---------------------------------------------------------------------------


class DuplicateEventError(Exception):
    """source_event_id already exists; caller treats as inert no-op."""

    def __init__(self, source_event_id: str):
        self.source_event_id = source_event_id
        super().__init__(f"duplicate source_event_id: {source_event_id}")


async def record_event(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_id: str,
    entity_type: str,
    event_type: str,
    occurred_at: datetime,
    payload: dict[str, Any] | None = None,
    source_event_id: str | None = None,
    decision_id: int | None = None,
    channel: str | None = None,
) -> Event:
    """Append one event to L0; idempotent by source_event_id.

    Raises DuplicateEventError on redelivery. `channel` defaults to
    payload["channel"] so per-channel caps stay indexed.
    """
    if source_event_id is not None:
        existing = await session.execute(
            select(Event.id).where(Event.source_event_id == source_event_id)
        )
        if existing.scalar_one_or_none() is not None:
            raise DuplicateEventError(source_event_id)

    last_hash_result = await session.execute(
        select(Event.hash)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.id.desc())
        .limit(1)
    )
    prev_hash = last_hash_result.scalar_one_or_none()

    p_hash = compute_payload_hash(payload)
    e_hash = compute_event_hash(
        prev_hash, merchant_id, entity_id, event_type, occurred_at, p_hash
    )

    if channel is None and isinstance(payload, dict):
        candidate = payload.get("channel")
        channel = candidate if isinstance(candidate, str) else None
    event = Event(
        merchant_id=merchant_id,
        entity_id=entity_id,
        entity_type=entity_type,
        event_type=event_type,
        occurred_at=occurred_at,
        payload=payload,
        payload_hash=p_hash,
        source_event_id=source_event_id,
        decision_id=decision_id,
        channel=channel,
        prev_hash=prev_hash,
        hash=e_hash,
    )
    session.add(event)
    await session.flush()  # flush to get the id, caller commits

    return event


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


async def get_timeline(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
    as_of: datetime | None = None,
) -> list[Event]:
    """Entity history oldest-first; default limit bounds full-table reads (§8).

    `as_of` pins a snapshot: only events recorded at or before it are
    visible. Reads default to the latest view (causal hot path); pass `as_of`
    for a point-in-time read — "what was true when this was decided".
    """
    query = (
        select(Event)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.occurred_at.asc(), Event.id.asc())
        .offset(offset)
        .limit(limit)
    )
    if as_of is not None:
        query = query.where(Event.recorded_at <= as_of)
    result = await session.execute(query)
    return list(result.scalars().all())


async def get_event_by_source_id(
    session: AsyncSession, source_event_id: str
) -> Event | None:
    """Fetch an event by its source_event_id (webhook id)."""
    result = await session.execute(
        select(Event).where(Event.source_event_id == source_event_id)
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# verify_chain — tamper evidence
# ---------------------------------------------------------------------------


class ChainVerificationResult:
    """Verified flag + event count + break reason for one entity."""

    def __init__(
        self,
        entity_id: str,
        verified: bool,
        event_count: int,
        error: str | None = None,
        archived_prefix: bool = False,
    ):
        self.entity_id = entity_id
        self.verified = verified
        self.event_count = event_count
        self.error = error
        # True when the hot chain starts mid-history and an ArchiveAnchor
        # vouches for the cold prefix (cold rows + anchor + hot rows).
        self.archived_prefix = archived_prefix

    def __repr__(self) -> str:
        if self.verified:
            suffix = " (via archive anchor)" if self.archived_prefix else ""
            return (
                f"✓ chain verified: {self.entity_id} "
                f"({self.event_count} events, zero breaks{suffix})"
            )
        return f"✗ chain BROKEN: {self.entity_id} ({self.error})"


async def verify_chain(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
) -> ChainVerificationResult:
    """Verify an entity's chain in insertion (id) order, not occurred_at (§1b).

    A cut starting mid-history verifies only if the latest ArchiveAnchor
    vouches for exactly it (anchor_hash == prev_hash, older event id).
    """
    result = await session.execute(
        select(Event)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.id.asc())
        .limit(10_000)
    )
    events = list(result.scalars().all())

    if not events:
        return ChainVerificationResult(entity_id, verified=True, event_count=0)

    prev_hash: bytes | None = None
    archived_prefix = False
    for i, event in enumerate(events):
        if event.prev_hash != prev_hash:
            if i == 0 and event.prev_hash is not None and await _anchor_vouches(
                session, merchant_id, entity_id, event
            ):
                archived_prefix = True
            else:
                return ChainVerificationResult(
                    entity_id,
                    verified=False,
                    event_count=len(events),
                    error=f"event {event.id} (index {i}): prev_hash mismatch — "
                    f"expected {prev_hash.hex()[:16] if prev_hash else 'None'}, "
                    f"got {event.prev_hash.hex()[:16] if event.prev_hash else 'None'}",
                )

        expected = compute_event_hash(
            event.prev_hash,
            event.merchant_id,
            event.entity_id,
            event.event_type,
            event.occurred_at,
            event.payload_hash,
        )
        if event.hash != expected:
            return ChainVerificationResult(
                entity_id,
                verified=False,
                event_count=len(events),
                error=f"event {event.id} (index {i}): hash mismatch — "
                f"payload may have been tampered with",
            )

        prev_hash = event.hash

    return ChainVerificationResult(
        entity_id, verified=True, event_count=len(events), archived_prefix=archived_prefix
    )


async def _anchor_vouches(
    session: AsyncSession,
    merchant_id: UUID,
    entity_id: str,
    first_hot_event: Event,
) -> bool:
    """True iff the latest anchor vouches for this exact cut link."""
    result = await session.execute(
        select(ArchiveAnchor)
        .where(
            ArchiveAnchor.merchant_id == merchant_id,
            ArchiveAnchor.entity_id == entity_id,
        )
        .order_by(ArchiveAnchor.id.desc())
        .limit(1)
    )
    anchor = result.scalar_one_or_none()
    return (
        anchor is not None
        and anchor.anchor_hash == first_hot_event.prev_hash
        and anchor.archived_through_event_id < first_hot_event.id
    )


# ---------------------------------------------------------------------------
# Redaction — PII ages out, the chain still verifies
# ---------------------------------------------------------------------------


async def redact_payload(
    session: AsyncSession,
    event_id: int,
) -> bool:
    """Null an event payload; payload_hash stays so the chain verifies (§1c).

    The one sanctioned UPDATE to events, and only the payload column.
    """
    from sqlalchemy import update

    result = await session.execute(
        update(Event)
        .where(Event.id == event_id)
        .values(payload=None)
    )
    await session.flush()
    return result.rowcount > 0
