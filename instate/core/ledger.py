"""Instate L0 ledger — append-only event storage with per-entity hash chains.

This is the truth tier (§1 of architecture.md). Every function here
writes to or reads from the `events` table. Nothing ever updates or
deletes an event.

Key invariants:
- record_event() is idempotent by source_event_id (webhook redelivery is inert)
- The hash chain is per-entity: prev_hash = hash of the previous event
  for the SAME (merchant_id, entity_id), not a global chain
- payload_hash is computed from the serialized payload; it survives
  payload redaction (payload → NULL) without breaking the chain
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
    """Hash the payload deterministically (sorted keys → stable output).

    Returns a fixed 32-byte digest even for None, so the chain
    still verifies after payload redaction (payload → NULL).
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
    """Compute the per-entity chain hash.

    hash = sha256(prev_hash || merchant_id || entity_id || event_type
                  || occurred_at || payload_hash)

    prev_hash is None for the first event of an entity (genesis).
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
    """Raised when a source_event_id already exists (webhook redelivery).

    The caller treats this as a no-op (inert), not an error condition
    worth alarming on — Razorpay retries delivery as a matter of course.
    """

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
) -> Event:
    """Append an event to L0. Idempotent by source_event_id.

    This is the single entry point for all writes to the events table.
    It:
    1. Checks dedupe (source_event_id UNIQUE constraint)
    2. Fetches the entity's last hash (per-entity chain)
    3. Computes payload_hash and event hash
    4. INSERTs (append-only)

    Returns the persisted Event. Raises DuplicateEventError if
    source_event_id already exists — treat as a no-op.
    """
    # 1. Dedupe check (application-level; the UNIQUE constraint is the backstop)
    if source_event_id is not None:
        existing = await session.execute(
            select(Event.id).where(Event.source_event_id == source_event_id)
        )
        if existing.scalar_one_or_none() is not None:
            raise DuplicateEventError(source_event_id)

    # 2. Fetch the entity's previous hash (per-entity chain, NOT global)
    last_hash_result = await session.execute(
        select(Event.hash)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.id.desc())
        .limit(1)
    )
    prev_hash = last_hash_result.scalar_one_or_none()

    # 3. Compute hashes
    p_hash = compute_payload_hash(payload)
    e_hash = compute_event_hash(
        prev_hash, merchant_id, entity_id, event_type, occurred_at, p_hash
    )

    # 4. Append (INSERT only — no UPDATE, no DELETE, ever)
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
) -> list[Event]:
    """Ordered event history for an entity (oldest first).

    `limit` has a sane default so a naive caller cannot dump the
    entire event log into their context window (poka-yoke, §8).
    """
    result = await session.execute(
        select(Event)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
        .order_by(Event.occurred_at.asc(), Event.id.asc())
        .offset(offset)
        .limit(limit)
    )
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
    """Result of walking an entity's hash chain."""

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
    """Walk an entity's hash chain and verify integrity.

    The chain is built in INSERTION order (that's what prev_hash links),
    so verification walks by event id — NOT by occurred_at. A late or
    out-of-order webhook appends at the chain's tail even though its
    valid-time (occurred_at) is older; bi-temporal reads sort by
    occurred_at, chain verification sorts by id. Conflating the two is
    exactly how a tamper-evidence check produces false breaks.

    For each event (in insertion order):
    - prev_hash must equal the previous event's hash (None for genesis)
    - hash must equal sha256(prev_hash || merchant || entity || type || time || payload_hash)

    After cold archival the hot chain no longer starts at genesis: its
    first event's prev_hash points at an archived event. That link is
    accepted IFF the latest ArchiveAnchor for the entity vouches for
    exactly it (anchor_hash == prev_hash and the anchor covers an older
    event id) — cold rows + anchor + hot rows verify as one chain. A
    cut with no (or a tampered) anchor fails honestly.

    Returns a ChainVerificationResult with verified=True if the chain
    is intact (zero breaks), or verified=False with the specific error.
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
        # Check the link
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

        # Recompute the hash
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
    """Does the latest archive anchor vouch for this exact cut link?

    True iff an anchor exists with anchor_hash == the hot event's
    prev_hash AND the anchor covers a strictly older event id — i.e.
    the cold prefix this hot chain continues from is exactly the one
    that was archived. Anything else (no anchor, tampered anchor,
    anchor for a different cut) is a break, reported honestly.
    """
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
    """Null the payload of an event (data-retention / PII redaction).

    The payload_hash stays, so the chain still verifies: you can prove
    a record was not altered AFTER deleting its contents (§1c).

    NOTE: this is the ONE sanctioned UPDATE to the events table,
    and it only nulls payload — nothing else. In production, this
    would be a DB-level trigger or a separate retention job, but
    the mechanism is the same: payload → NULL, payload_hash stays.
    """
    from sqlalchemy import update

    result = await session.execute(
        update(Event)
        .where(Event.id == event_id)
        .values(payload=None)
    )
    await session.flush()
    return result.rowcount > 0
