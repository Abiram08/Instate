"""L3 precedent: embedded resolved-case summaries, advisory only (§4).

Never gates an action; [] is a normal answer. Pre-filter by type, cause,
and scope before similarity rank.
"""

import hashlib
import math
import re
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    Case,
    EMBEDDING_DIMS,
    EntityState,
    Event,
    STATUS_RECOVERED,
)
from instate.core.projection import get_windowed_count


# ---------------------------------------------------------------------------
# Embedders — the protocol the tier depends on
# ---------------------------------------------------------------------------


class Embedder(Protocol):
    """Text → fixed-dim vector. Swap implementations freely."""

    def embed(self, text: str) -> list[float]: ...


_TOKEN_RE = re.compile(r"[a-z0-9_]+")


class HashingEmbedder:
    """Deterministic bag-of-tokens embedder; cosine ≈ keyword overlap."""

    def __init__(self, dims: int = EMBEDDING_DIMS):
        self.dims = dims

    def embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dims
        for token in _TOKEN_RE.findall(text.lower()):
            idx = int(hashlib.sha256(token.encode()).hexdigest()[:8], 16) % self.dims
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


class GeminiEmbedder:
    """Production embedder with the same protocol; imported lazily."""

    def __init__(self, model: str = "gemini-embedding-001", api_key: str | None = None):
        from google import genai  # lazy

        self._model = model
        self._client = genai.Client(api_key=api_key) if api_key else genai.Client()

    def embed(self, text: str) -> list[float]:
        from google.genai import types

        response = self._client.models.embed_content(
            model=self._model,
            contents=text,
            config=types.EmbedContentConfig(output_dimensionality=EMBEDDING_DIMS),
        )
        return list(response.embeddings[0].values)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return dot / (na * nb)


# ---------------------------------------------------------------------------
# Case building — resolved entities become {situation → action → outcome}
# ---------------------------------------------------------------------------


def case_situation(
    *,
    entity_id: str,
    root_cause: str,
    amount_minor: int | None,
    retries_7d: int,
) -> str:
    """Compact, structured, one-line situation — the embedding target."""
    amount = f"{amount_minor / 100:.0f} INR" if amount_minor else "unknown amount"
    return (
        f"{entity_id}: subscription charge failed ({root_cause}); "
        f"amount {amount}; {retries_7d} retries in past 7 days"
    )


async def build_case_from_entity(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity: EntityState,
    embedder: Embedder | None = None,
) -> Case | None:
    """Summarize a resolved entity into a case; unresolved → None."""
    if entity.status != STATUS_RECOVERED:
        return None

    events = await session.execute(
        select(Event)
        .where(Event.merchant_id == merchant_id, Event.entity_id == entity.entity_id)
        .order_by(Event.id.asc())
    )
    timeline = list(events.scalars().all())
    if not timeline:
        return None

    root_cause = entity.last_failure_reason or "unknown"
    retries_7d = await get_windowed_count(
        session,
        merchant_id,
        entity.entity_id,
        "retry_count_7d",
        timedelta(days=7),
        now=max(e.occurred_at for e in timeline),
    )
    action_taken = "human_resolution"
    for event in timeline:
        if event.event_type in ("RetryAttempted", "RetrySucceeded"):
            action_taken = "retry"
        elif event.event_type == "PaymentLinkSent":
            action_taken = "payment_link"
        elif event.event_type == "PromiseMade" and entity.status == STATUS_RECOVERED:
            action_taken = "promise_to_pay"

    outcome = "recovered"
    recovered_minor = None
    for event in timeline:
        amount = (event.payload or {}).get("amount_minor")
        if event.event_type in ("RetrySucceeded", "PromiseHonored", "HumanResolved"):
            if amount:
                recovered_minor = amount

    embedder = embedder or HashingEmbedder()
    situation = case_situation(
        entity_id=entity.entity_id,
        root_cause=root_cause,
        amount_minor=entity.amount_at_risk_minor,
        retries_7d=retries_7d,
    )
    case = Case(
        merchant_id=merchant_id,
        scope="private",
        entity_type=entity.entity_type,
        root_cause=root_cause,
        situation=situation,
        action_taken=action_taken,
        outcome=outcome,
        recovered_minor=recovered_minor,
        embedding=embedder.embed(f"{situation} action:{action_taken}"),
        source_entity_id=entity.entity_id,  # UNIQUE → one case per entity
    )
    return case


async def seed_precedents(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    embedder: Embedder | None = None,
) -> int:
    """Turn every resolved entity into a case (idempotent by entity)."""
    states = await session.execute(
        select(EntityState).where(
            EntityState.merchant_id == merchant_id,
            EntityState.status == STATUS_RECOVERED,
        )
    )
    inserted = 0
    for entity in states.scalars():
        existing = await session.execute(
            select(Case.case_id).where(
                Case.merchant_id == merchant_id,
                Case.source_entity_id == entity.entity_id,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue
        case = await build_case_from_entity(
            session, merchant_id=merchant_id, entity=entity, embedder=embedder
        )
        if case is None:
            continue
        session.add(case)
        inserted += 1
    await session.flush()
    return inserted


# ---------------------------------------------------------------------------
# find_precedent — pre-filter, THEN rank (never a flat search)
# ---------------------------------------------------------------------------


async def find_precedent(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    entity_type: str,
    root_cause: str,
    query_text: str | None = None,
    query_embedding: list[float] | None = None,
    embedder: Embedder | None = None,
    top_k: int = 3,
) -> list[dict]:
    """Top-k resolved cases, advisory only; [] on any failure path."""
    # Pre-filter by type + cause + scope; never a flat search (§4).
    result = await session.execute(
        select(Case).where(
            Case.entity_type == entity_type,
            Case.root_cause == root_cause,
            (Case.scope == "network") | (Case.merchant_id == merchant_id),
        )
    )
    candidates = list(result.scalars().all())
    if not candidates:
        return []

    if query_embedding is None:
        if query_text is None:
            return []
        query_embedding = (embedder or HashingEmbedder()).embed(query_text)

    ranked = sorted(
        candidates,
        key=lambda c: cosine_similarity(query_embedding, c.embedding or []),
        reverse=True,
    )
    return [
        {
            "case_id": c.case_id,
            "scope": c.scope,
            "situation": c.situation,
            "action_taken": c.action_taken,
            "outcome": c.outcome,
            "recovered_minor": c.recovered_minor,
            "similarity": round(cosine_similarity(query_embedding, c.embedding or []), 4),
        }
        for c in ranked[:top_k]
    ]
