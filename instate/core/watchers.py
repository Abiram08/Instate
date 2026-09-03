"""Instate watchers — memory that initiates (§2, the Context.dev monitor pattern).

A memory layer that only answers questions is half a product: agents that
act over time also need to be TOLD when something changes. A watcher is a
condition over L1/L2 facts — integers and timestamps, never embeddings
(the same trust rule as the gates) — that pushes a SIGNED webhook when it
trips. The agent doesn't poll Instate; Instate calls the agent.

Built-in conditions (all computable from L1 + indexed L0 counts):
  - retry_count_7d >= threshold   (warn BEFORE the ceiling — the same
    indexed count the gates use)
  - open_ptp_due                  (promise-to-pay overdue)
  - stale_awaiting >= N days      (AWAITING_PROMISE with no activity)

Cooldown prevents re-fire spam; the tick loop owns the checks.
"""

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import (
    STATUS_AWAITING_PROMISE,
    EntityState,
    Watcher,
)
from instate.core.projection import get_windowed_count


class Notifier(Protocol):
    """How a tripped watcher reaches the outside world."""

    async def send(self, url: str, payload: dict, signature: str | None) -> bool: ...


class HTTPXNotifier:
    """Signed webhook delivery — the signature lets the receiver prove the
    push came from Instate, the same trust direction as webhook intake."""

    def __init__(self, timeout_seconds: float = 10.0):
        self._timeout = timeout_seconds

    async def send(self, url: str, payload: dict, signature: str | None) -> bool:
        import httpx  # lazy

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(
                    url,
                    json=payload,
                    headers={"X-Instate-Signature": signature} if signature else {},
                )
                return resp.status_code < 300
        except Exception:
            return False  # delivery failure is a degradation, not an outage


def sign_payload(payload: dict, secret: str) -> str | None:
    """HMAC-SHA256 over the canonical payload — mirrors webhook intake."""
    if not secret:
        return None
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()


def _condition_met(
    condition: dict,
    *,
    state: EntityState | None,
    retry_count_7d: int,
    now: datetime,
) -> bool:
    metric = condition.get("metric")
    op = condition.get("op", ">=")
    threshold = condition.get("threshold", 0)

    if metric == "retry_count_7d":
        value = retry_count_7d
    elif metric == "open_ptp_due":
        if state is None or state.open_ptp_due_at is None:
            return False
        value = 1 if state.open_ptp_due_at < now else 0
        threshold = 1
        op = ">="
    elif metric == "stale_awaiting":
        if state is None or state.status != STATUS_AWAITING_PROMISE:
            return False
        anchor = state.last_contact_at or now
        value = int((now - anchor).total_seconds() // 86400)
    else:
        return False

    if op == ">=":
        return value >= threshold
    if op == ">":
        return value > threshold
    if op == "==":
        return value == threshold
    return False


async def check_watchers(
    session: AsyncSession,
    *,
    notifier: Notifier,
    now: datetime | None = None,
) -> int:
    """Tick-loop half: evaluate every active watcher, fire with cooldown.

    Returns how many webhooks were pushed. A watcher that trips for an
    entity fires once per cooldown window — memory that initiates should
    nudge, not spam.
    """
    now = now or datetime.now(UTC)
    watchers = await session.execute(
        select(Watcher).where(Watcher.active.is_(True)).order_by(Watcher.id.asc())
    )
    fired = 0

    for watcher in watchers.scalars():
        # Cooldown
        if watcher.last_fired_at and now - watcher.last_fired_at < timedelta(
            seconds=watcher.cooldown_seconds
        ):
            continue

        # Candidate entities: scoped by type (+ optional entity_id pin)
        q = select(EntityState).where(
            EntityState.merchant_id == watcher.merchant_id,
            EntityState.entity_type == watcher.entity_type,
        )
        if watcher.entity_id:
            q = q.where(EntityState.entity_id == watcher.entity_id)
        states = await session.execute(q)

        for state in states.scalars():
            retry_count_7d = await get_windowed_count(
                session,
                state.merchant_id,
                state.entity_id,
                "retry_count_7d",
                timedelta(days=7),
                now=now,
            )
            if not _condition_met(
                watcher.condition, state=state, retry_count_7d=retry_count_7d, now=now
            ):
                continue

            payload = {
                "kind": "instate.watcher.fired",
                "merchant_id": str(watcher.merchant_id),
                "entity_id": state.entity_id,
                "entity_type": state.entity_type,
                "status": state.status,
                "condition": watcher.condition,
                "observed": {
                    "retry_count_7d": retry_count_7d,
                    "open_ptp_due_at": (
                        state.open_ptp_due_at.isoformat() if state.open_ptp_due_at else None
                    ),
                },
                "fired_at": now.isoformat(),
            }
            signature = sign_payload(payload, watcher.secret)
            delivered = await notifier.send(watcher.target_url, payload, signature)
            if delivered:
                watcher.last_fired_at = now
                await session.commit()
                fired += 1

    return fired


async def seed_default_watchers(
    session: AsyncSession,
    *,
    merchant_id: UUID,
    target_url: str,
    secret: str = "watcher-secret",
    entity_type: str = "subscription",
) -> int:
    """The three demo watchers (idempotent per URL+condition)."""
    defaults = [
        {"metric": "retry_count_7d", "op": ">=", "threshold": 2},
        {"metric": "open_ptp_due", "op": "<", "threshold": 0},
        {"metric": "stale_awaiting", "op": ">=", "threshold": 3},
    ]
    existing_rows = await session.execute(
        select(Watcher).where(
            Watcher.merchant_id == merchant_id,
            Watcher.entity_type == entity_type,
            Watcher.target_url == target_url,
        )
    )
    existing_conditions = [w.condition for w in existing_rows.scalars().all()]

    inserted = 0
    for condition in defaults:
        if condition in existing_conditions:
            continue
        session.add(
            Watcher(
                merchant_id=merchant_id,
                entity_type=entity_type,
                condition=condition,
                target_url=target_url,
                secret=secret,
            )
        )
        inserted += 1
    await session.commit()
    return inserted
