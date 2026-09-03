"""Instate ops — traffic control, circuit breaking, spend guards (§10).

Graceful degradation covers *dependencies*; this module covers *traffic*.
Every mechanism here is small, testable, and framework-free — the webhook
receiver and the MCP surface compose them; the core never sees them.

- TokenBucket: the rate limiter. Per-merchant, per-surface.
- RateLimits: the per-merchant registry (reads ~100/min, writes ~30/min).
- CircuitBreaker: consecutive failures → open (fail fast to the
  deterministic policy default) → half-open probe. Prevents retry storms
  against a degraded dependency.
- SpendGuard: the LLM budget — global concurrency semaphore + per-merchant
  cost cap. Over budget → refuse-with-default-action (the fallback path
  already exists, §7).
"""

import asyncio
import time
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------


@dataclass
class BucketState:
    tokens: float
    last_refill: float


class TokenBucket:
    """Classic token bucket — capacity + refill per second, thread-safe
    enough for asyncio (single-threaded event loop, no awaits inside)."""

    def __init__(self, capacity: float, refill_per_second: float):
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._state = BucketState(tokens=capacity, last_refill=time.monotonic())

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._state.last_refill
        self._state.tokens = min(
            self.capacity, self._state.tokens + elapsed * self.refill_per_second
        )
        self._state.last_refill = now

    def try_take(self, amount: float = 1.0) -> bool:
        """Take a token if available; never blocks — the caller answers
        429 + Retry-After instead."""
        self._refill()
        if self._state.tokens >= amount:
            self._state.tokens -= amount
            return True
        return False

    def retry_after_seconds(self, amount: float = 1.0) -> float:
        """How long until a retry could succeed (for the Retry-After header)."""
        self._refill()
        deficit = amount - self._state.tokens
        if deficit <= 0:
            return 0.0
        return deficit / self.refill_per_second


# ---------------------------------------------------------------------------
# Per-merchant rate limits (§10: reads ~100/min, writes ~30/min)
# ---------------------------------------------------------------------------


@dataclass
class RateLimits:
    read_per_minute: int = 100
    write_per_minute: int = 30

    def _bucket(self, buckets: dict, key: tuple, capacity: int) -> TokenBucket:
        bucket = buckets.get(key)
        if bucket is None:
            bucket = TokenBucket(capacity=capacity, refill_per_second=capacity / 60)
            buckets[key] = bucket
        return bucket

    def allow_read(self, merchant_id: str) -> bool:
        return self._bucket(self._reads, merchant_id, self.read_per_minute).try_take()

    def allow_write(self, merchant_id: str) -> bool:
        return self._bucket(self._writes, merchant_id, self.write_per_minute).try_take()

    def retry_after_read(self, merchant_id: str) -> float:
        return self._bucket(self._reads, merchant_id, self.read_per_minute).retry_after_seconds()

    _reads: dict = field(default_factory=dict)
    _writes: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """CLOSED → (N consecutive failures) → OPEN → (cooldown) → HALF_OPEN.

    OPEN means fail fast: the caller uses the deterministic policy default
    instead of hammering a degraded dependency. HALF_OPEN lets one probe
    through; success closes, failure re-opens.
    """

    def __init__(self, failure_threshold: int = 5, cooldown_seconds: float = 30.0):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_probe_in_flight = False

    @property
    def state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.monotonic() - self._opened_at >= self.cooldown_seconds:
            return "half_open"
        return "open"

    def allow_request(self) -> bool:
        """closed → yes; open → no; half_open → one probe at a time."""
        state = self.state
        if state == "closed":
            return True
        if state == "open":
            return False
        if self._half_open_probe_in_flight:
            return False
        self._half_open_probe_in_flight = True
        return True

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None
        self._half_open_probe_in_flight = False

    def record_failure(self) -> None:
        if self.state == "half_open":
            # the probe failed — trip again, restart the cooldown
            self._opened_at = time.monotonic()
            self._half_open_probe_in_flight = False
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._opened_at = time.monotonic()


# ---------------------------------------------------------------------------
# LLM spend guard
# ---------------------------------------------------------------------------


class SpendGuard:
    """The LLM budget (§10): a global concurrency semaphore + per-merchant
    cost caps. Over budget → refuse-with-default-action (the deterministic
    fallback path already exists — the guard just invokes it earlier).

    Cost is recorded in micros (1e-6 currency units), matching
    `decisions.cost_micros`.
    """

    def __init__(self, max_in_flight: int = 5, per_merchant_budget_micros: int | None = None):
        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._budget = per_merchant_budget_micros
        self._spent: dict[str, int] = {}

    @property
    def max_in_flight(self) -> int:
        return self._semaphore._value  # noqa: SLF001 — read-only peek for metrics

    async def acquire(self, merchant_id: str, estimated_cost_micros: int = 0) -> bool:
        """May this LLM call proceed at all? (concurrency + budget)"""
        if self._budget is not None:
            if self._spent.get(merchant_id, 0) + estimated_cost_micros > self._budget:
                return False
        await self._semaphore.acquire()
        return True

    def release(self) -> None:
        self._semaphore.release()

    def record_cost(self, merchant_id: str, cost_micros: int) -> None:
        self._spent[merchant_id] = self._spent.get(merchant_id, 0) + cost_micros

    def spent(self, merchant_id: str) -> int:
        return self._spent.get(merchant_id, 0)
