"""Tests for the ops layer — rate limits, circuit breaker, spend guard.

Small mechanisms, hard assertions: the token bucket refills, the breaker
goes closed → open → half-open → closed, and the spend guard refuses
over-budget merchants to the deterministic default path.
"""

import asyncio

from instate.core.ops import CircuitBreaker, RateLimits, SpendGuard, TokenBucket


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------


def test_token_bucket_denies_when_empty_then_refills():
    bucket = TokenBucket(capacity=3, refill_per_second=10)
    assert bucket.try_take() is True
    assert bucket.try_take() is True
    assert bucket.try_take() is True
    assert bucket.try_take() is False  # empty
    assert bucket.retry_after_seconds() > 0
    asyncio.run(asyncio.sleep(0.15))  # 0.15s × 10/s = 1.5 tokens
    assert bucket.try_take() is True


def test_token_bucket_capacity_is_a_ceiling():
    bucket = TokenBucket(capacity=2, refill_per_second=100)
    asyncio.run(asyncio.sleep(0.05))
    # bursts can't exceed capacity
    assert bucket.try_take() is True
    assert bucket.try_take() is True
    assert bucket.try_take() is False


def test_rate_limits_per_merchant():
    limits = RateLimits(read_per_minute=2, write_per_minute=1)
    assert limits.allow_read("m1") is True
    assert limits.allow_read("m1") is True
    assert limits.allow_read("m1") is False  # m1 exhausted
    assert limits.allow_read("m2") is True  # m2 unaffected — isolation


def test_rate_limits_reads_and_writes_are_separate_buckets():
    limits = RateLimits(read_per_minute=1, write_per_minute=1)
    assert limits.allow_read("m1") is True
    assert limits.allow_read("m1") is False
    assert limits.allow_write("m1") is True  # writes don't share the read bucket
    assert limits.allow_write("m1") is False


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


def test_breaker_stays_closed_below_threshold():
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=0.05)
    for _ in range(2):
        breaker.record_failure()
    assert breaker.state == "closed"
    assert breaker.allow_request() is True


def test_breaker_opens_at_threshold_and_fails_fast():
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=60)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.state == "open"
    assert breaker.allow_request() is False  # fail fast, no retry storms


def test_breaker_half_open_probe_then_close():
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=0.01)
    breaker.record_failure()
    assert breaker.state == "open"
    asyncio.run(asyncio.sleep(0.02))

    assert breaker.state == "half_open"
    assert breaker.allow_request() is True  # one probe
    assert breaker.allow_request() is False  # no second probe in flight
    breaker.record_success()
    assert breaker.state == "closed"
    assert breaker.allow_request() is True


def test_breaker_half_open_probe_failure_reopens():
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=0.01)
    breaker.record_failure()
    asyncio.run(asyncio.sleep(0.02))

    assert breaker.allow_request() is True  # the probe
    breaker.record_failure()  # probe failed → re-open
    assert breaker.state == "open"


def test_breaker_success_resets_the_counter():
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=60)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()  # one success resets the streak
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == "closed"


# ---------------------------------------------------------------------------
# Spend guard
# ---------------------------------------------------------------------------


async def test_spend_guard_concurrency_semaphore():
    guard = SpendGuard(max_in_flight=2)
    assert await guard.acquire("m1") is True
    assert await guard.acquire("m1") is True
    # third concurrent call waits on the semaphore — release + acquire in a task
    task = asyncio.ensure_future(guard.acquire("m2"))
    await asyncio.sleep(0.01)
    assert not task.done()  # blocked at capacity
    guard.release()
    await asyncio.sleep(0.01)
    assert task.done() and task.result() is True


async def test_spend_guard_budget_cap_per_merchant():
    guard = SpendGuard(max_in_flight=10, per_merchant_budget_micros=1_000_000)
    guard.record_cost("m1", 900_000)
    assert await guard.acquire("m1", estimated_cost_micros=50_000) is True  # still under
    guard.record_cost("m1", 50_000)
    assert await guard.acquire("m1", estimated_cost_micros=100_000) is False  # over → refuse
    # → the caller uses the deterministic policy default (§7 fallback)
    assert await guard.acquire("m2", estimated_cost_micros=900_000) is True  # m2 unaffected


async def test_spend_guard_records_accumulate():
    guard = SpendGuard(max_in_flight=10)
    guard.record_cost("m1", 100)
    guard.record_cost("m1", 200)
    assert guard.spent("m1") == 300
    assert guard.spent("m2") == 0
