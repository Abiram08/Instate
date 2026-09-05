"""Watchers over L1/L2 facts with signed delivery and cooldown."""

from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import Watcher
from instate.core.projection import fold_events
from instate.core.watchers import (
    HTTPXNotifier,
    check_watchers,
    seed_default_watchers,
    sign_payload,
)
from tests.conftest import days_ago, make_merchant_id, now_utc


class RecordingNotifier:
    """Test notifier capturing pushes."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.pushes: list[tuple[str, dict, str | None]] = []

    async def send(self, url: str, payload: dict, signature: str | None) -> bool:
        if self.fail:
            return False
        self.pushes.append((url, payload, signature))
        return True


async def _watcher(
    session, merchant, condition, *, entity_id=None, cooldown=3600, url="https://agent.example/hook"
) -> Watcher:
    w = Watcher(
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        condition=condition,
        target_url=url,
        secret="s3cret",
        cooldown_seconds=cooldown,
    )
    session.add(w)
    await session.commit()
    return w


async def test_retry_warning_fires_before_the_ceiling(session: AsyncSession):
    """Pins warn at 2/3 before ceiling."""
    merchant = make_merchant_id()
    for i in range(2):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="w_retry",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(i + 1),
            source_event_id=f"w_r{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await _watcher(session, merchant, {"metric": "retry_count_7d", "op": ">=", "threshold": 2})

    notifier = RecordingNotifier()
    fired = await check_watchers(session, notifier=notifier, now=now_utc())

    assert fired == 1
    url, payload, signature = notifier.pushes[0]
    assert payload["entity_id"] == "w_retry"
    assert payload["observed"]["retry_count_7d"] == 2
    assert signature is not None


async def test_watcher_ignores_entities_below_threshold(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="w_low",
        entity_type="subscription",
        event_type="RetryAttempted",
        occurred_at=days_ago(1),
        source_event_id="w_r0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await _watcher(session, merchant, {"metric": "retry_count_7d", "op": ">=", "threshold": 2})

    fired = await check_watchers(session, notifier=RecordingNotifier(), now=now_utc())
    assert fired == 0


async def test_open_ptp_due_watcher(session: AsyncSession):
    merchant = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="w_ptp",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=days_ago(5),
        source_event_id="w_f",
    )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id="w_ptp",
        entity_type="subscription",
        event_type="PromiseMade",
        occurred_at=days_ago(4),
        payload={"due_at": (now_utc() - timedelta(hours=1)).isoformat()},
    )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await _watcher(session, merchant, {"metric": "open_ptp_due", "op": "<", "threshold": 0})

    notifier = RecordingNotifier()
    fired = await check_watchers(session, notifier=notifier, now=now_utc())
    assert fired == 1


async def test_cooldown_prevents_spam(session: AsyncSession):
    merchant = make_merchant_id()
    for i in range(2):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="w_cool",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(i + 1),
            source_event_id=f"w_c{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await _watcher(
        session, merchant, {"metric": "retry_count_7d", "op": ">=", "threshold": 2}, cooldown=3600
    )

    notifier = RecordingNotifier()
    first = await check_watchers(session, notifier=notifier, now=now_utc())
    second = await check_watchers(
        session,
        notifier=notifier,
        now=now_utc() + timedelta(minutes=30),
    )
    third = await check_watchers(
        session,
        notifier=notifier,
        now=now_utc() + timedelta(hours=2),
    )

    assert (first, second, third) == (1, 0, 1)


async def test_failed_delivery_does_not_consume_cooldown(session: AsyncSession):
    merchant = make_merchant_id()
    for i in range(2):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="w_fail",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(i + 1),
            source_event_id=f"w_f{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()
    await _watcher(session, merchant, {"metric": "retry_count_7d", "op": ">=", "threshold": 2})

    failing = RecordingNotifier(fail=True)
    await check_watchers(session, notifier=failing, now=now_utc())

    healthy = RecordingNotifier()
    fired = await check_watchers(session, notifier=healthy, now=now_utc() + timedelta(seconds=1))
    assert fired == 1


async def test_sign_payload_is_hmac_and_deterministic():
    payload = {"entity_id": "w", "observed": {"retry_count_7d": 2}}
    sig1 = sign_payload(payload, "s3cret")
    sig2 = sign_payload(payload, "s3cret")
    other = sign_payload(payload, "other")
    assert sig1 == sig2
    assert sig1 != other
    assert sign_payload(payload, "") is None  # no secret → unsigned (rejected by receiver)


async def test_seed_default_watchers_is_idempotent(session: AsyncSession):
    merchant = make_merchant_id()
    first = await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    second = await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    assert first == 4  # retry, PTP overdue, stale, pre-expiry
    assert second == 0


def test_httpx_notifier_exists_as_protocol_implementation():
    notifier = HTTPXNotifier(timeout_seconds=1.0)
    assert hasattr(notifier, "send")
