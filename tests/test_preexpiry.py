"""Pre-expiry card watcher within 30-day window."""

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.projection import fold_events
from instate.core.watchers import check_watchers, seed_default_watchers
from tests.conftest import make_merchant_id


class RecordingNotifier:
    def __init__(self):
        self.sent = []

    async def send(self, url, payload, signature):
        self.sent.append((url, payload))
        return True


async def _card_expiring(session, merchant, entity_id, year, month, source):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="CardExpiring",
        occurred_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        payload={"exp_year": year, "exp_month": month},
        source_event_id=source,
    )
    await session.commit()
    await fold_events(session)
    await session.commit()


async def test_pre_expiry_watcher_fires_within_window(session: AsyncSession):
    merchant = make_merchant_id()
    # 21 days out, inside 30-day window.
    await _card_expiring(session, merchant, "sub_pre", 2026, 8, "sub_pre_0")
    await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    await session.commit()

    notifier = RecordingNotifier()
    fired = await check_watchers(
        session, notifier=notifier, now=datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    )
    assert fired >= 1
    assert any(p["entity_id"] == "sub_pre" for _, p in notifier.sent)


async def test_pre_expiry_watcher_silent_far_out(session: AsyncSession):
    merchant = make_merchant_id()
    await _card_expiring(session, merchant, "sub_far", 2027, 6, "sub_far_0")
    await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    await session.commit()

    notifier = RecordingNotifier()
    fired = await check_watchers(
        session, notifier=notifier, now=datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    )
    assert fired == 0
    assert notifier.sent == []


async def test_expired_card_is_not_a_prevention_case(session: AsyncSession):
    merchant = make_merchant_id()
    await _card_expiring(session, merchant, "sub_old", 2026, 6, "sub_old_0")
    await seed_default_watchers(
        session, merchant_id=merchant, target_url="https://agent.example/hook"
    )
    await session.commit()

    notifier = RecordingNotifier()
    fired = await check_watchers(
        session, notifier=notifier, now=datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    )
    assert fired == 0
