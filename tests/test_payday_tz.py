"""Payday inference, bank-holiday shift, and local-morning clamp.
Sparse history returns None; callers fall back to T+48H."""

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from instate.agent.execute import (
    apply_bank_holiday_shift,
    clamp_to_local_morning,
    schedule_retry,
)
from instate.core.ledger import record_event
from instate.core.models import Decision, EntityState
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events, infer_payday
from tests.conftest import make_merchant_id


async def _success(session, merchant, entity_id, at, source):
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="RetrySucceeded",
        occurred_at=at,
        payload={"amount_minor": 49900},
        source_event_id=source,
    )


async def test_infer_payday_finds_day_of_month_pattern(session: AsyncSession):
    merchant = make_merchant_id()
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    for i, back in enumerate([70, 40, 9]):  # dom 1, 1, 1
        await _success(session, merchant, "sub_pay", base - timedelta(days=back), f"p{i}")
    await session.commit()

    payday = await infer_payday(session, merchant_id=merchant, entity_id="sub_pay", now=base)
    assert payday is not None
    assert (payday.day, payday.hour) == (1, 10)
    assert payday > base  # next occurrence (Sep 1, since Aug 1 passed)


async def test_infer_payday_today_morning_counts(session: AsyncSession):
    merchant = make_merchant_id()
    base = datetime(2026, 8, 5, 8, 0, tzinfo=UTC)  # before 10:00 on the 5th
    for i, back in enumerate([61, 31]):  # Jun 5, Jul 5 — exact monthly pattern
        await _success(session, merchant, "sub_pay2", base - timedelta(days=back), f"q{i}")
    await session.commit()

    payday = await infer_payday(session, merchant_id=merchant, entity_id="sub_pay2", now=base)
    assert payday is not None
    assert (payday.day, payday.hour) == (5, 10)
    assert payday.date() == base.date()  # today still counts


async def test_infer_payday_sparse_history_returns_none(session: AsyncSession):
    merchant = make_merchant_id()
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    await _success(session, merchant, "sub_sparse", base - timedelta(days=40), "s0")
    await session.commit()

    assert (
        await infer_payday(session, merchant_id=merchant, entity_id="sub_sparse", now=base) is None
    )


async def test_infer_payday_scattered_days_returns_none(session: AsyncSession):
    merchant = make_merchant_id()
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    for i, back in enumerate([70, 50, 30]):  # dom 1, 21, 11 — no pattern
        await _success(session, merchant, "sub_scat", base - timedelta(days=back), f"t{i}")
    await session.commit()

    assert await infer_payday(session, merchant_id=merchant, entity_id="sub_scat", now=base) is None


async def test_schedule_next_payday_uses_learned_day(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    for i, back in enumerate([70, 40]):
        await _success(session, merchant, "sub_sched", base - timedelta(days=back), f"u{i}")
    await session.commit()
    decision = Decision(merchant_id=merchant, entity_id="sub_sched", root_cause="x")
    session.add(decision)
    await session.flush()

    row = await schedule_retry(
        session,
        merchant_id=merchant,
        entity_id="sub_sched",
        entity_type="subscription",
        decision=decision,
        timing="NEXT_PAYDAY",
        root_cause="insufficient_funds",
        now=base,
    )
    await session.commit()

    assert (row.due_at.day, row.due_at.hour) == (1, 10)
    assert row.due_at > base


async def test_schedule_next_payday_falls_back_without_pattern(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    base = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    decision = Decision(merchant_id=merchant, entity_id="sub_fb", root_cause="x")
    session.add(decision)
    await session.flush()

    row = await schedule_retry(
        session,
        merchant_id=merchant,
        entity_id="sub_fb",
        entity_type="subscription",
        decision=decision,
        timing="NEXT_PAYDAY",
        root_cause="insufficient_funds",
        now=base,
    )
    await session.commit()

    assert row.due_at == base + timedelta(hours=48)


# ---------------------------------------------------------------------------
# Bank holidays
# ---------------------------------------------------------------------------


def test_holiday_shifts_to_t_minus_1():
    due = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)  # Independence Day
    shifted = apply_bank_holiday_shift(due)
    assert shifted.date().isoformat() == "2026-08-14"


def test_double_holiday_shifts_to_t_minus_3():
    # Synthetic: pretend the 14th is also a holiday via monkeypatched set
    import instate.agent.execute as execute_mod

    original = execute_mod.IN_BANK_HOLIDAYS
    execute_mod.IN_BANK_HOLIDAYS = frozenset({"2026-08-15", "2026-08-14"})
    try:
        shifted = apply_bank_holiday_shift(datetime(2026, 8, 15, 12, 0, tzinfo=UTC))
        assert shifted.date().isoformat() == "2026-08-12"
    finally:
        execute_mod.IN_BANK_HOLIDAYS = original


def test_ordinary_day_passes_through():
    due = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    assert apply_bank_holiday_shift(due) == due


# ---------------------------------------------------------------------------
# Local morning
# ---------------------------------------------------------------------------


def test_clamp_moves_night_to_morning():
    # 02:30 IST → 10:00 IST same day
    due = datetime(2026, 8, 12, 2, 30, tzinfo=UTC).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Kolkata")
    )
    due_utc = due.astimezone(UTC)
    clamped = clamp_to_local_morning(due_utc, "Asia/Kolkata")
    from zoneinfo import ZoneInfo

    local = clamped.astimezone(ZoneInfo("Asia/Kolkata"))
    assert (local.hour, local.minute) == (10, 0)
    assert local.date().isoformat() == "2026-08-12"


def test_clamp_leaves_business_hours_alone():
    due = datetime(2026, 8, 12, 6, 0, tzinfo=UTC)  # 11:30 IST
    assert clamp_to_local_morning(due, "Asia/Kolkata") == due


def test_clamp_ignores_bad_timezone():
    due = datetime(2026, 8, 12, 2, 30, tzinfo=UTC)
    assert clamp_to_local_morning(due, "Mars/Olympus") == due
    assert clamp_to_local_morning(due, None) == due


# ---------------------------------------------------------------------------
# Fold
# ---------------------------------------------------------------------------


async def test_fold_stores_valid_timezone(session: AsyncSession):
    await record_event(
        session,
        merchant_id=make_merchant_id(),
        entity_id="sub_tz",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        payload={"failure_code": "x", "customer_tz": "Asia/Kolkata"},
        source_event_id="sub_tz_0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (await _merchant_of(session, "sub_tz"), "sub_tz"))
    assert state.timezone == "Asia/Kolkata"


async def test_fold_ignores_invalid_timezone(session: AsyncSession):
    await record_event(
        session,
        merchant_id=make_merchant_id(),
        entity_id="sub_tz2",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        payload={"failure_code": "x", "customer_tz": "Mars/Olympus"},
        source_event_id="sub_tz2_0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (await _merchant_of(session, "sub_tz2"), "sub_tz2"))
    assert state.timezone is None


async def test_fold_stores_card_expiry(session: AsyncSession):
    await record_event(
        session,
        merchant_id=make_merchant_id(),
        entity_id="sub_card",
        entity_type="subscription",
        event_type="CardExpiring",
        occurred_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        payload={"exp_year": 2026, "exp_month": 9},
        source_event_id="sub_card_0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (await _merchant_of(session, "sub_card"), "sub_card"))
    assert (state.card_exp_year, state.card_exp_month) == (2026, 9)


async def test_fold_rejects_partial_expiry(session: AsyncSession):
    await record_event(
        session,
        merchant_id=make_merchant_id(),
        entity_id="sub_card2",
        entity_type="subscription",
        event_type="CardExpiring",
        occurred_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
        payload={"exp_year": 2026, "exp_month": 13},
        source_event_id="sub_card2_0",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()

    state = await session.get(EntityState, (await _merchant_of(session, "sub_card2"), "sub_card2"))
    assert state.card_exp_year is None
    assert state.card_exp_month is None


async def _merchant_of(session, entity_id: str):
    from sqlalchemy import select

    from instate.core.models import Event

    result = await session.execute(
        select(Event.merchant_id).where(Event.entity_id == entity_id).limit(1)
    )
    return result.scalar_one()
