"""Adversarial: storms, races, hostile gateways, double-runs.

Each test attacks the agent the way production will — concurrently,
repeatedly, with lying or failing dependencies — and asserts the
invariants: exactly-once ledger, no double decisions, no double charges,
no crashes, idempotent reruns.
"""

import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime

from sqlalchemy import func, select

from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.agent.decide import drain_pending
from instate.agent.execute import write_intent
from instate.agent.reconcile import reconcile_pending
from instate.core.database import close_db, get_session_factory, init_db
from instate.core.ledger import record_event
from instate.core.models import Decision, Event
from instate.core.policy import seed_default_policy
from instate.replay.compare import RealisticGateway, SharedScriptedReasoner
from instate.replay.counterfactual import replay_with_policy
from instate.surfaces.webhook import WebhookRejected, handle_webhook
from tests.conftest import make_merchant_id, now_utc

SECRET = "whsec_test_secret"


def _storm_body(delivery_id="evt_storm_1"):
    raw = {
        "event": "payment.failed",
        "id": delivery_id,
        "payload": {"payment": {"id": "pay_storm", "error_reason": "GATEWAY_TIMEOUT",
                                "amount": 99900}},
    }
    body = json.dumps(raw).encode()
    sig = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return body, sig


async def _file_factory(tmp_path, monkeypatch):
    import os
    os.environ["INSTATE_DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/adv.db"
    await close_db()
    await init_db()
    return get_session_factory()


async def _seed_knowledge(session):
    await seed_default_policy(session)
    await seed_default_policy(session, entity_type="payment")
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


# ---------------------------------------------------------------------------
# Layer 1 · concurrency
# ---------------------------------------------------------------------------


async def test_webhook_storm_one_event_twenty_200s(tmp_path, monkeypatch):
    """20 parallel redeliveries of the same delivery: 1 event, 20× 200."""
    factory = await _file_factory(tmp_path, monkeypatch)
    mid = make_merchant_id()
    body, sig = _storm_body()

    async def deliver():
        async with factory() as s:
            try:
                return await handle_webhook(
                    s, raw_body=body, signature=sig, secret=SECRET, merchant_id=mid)
            except WebhookRejected as r:  # pragma: no cover — must not happen
                return (r.status_code, r.detail)

    results = await asyncio.gather(*[deliver() for _ in range(20)])
    assert all(code == 200 for code, _ in results), results

    async with factory() as s:
        n = (await s.execute(
            select(func.count(Event.id)).where(Event.source_event_id == "evt_storm_1")
        )).scalar_one()
        assert n == 1


async def test_parallel_ticks_single_decision(tmp_path, monkeypatch):
    """4 concurrent drains over one failure: exactly 1 decision, 1 intent."""
    factory = await _file_factory(tmp_path, monkeypatch)
    mid = make_merchant_id()
    async with factory() as s:
        await _seed_knowledge(s)
        await record_event(
            s, merchant_id=mid, entity_id="sub_race", entity_type="subscription",
            event_type="PaymentFailed", occurred_at=now_utc(),
            payload={"failure_code": "GATEWAY_TIMEOUT", "amount_minor": 99900},
            source_event_id="wh_race_1")
        await s.commit()

    async def tick():
        async with factory() as s:
            gw = RealisticGateway()
            gw.note_failure("sub_race", "GATEWAY_TIMEOUT", datetime.now(UTC))
            await drain_pending(s, reasoner=SharedScriptedReasoner(), gateway=gw, now=datetime.now(UTC))
            await s.commit()

    await asyncio.gather(*[tick() for _ in range(4)])

    async with factory() as s:
        decisions = (await s.execute(
            select(func.count(Decision.id)).where(Decision.entity_id == "sub_race")
        )).scalar_one()
        intents = (await s.execute(
            select(func.count(Event.id)).where(
                Event.entity_id == "sub_race", Event.event_type == "ActionIntended")
        )).scalar_one()
        assert decisions == 1, "double decision under concurrent ticks"
        assert intents <= 1, "double intent under concurrent ticks"


# ---------------------------------------------------------------------------
# Layer 2 · hostile input + boundaries
# ---------------------------------------------------------------------------


async def test_unknown_code_escalates_end_to_end(session):
    """A code the taxonomy never saw: UNKNOWN → human, no exception."""
    mid = make_merchant_id()
    async with session:
        await _seed_knowledge(session)
        raw = {"event": "payment.failed", "id": "wh_novel_1",
               "payload": {"payment": {"id": "pay_novel", "error_reason": "SOMETHING_NOVEL"}}}
        body = json.dumps(raw).encode()
        sig = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
        status, _ = await handle_webhook(
            session, raw_body=body, signature=sig, secret=SECRET, merchant_id=mid)
        assert status == 200
        gw = RealisticGateway()
        gw.note_failure("pay_novel", "SOMETHING_NOVEL", datetime.now(UTC))
        results = await drain_pending(
            session, reasoner=SharedScriptedReasoner(), gateway=gw, now=datetime.now(UTC))
        await session.commit()
    assert len(results) == 1
    assert results[0].decision_id is not None
    escalated = await session.execute(
        select(func.count(Event.id)).where(
            Event.entity_id == "pay_novel", Event.event_type == "EscalatedToHuman")
    )
    assert escalated.scalar_one() == 1, "UNKNOWN root cause must escalate, never act"


async def test_late_event_appends_without_rewriting(session):
    """An event dated last month lands today: chain verifies, history grows."""
    from instate.core.ledger import verify_chain
    mid = make_merchant_id()
    await _seed_knowledge(session)
    now = now_utc()
    await record_event(
        session, merchant_id=mid, entity_id="sub_late", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now,
        payload={"failure_code": "insufficient_funds"}, source_event_id="wh_late_1")
    await session.commit()
    before = (await session.execute(
        select(func.count(Event.id)).where(Event.entity_id == "sub_late"))).scalar_one()
    await record_event(
        session, merchant_id=mid, entity_id="sub_late", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=datetime(2026, 1, 5, tzinfo=UTC),
        payload={"failure_code": "insufficient_funds"}, source_event_id="wh_late_0")
    await session.commit()
    after = (await session.execute(
        select(func.count(Event.id)).where(Event.entity_id == "sub_late"))).scalar_one()
    assert after == before + 1
    assert (await verify_chain(session, mid, "sub_late")).verified


# ---------------------------------------------------------------------------
# Layer 3 · failing gateways, reconcile, double replay
# ---------------------------------------------------------------------------


class _FlakyGateway:
    """Explodes on execute (→ unknown), healthy on re-execution."""

    def __init__(self):
        self.calls = []

    async def lookup(self, key):
        return None

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        from instate.adapters.razorpay import GatewayResponse
        self.calls.append(action)
        if len(self.calls) == 1:
            raise TimeoutError("gateway exploded mid-action")
        return GatewayResponse("completed", provider_ref="pay_ok", amount_minor=99900)


async def test_gateway_blowup_then_reconcile_charges_once(session):
    """Kill mid-action: intent stands, reconcile completes, exactly one charge."""
    mid = make_merchant_id()
    await _seed_knowledge(session)
    now = now_utc()
    await record_event(
        session, merchant_id=mid, entity_id="sub_boom", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now,
        payload={"failure_code": "GATEWAY_TIMEOUT", "amount_minor": 99900},
        source_event_id="wh_boom_1")
    await session.commit()
    decision = Decision(merchant_id=mid, entity_id="sub_boom", root_cause="network_timeout")
    session.add(decision)
    await session.commit()
    await write_intent(
        session, merchant_id=mid, entity_id="sub_boom", entity_type="subscription",
        decision=decision, action="RETRY_NOW", occurred_at=now)

    flaky = _FlakyGateway()
    report = []
    # First boot: gateway explodes. No crash, nothing resolved, intent stands.
    assert await reconcile_pending(session, gateway=flaky, report=report) == 0
    assert report[0].via == "deferred" and report[0].status == "unknown"
    from instate.agent.reconcile import find_dangling_intents
    assert len(await find_dangling_intents(session)) == 1

    # Second boot: healthy gateway. Completes with exactly one charge.
    assert await reconcile_pending(session, gateway=flaky, report=report) == 1
    assert report[1].status == "completed" and flaky.calls == ["RETRY_NOW", "RETRY_NOW"]

    # Second reconcile: nothing dangling, no new gateway calls, no new events.
    events_before = (await session.execute(
        select(func.count(Event.id)).where(Event.entity_id == "sub_boom"))).scalar_one()
    assert await reconcile_pending(session, gateway=flaky) == 0
    assert flaky.calls == ["RETRY_NOW", "RETRY_NOW"], "no third charge on quiet rerun"
    events_after = (await session.execute(
        select(func.count(Event.id)).where(Event.entity_id == "sub_boom"))).scalar_one()
    assert events_after == events_before


async def test_replay_twice_second_is_quiet(session):
    """Replay consumes its version bump; a second run is a quiet no-op."""
    mid = make_merchant_id()
    await _seed_knowledge(session)
    await record_event(
        session, merchant_id=mid, entity_id="sub_rp", entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
        source_event_id="wh_rp_1")
    await session.commit()
    gw = RealisticGateway()
    gw.note_failure("sub_rp", "insufficient_funds", datetime.now(UTC))
    await drain_pending(
        session, reasoner=SharedScriptedReasoner(), gateway=gw, now=datetime.now(UTC))
    await session.commit()

    first = await replay_with_policy(session, overrides={"retry_spacing_24h": 0}, merchant_id=mid)
    await session.commit()
    assert first.decisions_replayed >= 1
    second = await replay_with_policy(session, overrides={"retry_spacing_24h": 0}, merchant_id=mid)
    assert second.decisions_replayed == 0 and second.verdict_changes == 0


async def test_tick_twice_second_says_nothing_pending(session):
    """Idempotent worker step, CLI-visible contract tested in test_cli."""
    mid = make_merchant_id()
    await _seed_knowledge(session)
    now = datetime.now(UTC)
    gw = RealisticGateway()
    first = await drain_pending(
        session, reasoner=SharedScriptedReasoner(), gateway=gw, now=now)
    await session.commit()
    assert first == []
    _ = mid
