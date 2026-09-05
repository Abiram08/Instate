"""Backup-method routing and attempted-recovery rate."""


from instate.agent.decide import process_failure
from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
from instate.core.models import Event
from instate.core.policy import seed_default_policy
from instate.core.ledger import record_event
from instate.core.projection import fold_events
from instate.replay.compare import RealisticGateway
from instate.replay.metrics import attempted_recovery
from sqlalchemy import select
from tests.conftest import make_merchant_id, now_utc


class BackupProposer:
    model_name = "backup-proposer"

    async def propose(self, context: dict) -> dict | None:
        return {
            "action": "RETRY_BACKUP_METHOD",
            "timing": "IMMEDIATE",
            "rationale": "primary dead, backup on file",
            "confidence": 0.9,
        }


async def _setup(session, merchant_id, entity_id="sub_bkp", code="CARD_EXPIRED"):
    await seed_default_policy(session)
    await seed_default_taxonomy(session)
    await seed_default_diagnosis(session)
    now = now_utc()
    await record_event(
        session, merchant_id=merchant_id, entity_id=entity_id, entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now,
        payload={"amount_minor": 99900, "failure_code": code},
        source_event_id="wh_bkp_1",
    )
    await record_event(
        session, merchant_id=merchant_id, entity_id=entity_id, entity_type="subscription",
        event_type="PaymentMethodChanged", occurred_at=now,
        payload={"via": "backup_on_file"},
        source_event_id="wh_bkp_1_method",
    )
    await session.commit()
    await fold_events(session)
    await session.commit()
    ev = (await session.execute(
        select(Event).where(Event.source_event_id == "wh_bkp_1"))).scalar_one()
    return ev, now


async def test_backup_route_charges_backup_zero_action(session):
    from instate.agent.execute import execute_action
    from instate.core.gate import check_proposal
    from instate.core.models import Decision
    mid = make_merchant_id()
    s = session
    ev, now = await _setup(s, mid)
    decision0 = Decision(merchant_id=mid, entity_id=ev.entity_id, root_cause="card_expired")
    s.add(decision0)
    await s.commit()
    g = await check_proposal(s, merchant_id=mid, entity_id=ev.entity_id,
                       entity_type="subscription", decision_id=decision0.id,
                       proposal={"action": "RETRY_BACKUP_METHOD", "confidence": 0.9},
                       root_cause="card_expired", now=now)
    assert g.allowed, f"backup blocked: {g.chain}"
    gw = RealisticGateway()
    gw.note_failure(ev.entity_id, "card_expired", now)
    gw.method_changed.add(ev.entity_id)  # backup instrument on file
    gw.now = now
    decision = Decision(merchant_id=mid, entity_id=ev.entity_id, root_cause="card_expired")
    s.add(decision)
    await s.commit()
    resp = await execute_action(
        s, gateway=gw, merchant_id=mid, entity_id=ev.entity_id,
        entity_type="subscription", decision=decision,
        action="RETRY_BACKUP_METHOD", now=now)
    assert resp.status == "completed"
    backup_calls = [c for c in gw.calls if c["action"] == "RETRY_BACKUP_METHOD"]
    assert backup_calls, "backup instrument must actually be charged"
    backup_calls = [c for c in gw.calls if c["action"] == "RETRY_BACKUP_METHOD"]
    assert backup_calls, "backup instrument must actually be charged"
    wins = (await s.execute(select(Event).where(
        Event.entity_id == ev.entity_id,
        Event.event_type == "RetrySucceeded"))).scalars().all()
    assert wins and (wins[0].payload or {}).get("via") == "backup"


async def test_backup_blocked_on_fraud(session):
    mid = make_merchant_id()
    s = session
    ev, now = await _setup(s, mid, entity_id="sub_fraud", code="FRAUD_DETECTED")
    gw = RealisticGateway()
    gw.note_failure(ev.entity_id, "fraud_block", now)
    gw.now = now
    await process_failure(s, event=ev, reasoner=BackupProposer(), gateway=gw, now=now)
    esc = (await s.execute(select(Event).where(
        Event.entity_id == ev.entity_id,
        Event.event_type == "EscalatedToHuman"))).scalars().all()
    assert esc, "fraud must escalate, never charge backup"


async def test_attempted_rate_excludes_never_attempted(session):
    mid = make_merchant_id()
    s = session
    now = now_utc()
    for eid in ("a1", "a2", "a3"):
        await record_event(
            s, merchant_id=mid, entity_id=eid, entity_type="subscription",
            event_type="PaymentFailed", occurred_at=now,
            payload={"amount_minor": 100}, source_event_id=f"wh_{eid}")
    await record_event(
        s, merchant_id=mid, entity_id="a1", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=now,
        payload={"success": True}, source_event_id="wh_a1_r")
    await record_event(
        s, merchant_id=mid, entity_id="a1", entity_type="subscription",
        event_type="RetrySucceeded", occurred_at=now,
        payload={"amount_minor": 100}, source_event_id="wh_a1_s")
    await record_event(
        s, merchant_id=mid, entity_id="a2", entity_type="subscription",
        event_type="RetryAttempted", occurred_at=now,
        payload={"success": False}, source_event_id="wh_a2_r")
    await s.commit()
    attempted, recovered = await attempted_recovery(s, merchant_id=mid)
    assert (attempted, recovered) == (2, 1)
