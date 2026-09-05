"""Cold archive with chain anchor and gate-window clamp."""

import json

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.archive import archive_old_events
from instate.core.gate import evaluate
from instate.core.ledger import record_event, verify_chain
from instate.core.models import ArchiveAnchor, Event
from instate.core.policy import seed_default_policy
from instate.verify.standalone import verify_exported_rows
from tests.conftest import days_ago, make_merchant_id, now_utc


async def _age_recorded_at(session: AsyncSession, merchant, entity_id: str, days: float):
    """Backdate recorded_at (excluded from chain hash)."""
    await session.execute(
        update(Event)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .values(recorded_at=days_ago(days))
    )
    await session.commit()


async def _seed_old_chain(session, merchant, entity_id: str, *, n_old: int = 3):
    """Seed old history plus one fresh event."""
    for i in range(n_old):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(100 - i),
            source_event_id=f"{entity_id}_old_{i}",
        )
    await record_event(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id=f"{entity_id}_new",
    )
    await session.commit()
    await session.execute(
        update(Event)
        .where(
            Event.merchant_id == merchant,
            Event.entity_id == entity_id,
            Event.event_type == "RetryAttempted",
        )
        .values(recorded_at=days_ago(100))
    )
    await session.commit()


async def test_archive_keeps_chain_verifiable_via_anchor(session: AsyncSession, tmp_path):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    await _seed_old_chain(session, merchant, "sub_arc")
    await session.commit()

    report = await archive_old_events(
        session, retention_days=90, export_dir=tmp_path, now=now_utc()
    )

    assert report["archived"] == 3
    assert report["anchors"] == 1
    assert report["skipped_in_window"] == 0
    assert len(report["files"]) == 1

    result = await verify_chain(session, merchant, "sub_arc")
    assert result.verified, f"anchored chain must verify, got: {result.error}"
    assert result.archived_prefix is True
    assert result.event_count == 1  # only the hot row remains


async def test_archive_clamp_protects_live_gate_window(session: AsyncSession, tmp_path):
    """Pins clamp: occurred_at in 7d window stays hot despite old recorded_at."""
    merchant = make_merchant_id()
    await seed_default_policy(session)  # 7d ceiling is the widest window

    for i, back in enumerate([3.0, 2.0, 1.0]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_live",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(back),
            source_event_id=f"sub_live_r{i}",
        )
    await session.commit()
    await _age_recorded_at(session, merchant, "sub_live", days=100)

    report = await archive_old_events(
        session, retention_days=90, export_dir=tmp_path, now=now_utc()
    )

    assert report["archived"] == 0
    assert report["skipped_in_window"] == 3

    verdict = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_live",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
        record=False,
    )
    assert verdict.verdict == "DENY"


async def test_tampered_anchor_fails_verify(session: AsyncSession, tmp_path):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    await _seed_old_chain(session, merchant, "sub_tamper")
    await session.commit()
    await archive_old_events(session, retention_days=90, export_dir=tmp_path, now=now_utc())

    await session.execute(
        update(ArchiveAnchor)
        .where(ArchiveAnchor.merchant_id == merchant, ArchiveAnchor.entity_id == "sub_tamper")
        .values(anchor_hash=b"\x00" * 32)
    )
    await session.commit()

    result = await verify_chain(session, merchant, "sub_tamper")
    assert result.verified is False
    assert "prev_hash mismatch" in result.error


async def test_cut_without_anchor_fails_honestly(session: AsyncSession):
    from sqlalchemy import delete

    merchant = make_merchant_id()
    await seed_default_policy(session)
    await _seed_old_chain(session, merchant, "sub_cut")
    await session.commit()

    old_ids = await session.execute(
        select(Event.id)
        .where(Event.merchant_id == merchant, Event.entity_id == "sub_cut")
        .order_by(Event.id.asc())
        .limit(3)
    )
    await session.execute(delete(Event).where(Event.id.in_([r[0] for r in old_ids.all()])))
    await session.commit()

    result = await verify_chain(session, merchant, "sub_cut")
    assert result.verified is False
    assert "prev_hash mismatch" in result.error


async def test_standalone_verifier_accepts_cold_plus_hot(session: AsyncSession, tmp_path):
    merchant = make_merchant_id()
    await seed_default_policy(session)
    await _seed_old_chain(session, merchant, "sub_audit")
    await session.commit()
    report = await archive_old_events(
        session, retention_days=90, export_dir=tmp_path, now=now_utc()
    )
    assert len(report["files"]) == 1

    cold_rows = []
    with open(report["files"][0]) as f:
        for line in f:
            cold_rows.append(json.loads(line))

    hot = await session.execute(
        select(Event).where(Event.merchant_id == merchant, Event.entity_id == "sub_audit")
    )
    hot_rows = [
        {
            "id": e.id,
            "merchant_id": str(e.merchant_id),
            "entity_id": e.entity_id,
            "event_type": e.event_type,
            "occurred_at": e.occurred_at.isoformat(),
            "payload_hash": e.payload_hash.hex(),
            "prev_hash": e.prev_hash.hex() if e.prev_hash else None,
            "hash": e.hash.hex(),
        }
        for e in hot.scalars()
    ]
    anchors = await session.execute(
        select(ArchiveAnchor).where(
            ArchiveAnchor.merchant_id == merchant, ArchiveAnchor.entity_id == "sub_audit"
        )
    )
    anchor_dicts = [
        {
            "entity_id": a.entity_id,
            "anchor_hash": a.anchor_hash.hex(),
            "archived_through_event_id": a.archived_through_event_id,
        }
        for a in anchors.scalars()
    ]

    report = verify_exported_rows(cold_rows + hot_rows, anchors=anchor_dicts)
    assert report["verified"], f"auditor path must verify, got: {report['breaks']}"
    assert report["events"] == 4

    hot_only = verify_exported_rows(hot_rows)
    assert hot_only["verified"] is False

    anchored = verify_exported_rows(hot_rows, anchors=anchor_dicts)
    assert anchored["verified"], f"anchored hot rows must verify, got: {anchored['breaks']}"
