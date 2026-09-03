"""Cold archive — chain-walkable, window-safe (§15).

Two invariants under test:
1. The clamp: rows whose occurred_at is still inside a live gate window
   NEVER leave the hot table, no matter how old their recorded_at is —
   or Gate-1 would wrongly ALLOW (a compliance breach by housekeeping).
2. The anchor: after a legitimate cut, cold rows + anchor + hot rows
   verify as one chain — in-DB and in the standalone verifier. A cut
   with no (or a tampered) anchor fails honestly.
"""

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
    """Test-only surgery: pretend these rows were LEARNED long ago.

    recorded_at is NOT part of the chain hash, so backdating it cannot
    break verification — it only changes archive eligibility.
    """
    await session.execute(
        update(Event)
        .where(Event.merchant_id == merchant, Event.entity_id == entity_id)
        .values(recorded_at=days_ago(days))
    )
    await session.commit()


async def _seed_old_chain(session, merchant, entity_id: str, *, n_old: int = 3):
    """Entity with old history + one fresh event.

    Both occurred_at AND recorded_at are backdated for the old rows —
    the archive cut keys on recorded_at, the clamp on occurred_at.
    """
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
    # Age the OLD rows' recorded_at (test-only surgery — recorded_at is
    # not part of the chain hash, so this cannot break verification).
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

    # Hot chain starts mid-history — the anchor vouches for the cut link
    result = await verify_chain(session, merchant, "sub_arc")
    assert result.verified, f"anchored chain must verify, got: {result.error}"
    assert result.archived_prefix is True
    assert result.event_count == 1  # only the hot row remains


async def test_archive_clamp_protects_live_gate_window(session: AsyncSession, tmp_path):
    """Rows LEARNED 100 days ago but OCCURRING 3/2/1 days ago stay hot:
    the 7d retry window still needs them. The gate must still DENY."""
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
    assert report["skipped_in_window"] == 3  # the clamp is visible, not silent

    # Functional proof: the gate still sees the full window → DENY
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
    """An anchor that doesn't vouch for the exact cut link is worthless —
    verification fails honestly instead of trusting it."""
    merchant = make_merchant_id()
    await seed_default_policy(session)
    await _seed_old_chain(session, merchant, "sub_tamper")
    await session.commit()
    await archive_old_events(session, retention_days=90, export_dir=tmp_path, now=now_utc())

    # Tamper with the anchor (test-only surgery on the trust root)
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
    """Rows deleted with no anchor (legacy data, manual surgery) are a
    break — reported as a mismatch, never silently accepted."""
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
    """The bank's auditor path: cold export file + hot rows + anchors
    verify with zero dependency on our service."""
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

    # Hot rows alone, no anchor: the cut is a break (the auditor cannot
    # tell archival from tampering without the anchor — by design)
    hot_only = verify_exported_rows(hot_rows)
    assert hot_only["verified"] is False

    # Hot rows alone WITH the anchor: verifies (the auditor's normal case —
    # they don't need the whole cold file, just the anchor)
    anchored = verify_exported_rows(hot_rows, anchors=anchor_dicts)
    assert anchored["verified"], f"anchored hot rows must verify, got: {anchored['breaks']}"
