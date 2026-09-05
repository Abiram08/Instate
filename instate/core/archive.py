"""Cold archive: chain-walkable export with per-entity anchors (§15)."""

import json
from datetime import datetime, timedelta, UTC
from pathlib import Path

from sqlalchemy import func, select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import ArchiveAnchor, Event, Policy


async def _max_policy_window_seconds(session: AsyncSession) -> int:
    """The widest live gate window. Gate counters key on occurred_at, so
    anything with occurred_at inside this window must stay hot."""
    result = await session.execute(select(func.max(Policy.window_seconds)))
    return result.scalar_one_or_none() or 0


async def archive_old_events(
    session: AsyncSession,
    *,
    retention_days: int = 90,
    export_dir: Path | None = None,
    now: datetime | None = None,
) -> dict:
    """Move events older than retention to export files; keep anchors.

    Rows leave only when recorded_at is past retention AND occurred_at is
    outside every live gate window; held-back rows report as skipped_in_window.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=retention_days)
    live_floor = now - timedelta(seconds=await _max_policy_window_seconds(session))

    q = (
        select(Event)
        .where(Event.recorded_at < cutoff, Event.occurred_at < live_floor)
        .order_by(Event.id.asc())
    )
    old = list((await session.execute(q)).scalars().all())

    held_back = await session.execute(
        select(func.count(Event.id)).where(
            Event.recorded_at < cutoff, Event.occurred_at >= live_floor
        )
    )

    if not old:
        return {
            "archived": 0,
            "anchors": 0,
            "files": [],
            "skipped_in_window": held_back.scalar_one(),
        }

    export_dir = export_dir or Path("./cold_archive")
    export_dir.mkdir(parents=True, exist_ok=True)

    by_entity: dict[tuple, list[Event]] = {}
    for e in old:
        by_entity.setdefault((e.merchant_id, e.entity_id), []).append(e)

    files = []
    anchors = 0
    for (mid, eid), evts in by_entity.items():
        last = evts[-1]
        session.add(
            ArchiveAnchor(
                merchant_id=mid,
                entity_id=eid,
                anchor_event_id=last.id,
                anchor_hash=last.hash,
                archived_through_event_id=last.id,
            )
        )
        anchors += 1
        p = export_dir / f"{mid}_{eid}_{cutoff.date()}.jsonl"
        with p.open("a") as f:
            for e in evts:
                f.write(
                    json.dumps(
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
                    )
                    + "\n"
                )
        files.append(str(p))

    # Delete with the same predicate as the select above.
    await session.execute(
        delete(Event).where(Event.recorded_at < cutoff, Event.occurred_at < live_floor)
    )
    await session.commit()
    return {
        "archived": len(old),
        "anchors": anchors,
        "files": files,
        "skipped_in_window": held_back.scalar_one(),
    }
