"""L1 snapshots: watermarked checkpoints for incremental rebuild (§15)."""


from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import EntityState, Event, L1Snapshot


async def create_snapshot(
    session: AsyncSession,
    merchant_id,
    entity_id: str,
) -> L1Snapshot | None:
    state = await session.get(EntityState, (merchant_id, entity_id))
    if state is None:
        return None
    snap = L1Snapshot(
        merchant_id=merchant_id,
        entity_id=entity_id,
        snapshot_at_event_id=state.last_event_id,
        state_json={
            "status": state.status,
            "last_contact_at": state.last_contact_at.isoformat() if state.last_contact_at else None,
            "last_failure_reason": state.last_failure_reason,
            "open_ptp_due_at": state.open_ptp_due_at.isoformat() if state.open_ptp_due_at else None,
            "amount_at_risk_minor": state.amount_at_risk_minor,
            "last_event_id": state.last_event_id,
            "entity_type": state.entity_type,
        },
    )
    session.add(snap)
    await session.flush()
    return snap


async def rebuild_incremental(session: AsyncSession) -> dict:
    """Rebuild using the newest snapshot per entity as starting point."""
    from instate.core.projection import apply_event

    snaps = await session.execute(select(L1Snapshot))
    latest: dict[tuple, L1Snapshot] = {}
    for s in snaps.scalars():
        key = (s.merchant_id, s.entity_id)
        if key not in latest or s.snapshot_at_event_id > latest[key].snapshot_at_event_id:
            latest[key] = s

    if not latest:
        from instate.core.projection import rebuild

        return await rebuild(session)

    await session.execute(delete(EntityState))
    await session.flush()
    for snap in latest.values():
        j = snap.state_json
        from datetime import datetime as dt

        def _parse(v):
            return dt.fromisoformat(v) if v else None

        session.add(
            EntityState(
                merchant_id=snap.merchant_id,
                entity_id=snap.entity_id,
                entity_type=j.get("entity_type", "subscription"),
                status=j["status"],
                last_contact_at=_parse(j.get("last_contact_at")),
                last_failure_reason=j.get("last_failure_reason"),
                open_ptp_due_at=_parse(j.get("open_ptp_due_at")),
                amount_at_risk_minor=j.get("amount_at_risk_minor"),
                last_event_id=j["last_event_id"],
            )
        )
    await session.flush()

    folded = 0
    for snap in latest.values():
        evts = await session.execute(
            select(Event)
            .where(
                Event.merchant_id == snap.merchant_id,
                Event.entity_id == snap.entity_id,
                Event.id > snap.snapshot_at_event_id,
            )
            .order_by(Event.id.asc())
        )
        for evt in evts.scalars():
            state = await session.get(EntityState, (snap.merchant_id, snap.entity_id))
            apply_event(state, evt)
            folded += 1

    # entities without snapshot: full fold
    all_entities = await session.execute(
        select(Event.merchant_id, Event.entity_id).group_by(Event.merchant_id, Event.entity_id)
    )
    for mid, eid in all_entities.all():
        if (mid, eid) in latest:
            continue
        state = await session.get(EntityState, (mid, eid))
        if state is not None:
            continue
        first = await session.execute(
            select(Event)
            .where(Event.merchant_id == mid, Event.entity_id == eid)
            .order_by(Event.id.asc())
            .limit(1)
        )
        f = first.scalar_one_or_none()
        state = EntityState(
            merchant_id=mid, entity_id=eid, entity_type=f.entity_type if f else "payment",
            status="ACTIVE", last_event_id=0,
        )
        session.add(state)
        await session.flush()
        evts = await session.execute(
            select(Event).where(Event.merchant_id == mid, Event.entity_id == eid).order_by(Event.id.asc())
        )
        for evt in evts.scalars():
            apply_event(state, evt)
            folded += 1
    await session.flush()
    return {"events_folded": folded, "entities": len(latest), "incremental": True}
