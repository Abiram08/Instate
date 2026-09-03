"""Standalone verifier — audit without our service (§15).

An external auditor exports raw rows (SELECT * FROM events WHERE
merchant_id = $1 ORDER BY id) and re-derives every chain with zero
dependency on our code. That's what 'tamper-evident' means to a bank.
"""

import hashlib
from datetime import datetime, UTC


def verify_exported_rows(
    rows: list[dict],
    anchors: list[dict] | None = None,
) -> dict:
    """rows: exported dicts with id, merchant_id, entity_id, event_type,
    occurred_at (ISO), payload_hash (hex), prev_hash (hex|None), hash (hex).

    anchors: exported ArchiveAnchor dicts with entity_id, anchor_hash
    (hex), archived_through_event_id — the cold prefix each hot chain
    continues from. A hot chain whose first link is vouched by an anchor
    verifies; a cut with no (or a wrong) anchor is a break.

    Returns {verified: bool, events: int, breaks: list}.
    """
    by_entity: dict[str, list[dict]] = {}
    for r in sorted(rows, key=lambda x: x["id"]):
        by_entity.setdefault(r["entity_id"], []).append(r)
    anchor_by_entity: dict[str, dict] = {}
    for a in anchors or []:
        anchor_by_entity[a["entity_id"]] = a

    breaks = []
    total = 0
    for entity_id, evts in by_entity.items():
        prev = None
        for e in evts:
            total += 1
            exp_prev = prev
            got_prev = e.get("prev_hash")
            if (exp_prev or "") != (got_prev or ""):
                anchor = anchor_by_entity.get(entity_id)
                vouched = (
                    anchor is not None
                    and exp_prev is None  # first hot event only
                    and anchor.get("anchor_hash") == got_prev
                    and anchor.get("archived_through_event_id", 0) < e["id"]
                )
                if not vouched:
                    breaks.append(f"{entity_id}@{e['id']}: prev_hash mismatch")
                    prev = e["hash"]
                    continue
                # Vouched: the recompute below must use the REAL prev_hash
                # (the archived link), not the empty genesis seed.
                exp_prev = got_prev
            # recompute payload_hash check is implicit via hash
            h = hashlib.sha256()
            h.update(bytes.fromhex(exp_prev) if exp_prev else b"")
            h.update(str(e["merchant_id"]).encode())
            h.update(e["entity_id"].encode())
            h.update(e["event_type"].encode())
            ts = e["occurred_at"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            h.update(ts.astimezone(UTC).isoformat().encode())
            h.update(bytes.fromhex(e["payload_hash"]))
            exp_hash = h.hexdigest()
            if exp_hash != e["hash"]:
                breaks.append(f"{entity_id}@{e['id']}: hash mismatch")
            prev = e["hash"]
    return {"verified": not breaks, "events": total, "breaks": breaks}
