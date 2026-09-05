"""Standalone chain verifier; no dependency on service code.
Auditor exports event rows ordered by id and re-derives hashes.
"""

import hashlib
from datetime import datetime, UTC


def verify_exported_rows(
    rows: list[dict],
    anchors: list[dict] | None = None,
) -> dict:
    """Verify exported event chains; returns {verified, events, breaks}. A hot chain starting from a matching anchor verifies."""
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
                # Vouched: recompute with the archived prev_hash, not the genesis seed.
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
