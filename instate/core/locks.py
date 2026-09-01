"""Process-wide per-entity locks — the SQLite-safe half of TOCTOU (§6).

Two layers close the gate→intent race:

1. DB row lock (`SELECT ... FOR UPDATE` on `entity_state`, see
   `gate.lock_entity_state`) — real on PostgreSQL, a **no-op on SQLite**
   by dialect design.
2. THIS module — an `asyncio.Lock` per (merchant_id, entity_id), held by
   every pipeline entry point for the whole gate→intent span. Works on
   every backend, including SQLite.

Layer 2 is what actually serializes two concurrent `process_failure`
calls in one process: the second waits before Gate-1, so it observes
the first call's committed RetryAttempted and hits the ceiling DENY —
instead of both passing and double-acting.

Rules for holders (deadlock-freedom by construction):
- A span holds AT MOST ONE entity lock. Never nest them.
- The lock is per (merchant_id, entity_id) — different entities never
  block each other.
- The registry is process-global and keyed by UUIDs, so concurrent
  tests (fresh merchant per test) never share a lock.

Production note: this serializes per entity inside ONE process. Across
processes/hosts, PostgreSQL's row lock (layer 1) is the authority —
which is why both layers exist and neither is removed.
"""

import asyncio
import threading
from uuid import UUID

_locks: dict[tuple[str, str], asyncio.Lock] = {}
_registry_guard = threading.Lock()


def get_entity_lock(merchant_id: UUID | str, entity_id: str) -> asyncio.Lock:
    """Return the process-wide lock for one entity (creating it if needed).

    `asyncio.Lock()` takes no loop at construction time (3.10+) — it binds
    to the running loop on first contention — so module-level creation is
    safe under pytest-asyncio's per-test loops.
    """
    key = (str(merchant_id), entity_id)
    lock = _locks.get(key)
    if lock is None:
        with _registry_guard:
            lock = _locks.setdefault(key, asyncio.Lock())
    return lock
