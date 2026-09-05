"""Process-wide per-entity asyncio locks; SQLite-safe half of TOCTOU (§6).

Hold at most one per span, keyed by (merchant_id, entity_id). Across
processes the PostgreSQL row lock is the authority.
"""

import asyncio
import threading
from uuid import UUID

_locks: dict[tuple[str, str], asyncio.Lock] = {}
_registry_guard = threading.Lock()


def get_entity_lock(merchant_id: UUID | str, entity_id: str) -> asyncio.Lock:
    """Process-wide lock for one entity, created on first use.

    Safe under per-test event loops (binds on first contention).
    """
    key = (str(merchant_id), entity_id)
    lock = _locks.get(key)
    if lock is None:
        with _registry_guard:
            lock = _locks.setdefault(key, asyncio.Lock())
    return lock
