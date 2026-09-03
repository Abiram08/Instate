"""Tenant isolation — RLS as code (§15) + application guard.

Postgres RLS is the hard wall: even a bug that forgets `WHERE merchant_id`
cannot leak rows. The helpers here emit the DDL and provide a session-level
guard for SQLite/test as well.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

RLS_TABLES = ["events", "entity_state", "decisions", "scheduled_actions", "cases", "watchers", "hitl_tasks"]


def rls_ddl() -> list[str]:
    stmts = []
    for tbl in RLS_TABLES:
        stmts.append(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY;")
        stmts.append(
            f"CREATE POLICY tenant_isolation ON {tbl} "
            f"USING (merchant_id = current_setting('app.current_merchant')::uuid);"
        )
    return stmts


async def set_tenant(session: AsyncSession, merchant_id) -> None:
    """Set the session's tenant. On Postgres this drives RLS; on SQLite
    it is a no-op (the app's WHERE clauses are the guard in dev)."""
    try:
        await session.execute(text("SELECT set_config('app.current_merchant', :m, true)"), {"m": str(merchant_id)})
    except Exception:
        pass  # SQLite / no RLS


async def assert_tenant_scope(session: AsyncSession, merchant_id, table) -> None:
    """Debug helper: verify a query is scoped. Raises if unscoped rows exist."""
    from sqlalchemy import select, func

    total = await session.execute(select(func.count()).select_from(table))
    scoped = await session.execute(select(func.count()).select_from(table).where(table.c.merchant_id == merchant_id))
    if total.scalar_one() != scoped.scalar_one():
        raise AssertionError(f"unscoped rows detected in {table.name}")
