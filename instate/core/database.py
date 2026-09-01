"""Instate database setup — async engine and session management."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from instate.core.config import Config

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine(config: Config | None = None) -> AsyncEngine:
    """Get or create the async engine (singleton per process)."""
    global _engine
    if _engine is None:
        cfg = config or Config()
        _engine = create_async_engine(
            cfg.database_url,
            echo=False,
            pool_pre_ping=True if cfg.is_postgres else False,
        )
    return _engine


def get_session_factory(config: Config | None = None) -> async_sessionmaker[AsyncSession]:
    """Get or create the session factory (singleton per process)."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine(config)
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


async def get_session() -> AsyncSession:
    """Context-managed session (use with `async with`)."""
    factory = get_session_factory()
    return factory()


async def init_db(config: Config | None = None) -> AsyncEngine:
    """Create all tables. Idempotent — safe to call on every startup."""
    from instate.core.models import Base

    engine = get_engine(config)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


async def close_db() -> None:
    """Dispose the engine (call on shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
