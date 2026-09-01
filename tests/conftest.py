"""Test fixtures — in-memory SQLite per test (fast, isolated)."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from instate.core.database import close_db, init_db
from instate.core.database import get_session_factory


@pytest.fixture(scope="session")
def event_loop():
    """Event loop for the test session (pytest-asyncio)."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def session():
    """Provide a fresh in-memory database session per test.

    Each test gets its own database, so tests are fully isolated.
    """
    import os

    os.environ["INSTATE_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

    # Force re-creation of the engine (bypass singleton for tests)
    await close_db()
    await init_db()

    factory = get_session_factory()
    async with factory() as s:
        yield s

    await close_db()


# ---------------------------------------------------------------------------
# Helpers for building test events
# ---------------------------------------------------------------------------


def make_merchant_id():
    return uuid4()


def now_utc():
    return datetime.now(UTC)


def hours_ago(hours: float):
    return datetime.now(UTC) - timedelta(hours=hours)


def days_ago(days: float):
    return datetime.now(UTC) - timedelta(days=days)
