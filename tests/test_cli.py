"""CLI audit-trail commands via CliRunner."""

import pytest
from typer.testing import CliRunner

from instate.surfaces.cli import app

runner = CliRunner()


@pytest.fixture()
def demo_db(tmp_path, monkeypatch):
    """File-backed DB shared across CLI invocations."""
    db_path = tmp_path / "cli.db"
    monkeypatch.setenv("INSTATE_DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    return str(db_path)


def _invoke(*args):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, f"command failed: {result.output}\n{result.exception}"
    return result.output


def _invoke_ok(*args):
    result = runner.invoke(app, list(args))
    return result


def test_help_is_informative(demo_db):
    out = _invoke("--help")
    assert "state of record" in out
    assert "timeline" in out and "verify" in out and "explain" in out


def test_seed_then_timeline_renders_the_trail(demo_db, capsys):
    out = _invoke("seed", "--entities", "6", "--no-cases")
    assert "history events" in out

    out = _invoke("timeline", "sub_000")
    assert "PaymentFailed" in out or "FailureDiagnosed" in out
    assert "— timeline" in out


def test_timeline_unknown_entity_still_verifies_empty(demo_db):
    _invoke("seed", "--entities", "4", "--no-cases")
    out = _invoke("timeline", "ghost_entity")
    assert "ghost_entity" in out and "timeline" in out


def test_verify_all_reports_zero_breaks(demo_db):
    _invoke("seed", "--entities", "6", "--no-cases")
    out = _invoke("verify")
    assert "verified, zero breaks" in out


def test_verify_one_entity(demo_db):
    _invoke("seed", "--entities", "4", "--no-cases")
    out = _invoke("verify", "sub_000")
    assert "hashes checked" in out
    assert "intact" in out


def test_explain_opens_a_decision(demo_db):
    _invoke("seed", "--entities", "4", "--no-cases")
    import asyncio

    async def _make_decision():
        from instate.core.database import close_db, get_session_factory, init_db
        from instate.core.gate import evaluate
        from sqlalchemy import select

        await close_db()
        await init_db()
        factory = get_session_factory()
        async with factory() as session:
            mid = (
                await session.execute(select(Event.merchant_id).distinct().limit(1))
            ).scalar_one()
            g1 = await evaluate(
                session,
                merchant_id=mid,
                entity_id="sub_000",
                entity_type="subscription",
                action_class="RETRY_SCHEDULED",
                root_cause="insufficient_funds",
            )
            await session.commit()
            return g1.decision_id

    decision_id = asyncio.run(_make_decision())
    out = _invoke("explain", str(decision_id))
    assert "gate-1" in out
    assert "retry_ceiling_7d" in out
    assert "ALLOW" in out or "DENY" in out
    assert "reproducible" in out


def test_explain_missing_decision_fails_cleanly(demo_db):
    result = _invoke_ok("explain", "999999")
    assert result.exit_code == 1
    assert "not found" in result.output


def test_rebuild_reports_zero_drift(demo_db):
    _invoke("seed", "--entities", "6", "--no-cases")
    out = _invoke("rebuild")
    assert "zero drift" in out


def test_worker_tick_decides_webhook_failures(demo_db):
    """The demo-script beat: seed → webhook append → tick → explain works."""
    import asyncio

    _invoke("seed", "--entities", "4", "--no-cases")

    async def _append():
        from instate.core.database import close_db, get_session_factory, init_db
        from instate.core.ledger import record_event
        from datetime import UTC, datetime

        await close_db()
        await init_db()
        factory = get_session_factory()
        async with factory() as session:
            from sqlalchemy import select

            mid = (await session.execute(select(Event.merchant_id).distinct().limit(1))).scalar_one()
            await record_event(
                session, merchant_id=mid, entity_id="pay_tick",
                entity_type="payment", event_type="PaymentFailed",
                occurred_at=datetime.now(UTC),
                payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
                source_event_id="wh_tick_1")
            await session.commit()

    asyncio.run(_append())
    out = _invoke("worker", "tick")
    assert "failures decided" in out
    out = _invoke("timeline", "pay_tick")
    assert "FailureDiagnosed" in out
    out = _invoke("rebuild")
    assert "zero drift" in out  # due-scheduled outcomes refold L1


def test_replay_spacing_zero_moves_numbers(demo_db):
    _invoke("seed", "--entities", "8", "--no-cases")
    _invoke("demo", "--entities", "6", "--pace", "0")
    out = _invoke("replay", "--set", "retry_spacing_24h=0")
    assert "verdict changes" in out
    assert "0 verdict changes" not in out


def test_demo_prints_the_comparison(demo_db):
    out = _invoke("demo", "--entities", "6")
    assert "One batch. Two agents." in out
    assert "net money recovered" in out
    assert "hash chain verified" in out


def test_watch_add_and_list(demo_db):
    _invoke("watch", "add", "retry_count_7d", "2", "--url", "https://agent.example/hook")
    out = _invoke("watch", "list")
    assert "retry_count_7d" in out
    assert "https://agent.example/hook" in out


def test_watch_list_empty_is_friendly(demo_db):
    out = _invoke("watch", "list")
    assert "no watchers" in out


# Late import for the decision test below.
from instate.core.models import Event  # noqa: E402
