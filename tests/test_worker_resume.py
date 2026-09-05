"""Worker resume + scoreboard escalation + cp1252-safe rendering."""

from rich.console import Console

from instate.agent.execute import write_intent
from instate.agent.reconcile import find_dangling_intents, reconcile_pending
from instate.core.models import Decision
from instate.surfaces.live_demo import _safe, final_table, run_resume


async def _dangling(session, mid, entity_id="sub_hang"):
    from instate.core.ledger import record_event
    from tests.conftest import now_utc
    s = session
    now = now_utc()
    await record_event(
        s, merchant_id=mid, entity_id=entity_id, entity_type="subscription",
        event_type="PaymentFailed", occurred_at=now,
        payload={"amount_minor": 99900, "failure_code": "GATEWAY_TIMEOUT"},
        source_event_id="wh_hang_1")
    await s.commit()
    decision = Decision(merchant_id=mid, entity_id=entity_id, root_cause="network_timeout")
    s.add(decision)
    await s.commit()
    await write_intent(
        s, merchant_id=mid, entity_id=entity_id, entity_type="subscription",
        decision=decision, action="RETRY_NOW", occurred_at=now)
    return decision


class _Gateway:
    def __init__(self, remote=None):
        self._remote = remote
        self.calls = []

    async def lookup(self, key):
        return self._remote

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        from instate.adapters.razorpay import GatewayResponse
        self.calls.append(action)
        return GatewayResponse("completed", provider_ref="pay_1", amount_minor=99900)


async def test_find_dangling_lists_unmatched_only(session):
    from tests.conftest import make_merchant_id
    s = session
    mid = make_merchant_id()
    assert await find_dangling_intents(s) == []
    await _dangling(s, mid)
    dangling = await find_dangling_intents(s)
    assert len(dangling) == 1 and dangling[0].entity_id == "sub_hang"


async def test_reconcile_report_names_via(session):
    from tests.conftest import make_merchant_id
    from instate.adapters.razorpay import GatewayResponse
    s = session
    mid = make_merchant_id()
    await _dangling(s, mid)
    report = []
    n = await reconcile_pending(
        s, gateway=_Gateway(remote=GatewayResponse("completed", provider_ref="pay_x")),
        report=report)
    assert n == 1
    assert report[0].via == "lookup" and report[0].status == "completed"
    assert await find_dangling_intents(s) == []


async def test_reconcile_reexecutes_when_lookup_misses(session):
    from tests.conftest import make_merchant_id
    s = session
    mid = make_merchant_id()
    await _dangling(s, mid)
    gw = _Gateway(remote=None)
    report = []
    assert await reconcile_pending(s, gateway=gw, report=report) == 1
    assert gw.calls == ["RETRY_NOW"] and report[0].via == "re-executed"


async def test_run_resume_renders_stages(session):
    from tests.conftest import make_merchant_id
    s = session
    mid = make_merchant_id()
    await _dangling(s, mid)
    console = Console(width=120)
    with console.capture() as cap:
        details = await run_resume(s, gateway=_Gateway(), pace=0, console=console)
    assert len(details) == 1
    out = cap.get()
    assert "sub_hang" in out and "ActionCompleted written" in out


async def test_run_resume_empty_is_quiet(session):
    console = Console(width=120)
    with console.capture() as cap:
        assert await run_resume(session, gateway=_Gateway(), pace=0, console=console) == []
    assert "nothing to reconcile" in cap.get()


async def test_final_table_carries_escalated_row(session):
    from instate.replay.metrics import RunMetrics
    console = Console(width=160)
    with console.capture() as cap:
        console.print(final_table(RunMetrics(), RunMetrics(), 4, 2))
    out = cap.get()
    assert "escalated entities" in out


def test_safe_downgrades_on_legacy_encoding(monkeypatch):
    import sys

    class _Out:
        encoding = "cp1252"

    monkeypatch.setattr(sys, "stdout", _Out())
    assert _safe("→ · ●") == "-> - *"

    class _Utf:
        encoding = "utf-8"

    monkeypatch.setattr(sys, "stdout", _Utf())
    assert _safe("→ · ●") == "→ · ●"
