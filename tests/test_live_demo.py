"""Live demo parity and rendering contracts."""

from rich.console import Console

from instate.surfaces.live_demo import (
    STAGE_ORDER,
    _stages_for_result,
    final_table,
    render_pipeline,
    render_scoreboard,
    run_live_demo,
)


class _Result:
    def __init__(self, path, root_cause="insufficient_funds", executed_action="RETRY_SCHEDULED",
                 llm_called=True, decision_id=1):
        self.path = path
        self.root_cause = root_cause
        self.executed_action = executed_action
        self.llm_called = llm_called
        self.decision_id = decision_id


class _Decision:
    def __init__(self, gate1=None, gate2=None, proposal=None):
        self.gate1 = gate1 or []
        self.gate2 = gate2 or []
        self.proposal = proposal or {}


def _chain(rule="retry_ceiling_7d", verdict="ALLOW", obs=1, lim=3):
    return [{"rule_id": rule, "metric": "retry_count_7d",
             "observed": obs, "limit": lim, "verdict": verdict}]


def test_stage_paths_cover_every_terminal():
    deny = _stages_for_result(
        _Result("gate1_deny", executed_action="ESCALATE_HUMAN", llm_called=False),
        _Decision(gate1=_chain(verdict="DENY", obs=3)))
    assert "DENY" in deny["GATE-1"][0]
    assert "0 tokens" in deny["REASON"][0]
    assert set(deny) == set(STAGE_ORDER)

    stop = _stages_for_result(
        _Result("gate2_stop", executed_action="ESCALATE_HUMAN"),
        _Decision(gate1=_chain(), gate2=_chain("contact_freq_24h", "DENY", 2, 2)))
    assert "escalated" in stop["GATE-2"][0]
    assert "skipped" in stop["EXECUTE"][0]

    llm = _stages_for_result(
        _Result("llm", executed_action="SEND_PAYMENT_LINK"),
        _Decision(gate1=_chain(), gate2=_chain("contact_freq_24h"),
                  proposal={"action": "SEND_PAYMENT_LINK", "timing": "T_PLUS_48H",
                            "confidence": 0.81}))
    assert "SEND_PAYMENT_LINK" in llm["REASON"][0] and "0.81" in llm["REASON"][0]
    assert "DNC" in llm["GATE-2"][0]

    det = _stages_for_result(
        _Result("deterministic", executed_action="ESCALATE_HUMAN", llm_called=False),
        _Decision(gate1=_chain()))
    assert "0 tokens" in det["REASON"][0]


def test_renderers_do_not_raise():
    console = Console(width=160)
    full = _stages_for_result(
        _Result("llm"), _Decision(gate1=_chain(), gate2=_chain("contact_freq_24h")))
    with console.capture() as cap:
        console.print(render_pipeline("sub_001", "insufficient_funds", full))
        console.print(render_scoreboard(
            {"recovered": 41200, "dupes": 11, "violations": 6, "llm_calls": (30, 30)},
            {"recovered": 58900, "dupes": 0, "violations": 0, "llm_calls": (9, 30)}))
    out = cap.get()
    assert "sub_001" in out
    assert "recovered" in out and "zero-token" in out


async def test_live_demo_matches_comparison_parity():
    from instate.replay.compare import run_comparison
    console = Console(width=120)
    live = await run_live_demo(entities=10, pace=0, console=console)
    ref = await run_comparison(entities=10)
    assert live["instate"].net_recovered_minor == ref["instate"].net_recovered_minor
    assert live["baseline"].net_recovered_minor == ref["baseline"].net_recovered_minor
    assert live["instate"].decisions == ref["instate"].decisions
    table = final_table(live["baseline"], live["instate"])
    assert any("delta" in str(c.header).lower() for c in table.columns)
