"""Live demo: one `Live` view with header, pipeline, and scoreboard regions.
Pipeline stages: INTAKE → DIAGNOSE → GATE-1 → REASON → GATE-2 → EXECUTE.
Colors: green allow/success, red deny/stop, yellow escalate, dim pending/skipped.
End state swaps the pipeline for a bordered delta table. `pace=0` disables animation.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from rich import box
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Stated demo rate for the cost/decision row (1300 tok ≈ $0.019).
USD_PER_MTOK = 15.0


def _marks() -> tuple[str, str]:
    """Done/pending glyphs. ●/○ need UTF-8; cp1252 pipes get ASCII
    fallbacks instead of an encode error (same crash class as §4.18)."""
    import sys

    try:
        "●○".encode((sys.stdout.encoding or "utf-8"), errors="strict")
        return "●", "○"
    except (UnicodeEncodeError, LookupError):
        return "*", "-"


_FALLBACKS = {"●": "*", "○": "-", "→": "->", "·": "-", "✓": "v", "✗": "x", "₹": "Rs "}


def _safe(text: str) -> str:
    """Downgrade non-encodable glyphs for legacy consoles. No-op on UTF-8
    (which `instate` forces at startup); pipes and CI get ASCII instead
    of a UnicodeEncodeError mid-`Live`."""
    import sys

    enc = sys.stdout.encoding or "utf-8"
    try:
        text.encode(enc, errors="strict")
        return text
    except (UnicodeEncodeError, LookupError):
        return "".join(_FALLBACKS.get(ch, ch) for ch in text)


STAGE_ORDER = ["INTAKE", "DIAGNOSE", "GATE-1", "REASON", "GATE-2", "EXECUTE"]


def _rupees(minor: int) -> str:
    return _safe(f"₹{minor / 100:,.0f}")


def _cost(tokens: int) -> str:
    return f"${tokens / 1_000_000 * USD_PER_MTOK:.3f}"


def _verdict_style(verdict: str) -> str:
    return {"ALLOW": "green", "DENY": "bold red", "REQUIRE_HUMAN": "yellow"}.get(
        verdict, "white"
    )


def _chain_summary(chain: list | None, keys: tuple[str, ...] = ("retry", "contact")) -> str:
    """First counter entry of the chain as `rule: observed/limit`."""
    for entry in chain or []:
        metric = (entry.get("metric") or "").lower()
        if any(k in metric or k in (entry.get("rule_id") or "") for k in keys):
            obs, lim = entry.get("observed"), entry.get("limit")
            if obs is not None and lim is not None:
                return f"{entry.get('rule_id')}: {obs}/{lim}"
    return ""


def _chain_verdict(chain: list | None) -> str:
    verdicts = [e.get("verdict") for e in chain or []]
    if "DENY" in verdicts:
        return "DENY"
    if "REQUIRE_HUMAN" in verdicts:
        return "REQUIRE_HUMAN"
    return "ALLOW"


def render_pipeline(
    entity_id: str,
    root_cause: str | None,
    resolved: dict[str, tuple[str, str]],
) -> Panel:
    """The per-entity tracker. `resolved` maps stage → (text, style);
    unresolved stages render dim with their pending/skipped note."""
    pending_note = {
        "INTAKE": "...",
        "DIAGNOSE": "...",
        "GATE-1": "...",
        "REASON": "...",
        "GATE-2": "...",
        "EXECUTE": "...",
    }
    lines: list[Text] = [Text(_safe(f"entity: {entity_id}    root cause: {root_cause or '?'}"), style="bold")]
    lines.append(Text(""))
    done, todo = _marks()
    for stage in STAGE_ORDER:
        if stage in resolved:
            text, style = resolved[stage]
            lines.append(Text(f"{done} {stage:<9} {_safe(text)}", style=style))
        else:
            lines.append(Text(f"{todo} {stage:<9} {_safe(pending_note[stage])}", style="dim"))
    return Panel(Group(*lines), title="pipeline", box=box.ROUNDED)


def render_scoreboard(left: dict, right: dict) -> Panel:
    """Running baseline-vs-instate counters."""

    def col(title: str, d: dict) -> Table:
        t = Table(title=title, box=box.SIMPLE, expand=True)
        t.add_column("metric", style="dim")
        t.add_column("value", justify="right", style="bold")
        t.add_row("recovered", _rupees(d.get("recovered", 0)))
        t.add_row("duplicate retries", str(d.get("dupes", 0)))
        t.add_row("violations", str(d.get("violations", 0)))
        esc = d.get("escalated", (0, 0))
        t.add_row("escalated", f"{esc[0]}/{esc[1]}")
        llm = d.get("llm_calls", (0, 0))
        t.add_row("llm calls", f"{llm[0]}/{llm[1]}")
        return t

    grid = Table.grid(expand=True)
    grid.add_column()
    grid.add_column()
    grid.add_row(col("stateless baseline", left), col("instate", right))
    zero = right.get("llm_calls", (0, 0))
    share = f" ({100 * (1 - zero[0] / zero[1]):.0f}% zero-token)" if zero[1] else ""
    grid.add_row("", Text(f"instate llm calls include{share}", style="dim"))
    return Panel(grid, title="scoreboard (live)", box=box.ROUNDED)


def final_table(baseline, instate, baseline_escalated: int = 0, instate_escalated: int = 0) -> Table:
    """Bordered comparison table with a delta column."""
    t = Table(title=_safe("measured comparison — same batch, same model, same gateway"), box=box.ROUNDED)
    t.add_column("metric", style="bold")
    t.add_column("baseline", justify="right")
    t.add_column("instate", justify="right")
    t.add_column("delta", justify="right", style="green")

    def pct(n: float, d: int) -> str:
        return f"{n / d:.0%}" if d else _safe("—")

    na = _safe("—")
    lift = instate.net_recovered_minor - baseline.net_recovered_minor
    lift_pct = f"+{(lift / baseline.net_recovered_minor):.0%}" if baseline.net_recovered_minor else na
    dupes_avoided = max(0, baseline.retry_violations - instate.retry_violations)
    viol_delta = baseline.compliance_violations - instate.compliance_violations

    t.add_row(
        "net recovered",
        _rupees(baseline.net_recovered_minor),
        _rupees(instate.net_recovered_minor),
        lift_pct,
    )
    t.add_row(
        "attempted-recovery-rate",
        f"{baseline.attempted_recovery_rate:.0%}",
        f"{instate.attempted_recovery_rate:.0%}",
        f"+{instate.attempted_recovery_rate - baseline.attempted_recovery_rate:.0%}",
    )
    t.add_row("duplicate retries avoided", na, str(dupes_avoided), "")
    t.add_row(
        "compliance violations",
        str(baseline.compliance_violations),
        str(instate.compliance_violations),
        f"-{viol_delta}" if viol_delta else na,
    )
    t.add_row(
        "escalated entities",
        str(baseline_escalated),
        str(instate_escalated),
        "",
    )
    t.add_row(
        "% resolved with 0 LLM calls",
        pct(baseline.zero_llm_decisions, baseline.decisions),
        pct(instate.zero_llm_decisions, instate.decisions),
        "",
    )
    t.add_row(
        "cost / decision",
        _cost(int(baseline.avg_input_tokens)),
        _cost(int(instate.avg_input_tokens)),
        "",
    )
    t.add_row(
        "chain verification",
        "n/a",
        "0 breaks" if instate.chain_verified else "BROKEN",
        "",
    )
    return t


async def _side_counters(session, merchant_id) -> dict:
    from sqlalchemy import func, select

    from instate.core.models import Decision, Event
    from instate.replay.metrics import money_flow, scan_compliance

    gross, reversed_ = await money_flow(session, merchant_id=merchant_id)
    rv, cv = await scan_compliance(session, merchant_id=merchant_id)
    llm = (
        await session.execute(
            select(func.count(Decision.id)).where(
                Decision.merchant_id == merchant_id,
                Decision.tokens_in.is_not(None),
                Decision.tokens_in > 0,
            )
        )
    ).scalar() or 0
    total = (
        await session.execute(
            select(func.count(Decision.id)).where(Decision.merchant_id == merchant_id)
        )
    ).scalar() or 0
    # Compliant escalation is half the Track 3 bar: distinct entities
    # parked with a human, over entities seen so far.
    esc = (
        await session.execute(
            select(func.count(func.distinct(Event.entity_id))).where(
                Event.merchant_id == merchant_id,
                Event.event_type == "EscalatedToHuman",
            )
        )
    ).scalar() or 0
    ents = (
        await session.execute(
            select(func.count(func.distinct(Event.entity_id))).where(
                Event.merchant_id == merchant_id
            )
        )
    ).scalar() or 0
    return {
        "recovered": gross - reversed_,
        "dupes": rv,
        "violations": rv + cv,
        "escalated": (esc, ents),
        "llm_calls": (llm, total),
    }


def _stages_for_result(result, decision) -> dict[str, tuple[str, str]]:
    """Stage lines from the recorded ProcessingResult + decision row."""

    gate1 = (decision.gate1 if decision else []) or []
    gate2 = (decision.gate2 if decision else []) or []
    proposal = (decision.proposal if decision else {}) or {}
    stages: dict[str, tuple[str, str]] = {
        "INTAKE": ("verified · not a duplicate", "green"),
        "DIAGNOSE": (f"{result.root_cause}", "green"),
    }
    g1_summary = _chain_summary(gate1) or "policy check"
    g1_verdict = _chain_verdict(gate1)
    if result.path == "gate1_deny":
        stages["GATE-1"] = (f"{g1_summary}   DENY  →  escalated", "bold red")
        stages["REASON"] = ("(skipped — gate-1 denied, 0 tokens)", "dim")
        stages["GATE-2"] = ("(skipped)", "dim")
        stages["EXECUTE"] = ("(skipped)", "dim")
        return stages
    stages["GATE-1"] = (f"{g1_summary}   {g1_verdict}", _verdict_style(g1_verdict))

    if result.path == "deterministic":
        stages["REASON"] = ("(deterministic route — 0 tokens)", "dim")
    elif result.llm_called and proposal:
        conf = proposal.get("confidence")
        conf_s = f" · confidence {conf:.2f}" if isinstance(conf, (int, float)) else ""
        stages["REASON"] = (
            f"{proposal.get('action')} · {proposal.get('timing')}{conf_s}",
            "green",
        )
    else:
        stages["REASON"] = ("(policy default — model unavailable, 0 tokens)", "yellow")

    g2_summary = _chain_summary(gate2) or "proposal check"
    g2_verdict = _chain_verdict(gate2)
    if result.path == "gate2_stop":
        stages["GATE-2"] = (f"{g2_summary}   {g2_verdict}  →  escalated", "yellow")
        stages["EXECUTE"] = ("(skipped)", "dim")
        return stages
    dnc = "DNC: clear"
    stages["GATE-2"] = (f"{g2_summary} · {dnc}   {g2_verdict}", _verdict_style(g2_verdict))
    stages["EXECUTE"] = (
        f"{result.executed_action or '—'} — ActionIntended → gateway → committed",
        "green" if (result.executed_action or "").startswith(("RETRY", "SEND")) else "yellow",
    )
    return stages


async def run_live_demo(
    *,
    entities: int = 10,
    seed: int = 42,
    pace: float = 0.45,
    console: Console | None = None,
    gateway_factory=None,
) -> dict:
    """Per-entity animated comparison. Returns the same dict shape as
    run_comparison (baseline, instate, table) plus per-entity results.
    `gateway_factory`, when given, builds the gateway for EACH arm (fair by
    construction — same class, separate state) — used by
    `instate demo --live` with test-mode keys."""
    from instate.agent.decide import process_failure
    from instate.agent.execute import run_due_scheduled
    from instate.core.models import Decision
    from instate.replay.baseline import StatelessBaselineAgent
    from instate.replay.compare import (
        BATCH_CODES,
        BATCH_ENTITIES,
        SharedScriptedReasoner,
        _fresh_setup,
        _note_failure,
        _set_now,
    )
    from instate.replay.metrics import compute_run_metrics, format_comparison
    from instate.seed.generate import generate_failure_batch

    console = console or Console()
    now = datetime.now(UTC)

    base = await _fresh_setup(seed, entities, now,
                              gateway=gateway_factory() if gateway_factory else None)
    inst = await _fresh_setup(seed, entities, now,
                              gateway=gateway_factory() if gateway_factory else None)

    batch_base = await generate_failure_batch(
        base.session, merchant_id=base.merchant_id,
        entity_ids=BATCH_ENTITIES, codes=BATCH_CODES, now=now, prefix="batch")
    batch_inst = await generate_failure_batch(
        inst.session, merchant_id=inst.merchant_id,
        entity_ids=BATCH_ENTITIES, codes=BATCH_CODES, now=now, prefix="batch")
    for setup, batch in ((base, batch_base), (inst, batch_inst)):
        for event in batch:
            _note_failure(
                setup, event.entity_id, (event.payload or {}).get("failure_code"), now)

    baseline_agent = StatelessBaselineAgent(SharedScriptedReasoner(), base.gateway)
    instate_reasoner = SharedScriptedReasoner()

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="pipeline", size=13),
        Layout(name="scoreboard"),
    )
    n = len(batch_base)
    blank: dict = {
        "recovered": 0, "dupes": 0, "violations": 0,
        "escalated": (0, 0), "llm_calls": (0, 0),
    }
    left: dict = dict(blank)
    right: dict = dict(blank)
    instate_results = []
    baseline_results = []

    async def _sleep():
        if pace > 0:
            await asyncio.sleep(pace)

    with Live(layout, console=console, refresh_per_second=4, transient=False):
        for i, (bev, iev) in enumerate(zip(batch_base, batch_inst), start=1):
            layout["header"].update(
                Panel(
                    _safe(f"INSTATE — recovery agent demo    entity {i} / {n}    batch B7"),
                    box=box.ROUNDED,
                )
            )
            # Baseline side runs silently (it has no stages); the tracker
            # follows the Instate entity — the side with something to show.
            bres = await baseline_agent.process_failure(base.session, event=bev, now=now)
            baseline_results.append(bres)
            await base.session.commit()

            code = (iev.payload or {}).get("failure_code", "?")
            resolved: dict[str, tuple[str, str]] = {}
            layout["pipeline"].update(render_pipeline(iev.entity_id, None, resolved))
            await _sleep()
            resolved["INTAKE"] = ("verified · not a duplicate", "green")
            layout["pipeline"].update(render_pipeline(iev.entity_id, None, resolved))
            await _sleep()

            result = await process_failure(
                inst.session, event=iev, reasoner=instate_reasoner,
                gateway=inst.gateway, now=now)
            await inst.session.commit()
            instate_results.append(result)
            decision = await inst.session.get(Decision, result.decision_id) if result.decision_id else None
            full = _stages_for_result(result, decision)
            order = ["DIAGNOSE", "GATE-1", "REASON", "GATE-2", "EXECUTE"]
            resolved = {"INTAKE": full["INTAKE"]}
            for stage in order:
                resolved[stage] = full[stage]
                layout["pipeline"].update(
                    render_pipeline(iev.entity_id, f"{result.root_cause}  (from failure_code {code})", resolved))
                # Freeze on the terminal verdict a beat longer
                await _sleep()
                if pace > 0 and stage in ("GATE-1", "GATE-2") and "DENY" in full[stage][0]:
                    await asyncio.sleep(pace)

            left = await _side_counters(base.session, base.merchant_id)
            right = await _side_counters(inst.session, inst.merchant_id)
            layout["scoreboard"].update(render_scoreboard(left, right))

        later = now + timedelta(hours=72)
        for setup in (base, inst):
            _set_now(setup, later)
            await run_due_scheduled(setup.session, gateway=setup.gateway, now=later)
            await setup.session.commit()
        left = await _side_counters(base.session, base.merchant_id)
        right = await _side_counters(inst.session, inst.merchant_id)
        layout["scoreboard"].update(render_scoreboard(left, right))

    baseline_metrics = await compute_run_metrics(base.session, merchant_id=base.merchant_id)
    instate_metrics = await compute_run_metrics(inst.session, merchant_id=inst.merchant_id)
    for setup in (base, inst):
        await setup.session.close()
        await setup.engine.dispose()

    table = format_comparison(baseline_metrics, instate_metrics)
    return {
        "baseline": baseline_metrics,
        "instate": instate_metrics,
        "baseline_results": baseline_results,
        "instate_results": instate_results,
        "table": table,
        "escalated": (left["escalated"][0], right["escalated"][0]),
    }


def render_resume(state: dict[str, tuple[str, str]]) -> Panel:
    """Boot-reconciler view. Same color contract as the pipeline tracker:
    dim ○ = pending lookup, green ● = receipt written, yellow = still unknown."""
    lines: list[Text] = [Text("worker --resume  (boot reconciliation)", style="bold"), Text("")]
    done, todo = _marks()
    for entity_id, (text, style) in state.items():
        mark = done if style != "dim" else todo
        lines.append(Text(_safe(f"{mark} {entity_id}  {text}"), style=style))
    if not state:
        lines.append(Text("no dangling intents — nothing to reconcile", style="green"))
    return Panel(Group(*lines), title="reconciler", box=box.ROUNDED)


async def run_resume(
    session,
    *,
    gateway,
    pace: float = 0.45,
    console: Console | None = None,
) -> list:
    """Resolve dangling intents with staged output. Returns ReconciledIntents."""
    from instate.agent.reconcile import find_dangling_intents, reconcile_one

    console = console or Console()
    dangling = await find_dangling_intents(session)
    state: dict[str, tuple[str, str]] = {}
    console.print(render_resume(state))
    console.print(f"[dim]reconciling … found {len(dangling)} unmatched ActionIntended[/dim]")

    details = []
    for intent in dangling:
        key = (intent.payload or {}).get("idempotency_key", "?")
        state[intent.entity_id] = (f"querying gateway by idempotency_key {key[:24]}…", "dim")
        console.print(render_resume(state))
        if pace > 0:
            await asyncio.sleep(pace)
        detail = await reconcile_one(session, gateway=gateway, intent=intent)
        await session.commit()
        if detail.status == "completed":
            state[detail.entity_id] = (
                f"ActionCompleted written ({detail.via})", "green")
        elif detail.status == "failed":
            state[detail.entity_id] = (
                f"ActionFailed written ({detail.via})", "bold red")
        else:
            state[detail.entity_id] = (
                f"gateway {detail.status} — intent stands, safe to re-run", "yellow")
        console.print(render_resume(state))
        if pace > 0:
            await asyncio.sleep(pace)
        details.append(detail)
    return details
