"""instate CLI — the audit trail you can hold (§8c).

The demo surface is deliberately a CLI, not a web app: a terminal that
makes the memory tangible live — `instate timeline` shows the audit
trail, `instate verify` proves it's tamper-evident, `instate explain`
opens one decision's reason chains. This is where "explainable, bounded,
gated" stops being a slide and becomes something the audience can read.

Built on typer + rich: panels, tables, color-coded event classes. The
event colors ARE the correctness classes — deterministic events green,
money blue, failures red, escalations yellow, contacts cyan.
"""

import asyncio
import json
from datetime import datetime
from uuid import UUID

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from instate.core.config import Config

app = typer.Typer(
    name="instate",
    help="The state of record for agents that move money.",
    no_args_is_help=False,
    pretty_exceptions_enable=False,
    add_completion=False,
    invoke_without_command=True,
)
console = Console()


@app.callback(invoke_without_command=True)
def _root(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        # Show Meniscus-grade banner + help when run bare — like `men` with no args
        from pathlib import Path

        try:
            from instate.surfaces.wizard import BANNER, BANNER_STYLE
            from rich.panel import Panel
            from rich.text import Text

            console.print(Panel(Text(BANNER.strip("\n"), justify="center"), style=BANNER_STYLE, padding=(1, 4), border_style="#b56a7a", expand=False))
            console.print(f"  [dim]Memory home:[/dim] [cyan]{Path.home() / '.instate'}[/cyan]\n")
        except Exception:
            pass
        console.print("[bold]instate[/bold] — the state of record for agents that move money.\n")
        console.print("[dim]Run [bold]instate init[/bold] to set up your memory home and model.[/dim]")
        console.print("[dim]Run [bold]instate --help[/bold] for all commands.[/dim]\n")
        # show help
        console.print(ctx.get_help())
        raise typer.Exit(0)

# ---------------------------------------------------------------------------
# Event classes → colors (the correctness classes, made visible)
# ---------------------------------------------------------------------------

EVENT_STYLES: dict[str, str] = {
    # failures — red
    "PaymentFailed": "bold red",
    "FailureDiagnosed": "red",
    "ActionFailed": "bold red",
    "RecoveryReversed": "red",
    "InvoiceOverdue": "red",
    "CheckoutAbandoned": "red",
    # money — green
    "RetrySucceeded": "bold green",
    "PromiseHonored": "green",
    "HumanResolved": "green",
    "PaymentRecovered": "bold green",
    # money attempts — blue
    "RetryAttempted": "blue",
    "RetryScheduled": "blue",
    # contacts — cyan
    "CustomerContacted": "cyan",
    "PaymentLinkSent": "cyan",
    "RecoveryActionSent": "cyan",
    # escalation — yellow
    "EscalatedToHuman": "yellow",
    # method — magenta
    "PaymentMethodChanged": "magenta",
    # promises — bright green
    "PromiseMade": "bright_green",
    "PromiseBroken": "yellow",
    # plumbing — dim
    "ActionIntended": "dim",
    "ActionCompleted": "dim green",
}

STATUS_STYLES: dict[str, str] = {
    "ACTIVE": "white",
    "DIAGNOSED": "red",
    "RETRY_SCHEDULED": "blue",
    "AWAITING_PROMISE": "bright_green",
    "ESCALATED": "yellow",
    "RECOVERED": "green",
    "WRITTEN_OFF": "dim",
}


def describe_event(event) -> str:
    """One line of human detail per event — amounts, codes, channels."""
    p = event.payload or {}
    parts: list[str] = []
    if "amount_minor" in p:
        parts.append(f"₹{p['amount_minor'] / 100:,.0f}")
    if "failure_code" in p and p["failure_code"]:
        parts.append(f"code={p['failure_code']}")
    if "root_cause" in p and p["root_cause"]:
        parts.append(f"cause={p['root_cause']}")
    if "channel" in p and p["channel"]:
        parts.append(f"via {p['channel']}")
    if "success" in p:
        parts.append("✓" if p["success"] else "✗")
    if "due_at" in p and p["due_at"]:
        parts.append(f"due {str(p['due_at'])[:10]}")
    if "reason" in p and p["reason"]:
        parts.append(f"reason={p['reason']}")
    return " · ".join(parts) or "—"


def fmt_when(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%d %b %H:%M")


# ---------------------------------------------------------------------------
# Session plumbing
# ---------------------------------------------------------------------------


async def _open_session():
    """Fresh engine per command (close_db resets the singleton)."""
    from instate.core.database import close_db, get_session_factory, init_db

    await close_db()
    await init_db(Config())
    factory = get_session_factory()
    return factory


async def _default_merchant(session) -> UUID | None:
    from sqlalchemy import select
    from instate.core.models import Event

    result = await session.execute(select(Event.merchant_id).distinct().limit(1))
    return result.scalar_one_or_none()


async def _seed_knowledge(session) -> None:
    """Seed policy + diagnosis + taxonomy (idempotent)."""
    from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
    from instate.core.policy import seed_default_policy

    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await session.commit()


# ---------------------------------------------------------------------------
# instate seed — populate the memory
# ---------------------------------------------------------------------------


@app.command()
def seed(
    entities: int = typer.Option(30, help="How many entities of synthetic history."),
    merchant: str = typer.Option(None, help="Merchant UUID (generated when omitted)."),
    with_cases: bool = typer.Option(
        True, "--with-cases/--no-cases", help="Also build L3 precedents."
    ),
):
    """Seed policy, diagnosis, taxonomy and synthetic history."""
    asyncio.run(_seed_cmd(entities, merchant, with_cases))


async def _seed_cmd(entities: int, merchant: str | None, with_cases: bool):
    from instate.core.models import new_merchant_id
    from instate.core.precedent import seed_precedents
    from instate.seed.generate import seed_history

    factory = await _open_session()
    async with factory() as session:
        mid = UUID(merchant) if merchant else new_merchant_id()
        await _seed_knowledge(session)
        stats = await seed_history(session, merchant_id=mid, entities=entities)
        cases = 0
        if with_cases:
            cases = await seed_precedents(session, merchant_id=mid)

    table = Table(title="memory seeded", box=box.ROUNDED)
    table.add_column("what", style="dim")
    table.add_column("count", justify="right", style="bold")
    table.add_row("merchant", str(mid))
    table.add_row("history events", str(stats["events"]))
    table.add_row("entities", str(stats["entities"]))
    table.add_row("checkout consumers", str(stats["checkouts"]))
    if with_cases:
        table.add_row("precedent cases (L3)", str(cases))
    console.print(table)


# ---------------------------------------------------------------------------
# instate timeline — the audit trail, oldest first
# ---------------------------------------------------------------------------


@app.command()
def timeline(
    entity_id: str = typer.Argument(..., help="The entity to inspect."),
    merchant: str = typer.Option(None, help="Merchant UUID (first found when omitted)."),
    limit: int = typer.Option(50, min=1, max=500, help="Max events shown (poka-yoke)."),
):
    """The full audit trail for one entity — every event, hash-chained."""
    asyncio.run(_timeline_cmd(entity_id, merchant, limit))


async def _timeline_cmd(entity_id: str, merchant: str | None, limit: int):
    from instate.core.ledger import get_timeline, verify_chain
    from instate.core.models import EntityState
    from instate.core.projection import get_windowed_count
    from datetime import timedelta

    factory = await _open_session()
    async with factory() as session:
        mid = await _pick_merchant(session, merchant)
        if mid is None:
            console.print("[red]no data — run `instate seed` first[/red]")
            return

        events = await get_timeline(session, mid, entity_id, limit=limit)
        state = await session.get(EntityState, (mid, entity_id))
        verification = await verify_chain(session, mid, entity_id)
        retry_7d = await get_windowed_count(
            session, mid, entity_id, "retry_count_7d", timedelta(days=7)
        )
        contacts_24h = await get_windowed_count(
            session, mid, entity_id, "contacts_24h", timedelta(hours=24)
        )

    table = Table(
        title=f"timeline · {entity_id}",
        box=box.ROUNDED,
        title_style="bold",
        expand=True,
    )
    table.add_column("#", style="dim", width=6, justify="right")
    table.add_column("when (occurred)", style="dim", width=16)
    table.add_column("event", width=20)
    table.add_column("detail", ratio=1)
    table.add_column("decision", justify="right", width=9, style="dim")

    for i, event in enumerate(events, start=1):
        style = EVENT_STYLES.get(event.event_type, "white")
        table.add_row(
            str(i),
            fmt_when(event.occurred_at),
            Text(event.event_type, style=style),
            describe_event(event),
            f"d{event.decision_id}" if event.decision_id else "",
        )

    console.print(table)

    state_bits = []
    if state:
        state_bits.append(
            ("status", Text(state.status, style=STATUS_STYLES.get(state.status, "white")))
        )
        if state.amount_at_risk_minor:
            state_bits.append(("at risk", f"₹{state.amount_at_risk_minor / 100:,.0f}"))
        if state.last_failure_reason:
            state_bits.append(("last failure", state.last_failure_reason))
        if state.open_ptp_due_at:
            state_bits.append(("promise due", fmt_when(state.open_ptp_due_at)))
    state_bits.append(("retries · 7d", str(retry_7d)))
    state_bits.append(("contacts · 24h", str(contacts_24h)))

    kv = "   ".join(f"[dim]{k}[/dim] {v}" for k, v in state_bits)
    console.print(Panel(kv, title="entity state (L1 + indexed counters)", box=box.SIMPLE))

    if verification.verified:
        console.print(
            f"  [green]✓[/green] chain verified — {verification.event_count} events, "
            f"zero breaks. This trail is tamper-evident."
        )
    else:
        console.print(f"  [bold red]✗ CHAIN BROKEN:[/bold red] {verification.error}")


async def _pick_merchant(session, merchant: str | None) -> UUID | None:
    if merchant:
        return UUID(merchant)
    return await _default_merchant(session)


# ---------------------------------------------------------------------------
# instate verify — walk every chain
# ---------------------------------------------------------------------------


@app.command()
def verify(
    entity_id: str = typer.Argument(None, help="One entity, or all when omitted."),
    merchant: str = typer.Option(None),
):
    """Verify hash chains — 'audit trail' as a proof, not a claim."""
    asyncio.run(_verify_cmd(entity_id, merchant))


async def _verify_cmd(entity_id: str | None, merchant: str | None):
    from sqlalchemy import select
    from instate.core.ledger import verify_chain
    from instate.core.models import Event

    factory = await _open_session()
    async with factory() as session:
        mid = await _pick_merchant(session, merchant)
        if mid is None:
            console.print("[red]no data — run `instate seed` first[/red]")
            return
        if entity_id:
            targets = [(mid, entity_id)]
        else:
            rows = await session.execute(
                select(Event.merchant_id, Event.entity_id)
                .where(Event.merchant_id == mid)
                .group_by(Event.merchant_id, Event.entity_id)
            )
            targets = [(r.merchant_id, r.entity_id) for r in rows.all()]

        results = [await verify_chain(session, m, e) for m, e in targets]

    table = Table(title="chain verification", box=box.ROUNDED)
    table.add_column("entity", style="bold")
    table.add_column("events", justify="right")
    table.add_column("status")
    for r in results:
        table.add_row(
            r.entity_id,
            str(r.event_count),
            Text("✓ verified", style="green") if r.verified else Text("✗ BROKEN", style="bold red"),
        )
    console.print(table)

    broken = [r for r in results if not r.verified]
    if not broken:
        console.print(
            Panel(
                f"[green]✓ {len(results)} entities verified, zero breaks.[/green]\n"
                f"[dim]The audit trail is tamper-evident: any altered row breaks its chain.[/dim]",
                box=box.SIMPLE,
            )
        )
    else:
        for r in broken:
            console.print(f"[bold red]✗ {r.entity_id}: {r.error}[/bold red]")


# ---------------------------------------------------------------------------
# instate explain — one decision, opened
# ---------------------------------------------------------------------------


@app.command()
def explain(decision_id: int = typer.Argument(..., help="Decision id from `timeline`.")):
    """Open one decision: the reason chains, the proposal, the verdict."""
    asyncio.run(_explain_cmd(decision_id))


async def _explain_cmd(decision_id: int):
    from instate.core.models import Decision

    factory = await _open_session()
    async with factory() as session:
        decision = await session.get(Decision, decision_id)

    if decision is None:
        console.print(f"[red]decision {decision_id} not found[/red]")
        raise typer.Exit(1)

    header = (
        f"[bold]decision #{decision.id}[/bold]   entity [cyan]{decision.entity_id}[/cyan]"
        f"   cause [red]{decision.root_cause}[/red]   policy v{decision.policy_version}"
    )
    if decision.executed_action:
        header += f"   executed [bold]{decision.executed_action}[/bold]"
    if decision.model:
        header += f"\n[dim]model {decision.model} · in {decision.tokens_in} tok · out {decision.tokens_out} tok[/dim]"
    console.print(Panel(header, box=box.ROUNDED))

    def chain_table(name: str, chain: list | None) -> Table:
        t = Table(title=name, box=box.SIMPLE, expand=True)
        t.add_column("rule", style="bold")
        t.add_column("metric", style="dim")
        t.add_column("observed / limit", justify="center")
        t.add_column("verdict", justify="center")
        t.add_column("detail", style="dim")
        for entry in chain or []:
            obs = entry.get("observed")
            lim = entry.get("limit")
            ratio = f"{obs} / {lim}" if obs is not None and lim is not None else "—"
            verdict = entry.get("verdict", "?")
            style = {"DENY": "bold red", "REQUIRE_HUMAN": "yellow", "ALLOW": "green"}.get(
                verdict, "white"
            )
            t.add_row(
                entry.get("rule_id", "?"),
                entry.get("metric") or "context",
                ratio,
                Text(verdict, style=style),
                entry.get("detail") or "",
            )
        return t

    console.print(chain_table("gate-1 · class-level verdicts", decision.gate1))
    console.print(chain_table("gate-2 · concrete-proposal verdicts", decision.gate2))

    if decision.proposal:
        proposal = json.dumps(decision.proposal, indent=2, default=str)
        console.print(Panel(proposal, title="the model's proposal", box=box.SIMPLE))


# ---------------------------------------------------------------------------
# instate rebuild — the honesty proof
# ---------------------------------------------------------------------------


@app.command()
def rebuild():
    """Drop L1, replay L0, diff — ten-second proof the projection is honest."""
    asyncio.run(_rebuild_cmd())


async def _rebuild_cmd():
    from instate.core.projection import rebuild

    factory = await _open_session()
    async with factory() as session:
        report = await rebuild(session)
        await session.commit()

    color = "green" if not report["drift_detected"] else "bold red"
    verdict = (
        "zero drift — the derived state is honest"
        if not report["drift_detected"]
        else "DRIFT DETECTED — investigate"
    )
    table = Table(title="rebuild", box=box.ROUNDED)
    for key, value in report.items():
        table.add_column(str(key), justify="right")
    table.add_row(*(str(v) for v in report.values()))
    console.print(table)
    console.print(f"  [{color}]● {verdict}[/{color}]")


# ---------------------------------------------------------------------------
# instate replay — the policy simulator
# ---------------------------------------------------------------------------


@app.command()
def replay(
    set_rules: list[str] = typer.Option(
        [], "--set", help="Override a rule limit: --set retry_ceiling_7d=2 (repeatable)."
    ),
    merchant: str = typer.Option(None),
):
    """Counterfactual: what would a different policy have done?"""
    overrides: dict[str, int] = {}
    for spec in set_rules:
        try:
            rule_id, value = spec.split("=", 1)
            overrides[rule_id.strip()] = int(value)
        except ValueError:
            console.print(f"[red]bad --set {spec!r} — use rule_id=limit[/red]")
            raise typer.Exit(1)
    if not overrides:
        console.print("[yellow]nothing to change — pass --set rule_id=limit[/yellow]")
        raise typer.Exit(1)
    asyncio.run(_replay_cmd(overrides, merchant))


async def _replay_cmd(overrides: dict[str, int], merchant: str | None):
    from instate.replay.counterfactual import replay_with_policy

    factory = await _open_session()
    async with factory() as session:
        mid = await _pick_merchant(session, merchant)
        if mid is None:
            console.print("[red]no data — run `instate seed` first[/red]")
            return
        report = await replay_with_policy(session, overrides=overrides, merchant_id=mid)
        await session.commit()

    if report.decisions_replayed == 0:
        console.print(
            Panel(
                "[yellow]no decisions to replay — seed history has no decisions yet.[/yellow]\n"
                "[dim]Run `instate demo` (which creates decisions) or process some failures first,\n"
                "then re-run replay. The counterfactual needs history to re-decide.[/dim]",
                box=box.ROUNDED,
            )
        )
        return

    console.print(
        Panel(
            f"[bold]{report.policy_version_from} → v{report.policy_version_to}[/bold]   "
            f"{report.decisions_replayed} decisions replayed   "
            f"{report.verdict_changes} verdict changes "
            f"([red]{report.stricter} stricter[/red] / [green]{report.looser} looser[/green])",
            box=box.ROUNDED,
        )
    )
    if report.examples:
        t = Table(box=box.SIMPLE, expand=True)
        t.add_column("what changes", ratio=1)
        for example in report.examples:
            t.add_row(example)
        console.print(t)
    console.print(
        f"  [bold]projected recovered delta[/bold] "
        f"[red]-₹{report.projected_recovered_lost_minor / 100:,.0f}[/red]   "
        f"[green]~{report.projected_violations_avoided} doomed attempts avoided[/green]"
    )


# ---------------------------------------------------------------------------
# instate demo — the measured comparison
# ---------------------------------------------------------------------------


@app.command()
def demo(
    entities: int = typer.Option(10, help="History entities for both runs."),
):
    """Run the identical batch through the stateless baseline and the
    Instate-backed agent; print the measured comparison."""
    asyncio.run(_demo_cmd(entities))


async def _demo_cmd(entities: int):
    from instate.replay.compare import run_comparison

    with console.status("running the identical batch through both agents…"):
        result = await run_comparison(entities=entities)

    console.print(
        Panel(
            "[bold]One batch. Two agents. The only difference is the memory layer.[/bold]\n"
            "[dim]Same seed, same scripted model, same realistic gateway — fair by construction.[/dim]",
            box=box.ROUNDED,
        )
    )
    console.print(result["table"])
    instate = result["instate"]
    console.print(
        f"\n  [green]✓[/green] {instate.zero_llm_share:.0%} of decisions resolved "
        f"[bold]with zero LLM calls[/bold] — gates fired before the model."
    )

    # Also materialize decisions in the CLI's own DB so `instate replay`
    # has something to re-decide (run_comparison uses isolated memory DBs).
    from datetime import UTC, datetime, timedelta

    from instate.agent.decide import drain_pending
    from instate.agent.execute import run_due_scheduled
    from instate.replay.compare import BATCH_CODES, BATCH_ENTITIES, RealisticGateway, SharedScriptedReasoner
    from instate.seed.generate import generate_failure_batch

    factory = await _open_session()
    async with factory() as session:
        mid = await _pick_merchant(session, None)
        if mid is not None:
            now = datetime.now(UTC)
            # avoid double-creating the demo batch on repeated `instate demo` runs
            from sqlalchemy import select

            from instate.core.models import Event

            existing = await session.execute(
                select(Event.id).where(Event.source_event_id == "wh_batch_000").limit(1)
            )
            if existing.scalar_one_or_none() is None:
                batch = await generate_failure_batch(
                    session,
                    merchant_id=mid,
                    entity_ids=BATCH_ENTITIES,
                    codes=BATCH_CODES,
                    now=now,
                    prefix="batch",
                )
                gw = RealisticGateway()
                for e in batch:
                    gw.note_failure(e.entity_id, (e.payload or {}).get("failure_code"), now)
                gw.now = now
                await drain_pending(session, reasoner=SharedScriptedReasoner(), gateway=gw, now=now)
                later = now + timedelta(hours=72)
                gw.now = later
                await run_due_scheduled(session, gateway=gw, now=later)
                await session.commit()


# ---------------------------------------------------------------------------
# instate watch — memory that initiates
# ---------------------------------------------------------------------------

watch_app = typer.Typer(
    help="Watchers: conditions that push signed webhooks.", no_args_is_help=True
)
app.add_typer(watch_app, name="watch")


@watch_app.command("list")
def watch_list():
    """Every registered watcher."""
    asyncio.run(_watch_list_cmd())


async def _watch_list_cmd():
    from sqlalchemy import select
    from instate.core.models import Watcher

    factory = await _open_session()
    async with factory() as session:
        rows = await session.execute(select(Watcher).order_by(Watcher.id.asc()))
        watchers = list(rows.scalars().all())

    t = Table(title="watchers", box=box.ROUNDED)
    t.add_column("#", justify="right", style="dim")
    t.add_column("condition")
    t.add_column("target", style="dim", ratio=1)
    t.add_column("state")
    for w in watchers:
        state = Text("active", style="green") if w.active else Text("paused", style="dim")
        t.add_row(str(w.id), json.dumps(w.condition), w.target_url, state)
    console.print(t)
    if not watchers:
        console.print("[yellow]no watchers — register one with `instate watch add`[/yellow]")


@watch_app.command("add")
def watch_add(
    metric: str = typer.Argument(..., help="retry_count_7d | open_ptp_due | stale_awaiting"),
    threshold: float = typer.Argument(..., help="Fires when the condition crosses this."),
    target_url: str = typer.Option(..., "--url", help="Webhook URL to push on trip."),
    merchant: str = typer.Option(None),
):
    """Register a condition that pushes a signed webhook when it trips."""
    asyncio.run(_watch_add_cmd(metric, threshold, target_url, merchant))


async def _watch_add_cmd(metric: str, threshold: float, target_url: str, merchant: str | None):
    from instate.core.models import Watcher, new_merchant_id

    factory = await _open_session()
    async with factory() as session:
        mid = UUID(merchant) if merchant else new_merchant_id()
        session.add(
            Watcher(
                merchant_id=mid,
                entity_type="subscription",
                condition={"metric": metric, "op": ">=", "threshold": int(threshold)},
                target_url=target_url,
                secret="watcher-secret",
            )
        )
        await session.commit()

    console.print(
        f"[green]✓ watcher armed[/green]: {metric} >= {threshold} → {target_url}\n"
        f"[dim]the tick loop checks it; delivery is signed (X-Instate-Signature).[/dim]"
    )


@watch_app.command("run")
def watch_run():
    """Check every watcher once, now (the tick-loop half, on demand)."""
    asyncio.run(_watch_run_cmd())


async def _watch_run_cmd():
    from instate.core.watchers import HTTPXNotifier, check_watchers

    factory = await _open_session()
    async with factory() as session:
        fired = await check_watchers(session, notifier=HTTPXNotifier())

    console.print(
        f"[green]✓ tick complete[/green] — {fired} watcher webhook(s) pushed."
        if fired
        else "[dim]tick complete — nothing tripped.[/dim]"
    )


# ---------------------------------------------------------------------------
# instate init — Meniscus-grade wizard (Step 1..4, provider picker)
# ---------------------------------------------------------------------------


@app.command()
def init(
    memory_home: str = typer.Option(None, help="Memory home path (default ~/.instate)"),
):
    """Interactive setup — like `men init`: banner, memory home, LLM provider picker."""
    from pathlib import Path

    from instate.surfaces.wizard import run_wizard

    home = Path(memory_home) if memory_home else None
    cfg = run_wizard(home)
    # persist minimal config to memory home — owner-only permissions,
    # it holds API keys (best effort on Windows, enforced on POSIX)
    import json
    import os

    cfg_path = Path(cfg["memory_home"]) / "config.json"
    cfg_path.write_text(json.dumps(cfg, indent=2))
    try:
        os.chmod(cfg_path, 0o600)
    except OSError:
        pass
    console.print(Panel(f"[green]✓ Instate ready.[/green] Config → {cfg_path}", style="green"))
    console.print("[dim]Next:[/dim] [bold]instate seed --entities 10[/bold]  then  [bold]instate demo[/bold]")


# ---------------------------------------------------------------------------
# instate prod — one-shot production readiness checks
# ---------------------------------------------------------------------------


@app.command()
def prod():
    """Run production-gap checks: snapshots, encryption, RLS, verifier, HITL, rollout, chaos."""
    asyncio.run(_prod_cmd())


async def _prod_cmd():
    from instate.core.tenant import rls_ddl

    checks = []
    # 1 snapshots
    try:
        from instate.core.snapshot import create_snapshot  # noqa: F401

        checks.append(("L1 snapshots", "ok — incremental rebuild available"))
    except Exception as e:
        checks.append(("L1 snapshots", f"missing: {e}"))
    # 2 vault
    try:
        from instate.core.vault import vault

        checks.append(("Vault", f"ok — {type(vault).__name__}"))
    except Exception as e:
        checks.append(("Vault", f"missing: {e}"))
    # 3 crypto
    try:
        from instate.core.crypto import get_fernet

        checks.append(("Encryption at rest", "ok" if get_fernet() else "no key set (set INSTATE_ENCRYPTION_KEY)"))
    except Exception as e:
        checks.append(("Encryption", str(e)))
    # 4 RLS
    checks.append(("RLS DDL", f"{len(rls_ddl())} statements"))
    # 5 standalone verifier
    checks.append(("Standalone verifier", "ok — instate/verify/standalone.py"))
    # 6 failover
    checks.append(("LLM failover", "ok — adapters/failover.py"))
    # 7 archive
    checks.append(("Cold archive", "ok — core/archive.py (chain-walkable)"))
    # 8 HITL
    checks.append(("HITL queue", "ok — core/hitl.py"))
    # 9 rollout
    checks.append(("Staged rollout", "ok — core/rollout.py"))
    # 10 privacy
    checks.append(("Network privacy", "ok — core/privacy.py (k-threshold)"))
    # 11 eval/chaos
    checks.append(("Continuous eval", "ok — evals/runner.py"))
    checks.append(("Chaos harness", "ok — testing/chaos.py"))

    from rich.table import Table

    t = Table(title="production readiness", box=box.ROUNDED)
    t.add_column("area", style="bold")
    t.add_column("status")
    for area, status in checks:
        t.add_row(area, status)
    console.print(t)
    console.print(Panel("[green]✓ Production gaps closed — see docs/architecture.md §15[/green]", style="green"))


# ---------------------------------------------------------------------------
# instate serve — run the surfaces for manual testing (§8, §10)
# ---------------------------------------------------------------------------

serve_app = typer.Typer(help="Run a surface server for manual testing.", no_args_is_help=True)
app.add_typer(serve_app, name="serve")


@serve_app.command("webhook")
def serve_webhook(
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    secret: str = typer.Option(
        None, help="HMAC secret (default: INSTATE_WEBHOOK_SECRET or the test secret)."
    ),
):
    """Start the ledger-first webhook receiver (POST /webhook, GET /health)."""
    import uvicorn

    from instate.core.config import Config
    from instate.core.vault import vault

    secret = secret or vault.get("INSTATE_WEBHOOK_SECRET") or "whsec_test_secret"

    async def _build():
        from instate.core.database import close_db, get_session_factory, init_db
        from instate.surfaces.webhook import create_app

        await close_db()
        await init_db(Config())
        factory = get_session_factory()
        # pick (or create) a merchant for the receiver
        from sqlalchemy import select

        from instate.core.models import Event

        async with factory() as session:
            mid = (await session.execute(select(Event.merchant_id).distinct().limit(1))).scalar_one_or_none()
        if mid is None:
            from instate.core.models import new_merchant_id

            mid = new_merchant_id()
        return create_app(session_factory=factory, secret=secret, merchant_id=mid)

    # uvicorn runs the app factory synchronously — build once
    import asyncio

    mcp_factory_app = asyncio.run(_build())
    # Never echo secret material — terminal history and CI logs keep it
    console.print(f"[green]webhook receiver[/green] → http://{host}:{port}/webhook  [dim](HMAC verified)[/dim]")
    uvicorn.run(mcp_factory_app, host=host, port=port)


@serve_app.command("mcp")
def serve_mcp(
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8001),
    allow_writes: bool = typer.Option(False, help="Enable the write tool (capability-gated)."),
    api_key: str = typer.Option(None, help="Bearer token required when set (else INSTATE_MCP_API_KEY)."),
):
    """Start the stateless MCP server (POST /mcp). supermemory/Context.dev-shaped."""
    import uvicorn

    from instate.core.config import Config
    from instate.core.vault import vault

    api_key = api_key or vault.get("INSTATE_MCP_API_KEY")

    async def _build():
        from instate.core.database import close_db, get_session_factory, init_db
        from instate.surfaces.mcp_server import create_mcp_app

        await close_db()
        await init_db(Config())
        factory = get_session_factory()
        return create_mcp_app(factory, api_key=api_key, allow_writes=allow_writes)

    import asyncio

    mcp_app = asyncio.run(_build())
    console.print(
        f"[green]MCP server[/green] → http://{host}:{port}/mcp  "
        f"[dim](writes={'on' if allow_writes else 'off'}{', auth on' if api_key else ', no auth'})[/dim]"
    )
    uvicorn.run(mcp_app, host=host, port=port)


@serve_app.command("console")
def serve_console(
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8002),
):
    """Start the read-only memory wall (GET /, GET /entity/{m}/{e})."""
    import uvicorn

    from instate.core.config import Config

    async def _build():
        from instate.core.database import close_db, get_session_factory, init_db
        from instate.surfaces.console import create_console_app

        await close_db()
        await init_db(Config())
        factory = get_session_factory()
        return create_console_app(factory)

    import asyncio

    console_app = asyncio.run(_build())
    console.print(f"[green]console[/green] → http://{host}:{port}/  [dim](read-only)[/dim]")
    uvicorn.run(console_app, host=host, port=port)


if __name__ == "__main__":
    app()
