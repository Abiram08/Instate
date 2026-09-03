"""Instate console — the read-only memory wall (§8d).

supermemory's product moment, adapted to guarantees: "see what your
agent remembers" — one merchant, every entity's state, the gates that
fired, every decision with its reason chain. Server-rendered HTML, no
JS build, an evening of work — and it is the moment a judge stops
seeing a script and starts seeing a product.
"""

from html import escape


CSS = """
body { font-family: ui-monospace, 'SF Mono', Menlo, monospace; background: #0d1117;
       color: #e6edf3; margin: 2rem; }
h1 { font-size: 1.2rem; } h1 span { color: #58a6ff; }
h2 { font-size: 0.95rem; color: #8b949e; border-bottom: 1px solid #21262d;
     padding-bottom: .3rem; margin-top: 2rem; }
table { border-collapse: collapse; width: 100%; font-size: .82rem; }
th, td { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid #21262d; }
th { color: #8b949e; font-weight: normal; }
td.num { text-align: right; }
.ACTIVE { color: #e6edf3; } .DIAGNOSED { color: #f85149; }
.RETRY_SCHEDULED { color: #58a6ff; } .AWAITING_PROMISE { color: #7ee787; }
.ESCALATED { color: #d29922; } .RECOVERED { color: #3fb950; }
.WRITTEN_OFF { color: #484f58; }
.verdict-DENY { color: #f85149; font-weight: bold; }
.verdict-REQUIRE_HUMAN { color: #d29922; }
.verdict-ALLOW { color: #3fb950; }
.chain { color: #8b949e; font-size: .75rem; }
a { color: #58a6ff; text-decoration: none; }
"""


def _row(cells: list[str], tags: str = "td") -> str:
    tds = "".join(f"<{tags}>{c}</{tags}>" for c in cells)
    return f"<tr>{tds}</tr>"


def _status_span(status: str | None) -> str:
    return f'<span class="{escape(status or "ACTIVE")}">{escape(status or "—")}</span>'


def render_index(states: list[dict]) -> str:
    rows = "".join(
        _row(
            [
                f'<a href="/entity/{s["merchant_id"]}/{s["entity_id"]}">{escape(s["entity_id"])}</a>',
                _status_span(s["status"]),
                escape(s.get("last_failure_reason") or "—"),
                f'<td class="num">{s.get("retry_count_7d", 0)}</td>',
                f'<td class="num">{s.get("contacts_24h", 0)}</td>',
                f'<td class="num">₹{(s.get("amount_at_risk_minor") or 0) / 100:,.0f}</td>',
            ]
        )
        for s in states
    )
    return f"""<!doctype html><html><head><title>instate console</title>
<style>{CSS}</style></head><body>
<h1>instate <span>· the memory wall</span></h1>
<p class="chain">every entity, its state, and the counters the gates enforce — read-only.
precedent is advisory; nothing on this page can act.</p>
<h2>entities</h2>
<table><tr><th>entity</th><th>status</th><th>last failure</th>
<th class="num">retries · 7d</th><th class="num">contacts · 24h</th><th class="num">at risk</th></tr>
{rows}</table>
<p class="chain">plain Postgres underneath — your memory exports with pg_dump.</p>
</body></html>"""


def render_entity(
    entity_id: str,
    state: dict,
    timeline: list[dict],
    decisions: list[dict],
) -> str:
    timeline_rows = "".join(
        _row(
            [
                escape(t["occurred_at"][:16]),
                f'<span class="chain">{escape(t["event_type"])}</span>',
                escape(t.get("detail") or "—"),
            ]
        )
        for t in timeline
    )

    decision_blocks = []
    for d in decisions:
        chain_rows = "".join(
            f"<tr><td>{escape(c.get('rule_id', '?'))}</td>"
            f"<td>{escape(str(c.get('observed') if c.get('observed') is not None else '—'))}"
            f" / {escape(str(c.get('limit') if c.get('limit') is not None else '—'))}</td>"
            f'<td><span class="verdict-{escape(c.get("verdict", "?"))}">'
            f"{escape(c.get('verdict', '?'))}</span></td></tr>"
            for c in (d.get("gate1") or [])
        )
        proposal = escape(str(d.get("proposal") or ""))
        decision_blocks.append(f"""
<h2>decision #{d.get("id")} · {escape(d.get("executed_action") or "not executed")}
    <span class="chain">policy v{d.get("policy_version")} · {escape(d.get("model") or "no model")}</span></h2>
<table><tr><th>rule</th><th>observed / limit</th><th>verdict</th></tr>{chain_rows}</table>
<p class="chain">proposal: {proposal}</p>""")

    return f"""<!doctype html><html><head><title>instate · {escape(entity_id)}</title>
<style>{CSS}</style></head><body>
<h1><a href="/">instate</a> <span>· {escape(entity_id)}</span></h1>
<p>status {_status_span(state.get("status"))} · at risk
₹{(state.get("amount_at_risk_minor") or 0) / 100:,.0f} · retries 7d
{state.get("retry_count_7d", 0)} · contacts 24h {state.get("contacts_24h", 0)}</p>
<h2>timeline</h2>
<table>{timeline_rows}</table>
{"".join(decision_blocks) or '<p class="chain">no decisions yet.</p>'}
</body></html>"""


def create_console_app(session_factory):
    """Read-only FastAPI wrapper. No POST routes exist — a console that
    cannot act is the point."""
    from fastapi import FastAPI, Response

    web = FastAPI(title="instate-console", docs_url=None, redoc_url=None)

    @web.get("/")
    async def index() -> Response:
        from datetime import timedelta

        from sqlalchemy import select

        from instate.core.models import EntityState
        from instate.core.projection import get_windowed_count

        async with session_factory() as session:
            rows = await session.execute(select(EntityState).limit(200))
            states = []
            for s in rows.scalars():
                states.append(
                    {
                        "merchant_id": str(s.merchant_id),
                        "entity_id": s.entity_id,
                        "status": s.status,
                        "last_failure_reason": s.last_failure_reason,
                        "amount_at_risk_minor": s.amount_at_risk_minor,
                        "retry_count_7d": await get_windowed_count(
                            session,
                            s.merchant_id,
                            s.entity_id,
                            "retry_count_7d",
                            timedelta(days=7),
                        ),
                        "contacts_24h": await get_windowed_count(
                            session,
                            s.merchant_id,
                            s.entity_id,
                            "contacts_24h",
                            timedelta(hours=24),
                        ),
                    }
                )
        return Response(render_index(states), media_type="text/html")

    @web.get("/entity/{merchant_id}/{entity_id}")
    async def entity(merchant_id: str, entity_id: str) -> Response:
        from datetime import timedelta
        from uuid import UUID as UUIDType

        from sqlalchemy import select

        from instate.core.ledger import get_timeline
        from instate.core.models import Decision, EntityState
        from instate.core.projection import get_windowed_count

        async with session_factory() as session:
            mid = UUIDType(merchant_id)
            state_row = await session.get(EntityState, (mid, entity_id))
            if state_row is None:
                return Response("unknown entity", status_code=404)
            state = {
                "status": state_row.status,
                "amount_at_risk_minor": state_row.amount_at_risk_minor,
                "retry_count_7d": await get_windowed_count(
                    session, mid, entity_id, "retry_count_7d", timedelta(days=7)
                ),
                "contacts_24h": await get_windowed_count(
                    session, mid, entity_id, "contacts_24h", timedelta(hours=24)
                ),
            }
            events = await get_timeline(session, mid, entity_id, limit=100)
            timeline = [
                {
                    "occurred_at": e.occurred_at.isoformat(),
                    "event_type": e.event_type,
                    "detail": "; ".join(f"{k}={v}" for k, v in (e.payload or {}).items()) or None,
                }
                for e in events
            ]
            decision_rows = await session.execute(
                select(Decision)
                .where(Decision.merchant_id == mid, Decision.entity_id == entity_id)
                .order_by(Decision.id.desc())
                .limit(20)
            )
            decisions = [
                {
                    "id": d.id,
                    "policy_version": d.policy_version,
                    "gate1": d.gate1,
                    "proposal": d.proposal,
                    "executed_action": d.executed_action,
                    "model": d.model,
                }
                for d in decision_rows.scalars()
            ]

        return Response(
            render_entity(entity_id, state, timeline, decisions),
            media_type="text/html",
        )

    return web
