"""Ledger-derived run metrics: money, compliance, tokens, chain integrity.
Same functions measure both agents; attempted rate = recovered / attempted entities."""

from dataclasses import dataclass, field
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import verify_chain
from instate.core.models import Decision, EntityState, Event, TERMINAL_STATUSES
from instate.core.projection import CONTACT_EVENT_TYPES, RETRY_EVENT_TYPES

# Violation scanner mirrors the gate caps.
RETRY_LIMIT = 3
RETRY_WINDOW = timedelta(days=7)
CONTACT_LIMIT = 2
CONTACT_WINDOW = timedelta(hours=24)

MONEY_RECOVERED_EVENTS = {"RetrySucceeded", "PaymentRecovered", "PromiseHonored", "HumanResolved"}


@dataclass
class RunMetrics:
    """One agent's run over one batch, measured."""

    decisions: int = 0
    zero_llm_decisions: int = 0
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0
    gross_recovered_minor: int = 0
    reversed_minor: int = 0
    retry_violations: int = 0
    contact_violations: int = 0
    entities: int = 0
    events: int = 0
    chain_breaks: int = 0
    entities_checked: int = 0
    # Recovery-by-value companions: how fast, and how much is still open.
    median_ttr_hours: float | None = None
    at_risk_minor: int = 0
    # Attempted = entities with a real attempt; excludes never-attempted.
    attempted_entities: int = 0
    recovered_entities: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def net_recovered_minor(self) -> int:
        return self.gross_recovered_minor - self.reversed_minor

    @property
    def compliance_violations(self) -> int:
        return self.retry_violations + self.contact_violations

    @property
    def zero_llm_share(self) -> float:
        return self.zero_llm_decisions / self.decisions if self.decisions else 0.0

    @property
    def avg_input_tokens(self) -> float:
        # Amortized per decision; zero-LLM counts as 0.
        return self.llm_input_tokens / self.decisions if self.decisions else 0.0

    @property
    def avg_input_tokens_modeled(self) -> float:
        modeled = self.decisions - self.zero_llm_decisions
        return self.llm_input_tokens / modeled if modeled else 0.0

    @property
    def attempted_recovery_rate(self) -> float:
        return self.recovered_entities / self.attempted_entities if self.attempted_entities else 0.0

    @property
    def chain_verified(self) -> bool:
        return self.chain_breaks == 0


# ---------------------------------------------------------------------------
# Money — gross and net
# ---------------------------------------------------------------------------


async def money_flow(session: AsyncSession, *, merchant_id: UUID | None = None) -> tuple[int, int]:
    """(gross_recovered_minor, reversed_minor) from event payloads; RecoveryReversed is netted out."""
    q = select(Event.event_type, Event.payload).where(
        Event.event_type.in_(MONEY_RECOVERED_EVENTS | {"RecoveryReversed"})
    )
    if merchant_id is not None:
        q = q.where(Event.merchant_id == merchant_id)

    gross = 0
    reversed_total = 0
    for event_type, payload in (await session.execute(q)).all():
        amount = (payload or {}).get("amount_minor") or 0
        if event_type == "RecoveryReversed":
            reversed_total += amount
        else:
            gross += amount
    return gross, reversed_total


# ---------------------------------------------------------------------------
# Compliance — a chronological violation scan over the ledger
# ---------------------------------------------------------------------------


async def scan_compliance(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
) -> tuple[int, int]:
    """(retry_violations, contact_violations): attempts landing while already at cap."""
    q = select(Event).order_by(Event.merchant_id.asc(), Event.entity_id.asc(), Event.id.asc())
    if merchant_id is not None:
        q = q.where(Event.merchant_id == merchant_id)

    retry_violations = 0
    contact_violations = 0
    retry_stamps: list = []
    contact_stamps: list = []
    current_key = None

    for event in (await session.execute(q)).scalars():
        key = (event.merchant_id, event.entity_id)
        if key != current_key:
            current_key = key
            retry_stamps, contact_stamps = [], []

        occurred = event.occurred_at
        if event.event_type in RETRY_EVENT_TYPES:
            retry_stamps = [t for t in retry_stamps if t > occurred - RETRY_WINDOW]
            if len(retry_stamps) >= RETRY_LIMIT:
                retry_violations += 1
            retry_stamps.append(occurred)
        elif event.event_type in CONTACT_EVENT_TYPES:
            contact_stamps = [t for t in contact_stamps if t > occurred - CONTACT_WINDOW]
            if len(contact_stamps) >= CONTACT_LIMIT:
                contact_violations += 1
            contact_stamps.append(occurred)

    return retry_violations, contact_violations


# ---------------------------------------------------------------------------
# Time-to-recovery + at-risk revenue
# ---------------------------------------------------------------------------


async def time_to_recovery_hours(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
) -> list[float]:
    """Hours from first failure to first recovery, recovered entities only."""
    q = select(Event.merchant_id, Event.entity_id, Event.event_type, Event.occurred_at)
    if merchant_id is not None:
        q = q.where(Event.merchant_id == merchant_id)
    q = q.order_by(Event.merchant_id.asc(), Event.entity_id.asc(), Event.occurred_at.asc())

    first_failure: dict[tuple, object] = {}
    ttrs: list[float] = []
    for mid, eid, etype, occurred in (await session.execute(q)).all():
        key = (mid, eid)
        if etype == "PaymentFailed" and key not in first_failure:
            first_failure[key] = occurred
        elif etype in MONEY_RECOVERED_EVENTS and key in first_failure:
            delta = (occurred - first_failure[key]).total_seconds() / 3600
            if delta >= 0:
                ttrs.append(delta)
                del first_failure[key]  # first recovery only — no double counting
    return ttrs


async def at_risk_revenue(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
) -> int:
    """Sum at-risk over open (non-terminal) entities."""
    q = select(EntityState.amount_at_risk_minor).where(EntityState.status.notin_(TERMINAL_STATUSES))
    if merchant_id is not None:
        q = q.where(EntityState.merchant_id == merchant_id)
    total = 0
    for (amount,) in (await session.execute(q)).all():
        total += amount or 0
    return total


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


# ---------------------------------------------------------------------------
# Link conversion by variant
# ---------------------------------------------------------------------------


async def attempted_recovery(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
) -> tuple[int, int]:
    """(attempted, recovered) entities; never-attempted stay out of the denominator."""
    q = select(Event.entity_id, Event.event_type, Event.occurred_at)
    if merchant_id is not None:
        q = q.where(Event.merchant_id == merchant_id)
    q = q.order_by(Event.occurred_at.asc())
    attempted: set = set()
    recovered: set = set()
    for eid, etype, _ in (await session.execute(q)).all():
        if etype in RETRY_EVENT_TYPES:
            attempted.add(eid)
        elif etype in MONEY_RECOVERED_EVENTS and eid in attempted:
            recovered.add(eid)
    return len(attempted), len(recovered)


async def link_conversion_by_variant(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
) -> dict[str, dict[str, int]]:
    """{variant: {sent, converted}} from PaymentLinkSent payloads."""
    q = select(Event.payload).where(Event.event_type == "PaymentLinkSent")
    if merchant_id is not None:
        q = q.where(Event.merchant_id == merchant_id)

    out: dict[str, dict[str, int]] = {}
    for (payload,) in (await session.execute(q)).all():
        payload = payload or {}
        variant = payload.get("variant")
        if not isinstance(variant, str):
            continue
        bucket = out.setdefault(variant, {"sent": 0, "converted": 0})
        bucket["sent"] += 1
        if payload.get("converted") is True:
            bucket["converted"] += 1
    return out


# ---------------------------------------------------------------------------
# The full measurement
# ---------------------------------------------------------------------------


async def compute_run_metrics(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
    max_chain_entities: int = 200,
) -> RunMetrics:
    """Measure a run from its ledger."""
    m = RunMetrics()

    # Decisions + token economics (zero-token paths have tokens_in NULL)
    dq = select(Decision.tokens_in, Decision.tokens_out)
    if merchant_id is not None:
        dq = dq.where(Decision.merchant_id == merchant_id)
    for tokens_in, tokens_out in (await session.execute(dq)).all():
        m.decisions += 1
        if tokens_in is None or tokens_in == 0:
            m.zero_llm_decisions += 1
        else:
            m.llm_input_tokens += tokens_in or 0
            m.llm_output_tokens += tokens_out or 0

    m.gross_recovered_minor, m.reversed_minor = await money_flow(session, merchant_id=merchant_id)
    m.median_ttr_hours = _median(await time_to_recovery_hours(session, merchant_id=merchant_id))
    m.at_risk_minor = await at_risk_revenue(session, merchant_id=merchant_id)
    m.retry_violations, m.contact_violations = await scan_compliance(
        session, merchant_id=merchant_id
    )
    m.attempted_entities, m.recovered_entities = await attempted_recovery(
        session, merchant_id=merchant_id
    )
    eq = select(func.count(func.distinct(Event.entity_id)), func.count(Event.id))
    if merchant_id is not None:
        eq = eq.where(Event.merchant_id == merchant_id)
    entities, events = (await session.execute(eq)).one()
    m.entities, m.events = entities, events

    # Chain integrity (bounded walk).
    idq = select(Event.merchant_id, Event.entity_id).group_by(Event.merchant_id, Event.entity_id)
    if merchant_id is not None:
        idq = idq.where(Event.merchant_id == merchant_id)
    idq = idq.limit(max_chain_entities)
    for mid, eid in (await session.execute(idq)).all():
        result = await verify_chain(session, mid, eid)
        m.entities_checked += 1
        if not result.verified:
            m.chain_breaks += 1
            m.notes.append(f"chain broken: {eid}: {result.error}")

    return m


# ---------------------------------------------------------------------------
# The comparison table
# ---------------------------------------------------------------------------


def format_comparison(baseline: RunMetrics, instate: RunMetrics) -> str:
    """Comparison table for baseline vs instate metrics."""
    header = f"{'metric':<34}{'baseline':>14}{'instate':>14}"
    line = "-" * len(header)

    def row(label: str, b, i) -> str:
        return f"{label:<34}{b:>14}{i:>14}"

    def rupees(minor: int) -> str:
        return f"₹{minor / 100:,.0f}"

    def hours(value: float | None) -> str:
        return f"{value:,.1f}h" if value is not None else "—"

    # Lift = instate net minus baseline net.
    lift_minor = instate.net_recovered_minor - baseline.net_recovered_minor

    rows = [
        row(
            "net money recovered",
            rupees(baseline.net_recovered_minor),
            rupees(instate.net_recovered_minor),
        ),
        row("lift over baseline (₹)", "—", rupees(lift_minor)),
        row(
            "  gross recovered",
            rupees(baseline.gross_recovered_minor),
            rupees(instate.gross_recovered_minor),
        ),
        row(
            "  reversed (refunds/chargebacks)",
            rupees(baseline.reversed_minor),
            rupees(instate.reversed_minor),
        ),
        row("retry-ceiling violations", baseline.retry_violations, instate.retry_violations),
        row("contact-cap violations", baseline.contact_violations, instate.contact_violations),
        row(
            "attempted recovery rate",
            f"{baseline.attempted_recovery_rate:.0%} ({baseline.recovered_entities}/{baseline.attempted_entities})",
            f"{instate.attempted_recovery_rate:.0%} ({instate.recovered_entities}/{instate.attempted_entities})",
        ),
        row(
            "% decisions with zero LLM calls",
            f"{baseline.zero_llm_share:.0%}",
            f"{instate.zero_llm_share:.0%}",
        ),
        row(
            "avg LLM input tokens / decision",
            f"{baseline.avg_input_tokens:,.0f}",
            f"{instate.avg_input_tokens:,.0f}",
        ),
        row("total LLM output tokens", baseline.llm_output_tokens, instate.llm_output_tokens),
        row(
            "median time-to-recovery",
            hours(baseline.median_ttr_hours),
            hours(instate.median_ttr_hours),
        ),
        row(
            "at-risk revenue (open)",
            rupees(baseline.at_risk_minor),
            rupees(instate.at_risk_minor),
        ),
        row(
            "hash chain verified",
            "yes" if baseline.chain_verified else "NO",
            "yes" if instate.chain_verified else "NO",
        ),
        row(
            "entities / events",
            f"{baseline.entities}/{baseline.events}",
            f"{instate.entities}/{instate.events}",
        ),
    ]
    return "\n".join([header, line, *rows, line])
