"""Instate metrics — every demo claim computed from the ledger (§11).

Nothing here trusts an agent's self-report: money comes from recovered /
reversed event payloads, compliance from a chronological violation scan,
token economics from the decisions table, integrity from the hash chain.
The same functions measure both agents — that's what makes the
comparison a measurement instead of a claim.
"""

from dataclasses import dataclass, field
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import verify_chain
from instate.core.models import Decision, Event
from instate.core.projection import CONTACT_EVENT_TYPES, RETRY_EVENT_TYPES

# The default policy caps (§3) — the violation scanner replays the ledger
# against exactly the rules the gates enforce.
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
        # Amortized cost per decision (zero-LLM counts as 0) — the
        # headline a finance team actually cares about.
        return self.llm_input_tokens / self.decisions if self.decisions else 0.0

    @property
    def avg_input_tokens_modeled(self) -> float:
        modeled = self.decisions - self.zero_llm_decisions
        return self.llm_input_tokens / modeled if modeled else 0.0

    @property
    def chain_verified(self) -> bool:
        return self.chain_breaks == 0


# ---------------------------------------------------------------------------
# Money — gross and net (§11: the headline must not overstate)
# ---------------------------------------------------------------------------


async def money_flow(session: AsyncSession, *, merchant_id: UUID | None = None) -> tuple[int, int]:
    """(gross_recovered_minor, reversed_minor) from event payloads.

    Refunds/chargebacks after a recovery are RecoveryReversed events —
    netting them out is what stops the headline from overstating.
    """
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
    """(retry_violations, contact_violations).

    Replays every entity's timeline in order, maintaining the same
    windowed counters the gates enforce, and counts attempts that landed
    while already at/over a cap:
      - a RetryAttempted when retry_count_7d is already >= 3
      - a customer contact when contacts_24h is already >= 2
    This catches exactly the behavior a stateless agent exhibits and the
    gated agent cannot have.
    """
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
# The full measurement
# ---------------------------------------------------------------------------


async def compute_run_metrics(
    session: AsyncSession,
    *,
    merchant_id: UUID | None = None,
    max_chain_entities: int = 200,
) -> RunMetrics:
    """Measure everything §11 promises, from one session's ledger."""
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

    # Money
    m.gross_recovered_minor, m.reversed_minor = await money_flow(session, merchant_id=merchant_id)

    # Compliance
    m.retry_violations, m.contact_violations = await scan_compliance(
        session, merchant_id=merchant_id
    )

    # Volume
    eq = select(func.count(func.distinct(Event.entity_id)), func.count(Event.id))
    if merchant_id is not None:
        eq = eq.where(Event.merchant_id == merchant_id)
    entities, events = (await session.execute(eq)).one()
    m.entities, m.events = entities, events

    # Chain integrity — walk every entity (bounded for big runs)
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
    """The demo's money shot: one batch, two agents, one honest table."""
    header = f"{'metric':<34}{'baseline':>14}{'instate':>14}"
    line = "-" * len(header)

    def row(label: str, b, i) -> str:
        return f"{label:<34}{b:>14}{i:>14}"

    def rupees(minor: int) -> str:
        return f"₹{minor / 100:,.0f}"

    rows = [
        row(
            "net money recovered",
            rupees(baseline.net_recovered_minor),
            rupees(instate.net_recovered_minor),
        ),
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
