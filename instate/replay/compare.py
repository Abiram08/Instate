"""Comparison runner: identical batch through both agents in isolated DBs.
Contract: same scripted model, same gateway, same seed; only context differs
(raw dump vs bounded digest)."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from instate.adapters.razorpay import GatewayResponse
from instate.agent.decide import drain_pending
from instate.agent.execute import run_due_scheduled
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_CHECK_METHOD_UPDATED,
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_BACKUP_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
)
from instate.replay.baseline import StatelessBaselineAgent
from instate.replay.metrics import compute_run_metrics, format_comparison
from instate.seed.generate import generate_failure_batch, seed_history

_CODE_TO_CAUSE = {
    "insufficient_funds": "insufficient_funds",
    "GATEWAY_TIMEOUT": "network_timeout",
    "NETWORK_ERROR": "network_timeout",
    "CARD_EXPIRED": "card_expired",
    "FRAUD_DETECTED": "fraud_block",
    "MANDATE_INACTIVE": "mandate_inactive",
    "customer_cancelled": "customer_initiated",
}


# ---------------------------------------------------------------------------
# Shared scripted model
# ---------------------------------------------------------------------------


class SharedScriptedReasoner:
    """Shared scripted model; output depends on context (digest vs raw dump)."""

    model_name = "scripted-shared-model"
    last_usage = (900, 60)

    def __init__(self):
        self.contexts: list[dict] = []

    async def propose(self, context: dict) -> dict | None:
        self.contexts.append(context)
        cause = self._infer_cause(context)
        if cause in ("fraud_block", "mandate_inactive", "UNKNOWN"):
            proposal = ACTION_ESCALATE_HUMAN
        elif cause == "card_expired":
            proposal = ACTION_SEND_PAYMENT_LINK
        elif cause == "network_timeout":
            proposal = ACTION_RETRY_NOW
        elif cause == "insufficient_funds" and "policy_version" in context:
            proposal = ACTION_RETRY_SCHEDULED  # payday-aligned, per policy framing
        else:
            proposal = ACTION_RETRY_NOW  # the naive default
        return {
            "action": proposal,
            "timing": "IMMEDIATE" if proposal == ACTION_RETRY_NOW else "T_PLUS_48H",
            "rationale": "scripted shared model",
            "confidence": 0.9,
        }

    @staticmethod
    def _infer_cause(context: dict) -> str | None:
        if "policy_version" in context:  # Instate digest
            return context.get("root_cause")
        for event in reversed(context.get("history", [])):  # baseline raw dump
            code = (event.get("payload") or {}).get("failure_code")
            if code:
                return _CODE_TO_CAUSE.get(code, "UNKNOWN")
        return None


# ---------------------------------------------------------------------------
# Realistic gateway
# ---------------------------------------------------------------------------


class RealisticGateway:
    """Same gateway for both arms: <48h insufficient-funds retry fails;
    hard-decline fails until method changes; 4th+ attempt fails; links succeed."""

    # Codes where variant B converts and A does not; both arms share seeds.
    B_LIFTS_THESE_CODES = {"card_expired", "CARD_EXPIRED"}

    def __init__(self, amount_minor: int = 49900, link_variant: str = "A"):
        self.amount_minor = amount_minor
        self.link_variant = link_variant
        self.now = datetime.now(UTC)
        self.failures: dict[str, tuple[str, datetime]] = {}
        self.attempts: dict[str, int] = {}
        self.method_changed: set[str] = set()
        self.calls: list[dict] = []

    def note_failure(self, entity_id: str, code: str | None, at: datetime) -> None:
        self.failures[entity_id] = (code or "UNKNOWN", at)

    def _retry_attempt(self, entity_id: str) -> GatewayResponse:
        code, failed_at = self.failures.get(entity_id, ("UNKNOWN", self.now))
        if code in (
            "card_expired",
            "fraud_block",
            "mandate_inactive",
            "lost_card",
            "stolen_card",
        ):
            if entity_id not in self.method_changed:
                return GatewayResponse("failed", detail="hard decline: method not updated")
        if code == "insufficient_funds" and self.now - failed_at < timedelta(hours=48):
            return GatewayResponse("failed", detail="insufficient funds — no payday yet")
        count = self.attempts.get(entity_id, 0) + 1
        if count > 3:
            return GatewayResponse("failed", detail="credential attempts exhausted")
        self.attempts[entity_id] = count
        return GatewayResponse(
            "completed", provider_ref=f"pay_{len(self.calls)}", amount_minor=self.amount_minor
        )

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append(
            {"action": action, "entity_id": entity_id, "idempotency_key": idempotency_key}
        )
        if action in ("SEND_PAYMENT_LINK", "REQUEST_PAYMENT_METHOD", "UPDATE_MANDATE"):
            code, _ = self.failures.get(entity_id, ("UNKNOWN", self.now))
            converted = self.link_variant == "B" or code not in self.B_LIFTS_THESE_CODES
            return GatewayResponse(
                "completed",
                provider_ref=f"link_{len(self.calls)}",
                data={"converted": converted, "variant": self.link_variant},
            )
        if action == ACTION_RETRY_NOW:
            return self._retry_attempt(entity_id)
        if action == ACTION_RETRY_BACKUP_METHOD:
            # A different instrument: the dead primary's block is
            # irrelevant — same budget accounting as a normal retry.
            code, _ = self.failures.get(entity_id, ("UNKNOWN", self.now))
            if code in ("fraud_block",):
                return GatewayResponse("failed", detail="fraud: backup blocked too")
            return self._retry_attempt(entity_id)
        if action == ACTION_CHECK_METHOD_UPDATED:
            return GatewayResponse(
                "completed",
                provider_ref=f"probe_{len(self.calls)}",
                data={"method_updated": entity_id in self.method_changed},
            )
        return GatewayResponse("failed", detail=f"unsupported {action}")

    async def lookup(self, idempotency_key: str):
        return None


# ---------------------------------------------------------------------------
# Isolated setup
# ---------------------------------------------------------------------------


@dataclass
class _Setup:
    engine: object
    factory: async_sessionmaker
    session: AsyncSession
    gateway: Any  # RealisticGateway (stand-in) or RazorpayGateway (test-mode)
    merchant_id: UUID
    batch: list = field(default_factory=list)


def _note_failure(setup: _Setup, entity_id: str, code: str | None, at: datetime) -> None:
    """Stand-in bookkeeping; real gateways have no scripted failure table."""
    note = getattr(setup.gateway, "note_failure", None)
    if callable(note):
        note(entity_id, code, at)


def _set_now(setup: _Setup, at: datetime) -> None:
    if hasattr(setup.gateway, "now"):
        setup.gateway.now = at


async def _fresh_setup(
    seed: int, entities: int, now: datetime, gateway: Any | None = None
) -> _Setup:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    # Build schema directly for full isolation (bypasses database.py singleton).
    from instate.core.models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = factory()
    merchant_id = uuid4()

    from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
    from instate.core.policy import seed_default_policy

    await seed_default_policy(session)
    await seed_default_diagnosis(session)
    await seed_default_taxonomy(session)
    await seed_history(session, merchant_id=merchant_id, entities=entities, seed=seed, now=now)
    await session.commit()
    return _Setup(
        engine=engine,
        factory=factory,
        session=session,
        gateway=gateway or RealisticGateway(),
        merchant_id=merchant_id,
    )


# Batch covers gated cases: at-ceiling, open with contacts, fresh root causes.
BATCH_ENTITIES = ["sub_000", "sub_004", "sub_008", "fresh_b", "fresh_c", "fresh_d"]
BATCH_CODES = [
    "insufficient_funds",
    "insufficient_funds",
    "insufficient_funds",
    "GATEWAY_TIMEOUT",
    "CARD_EXPIRED",
    "insufficient_funds",
]


async def _fatten_history(setup: _Setup, entity_id: str, n: int = 10) -> None:
    """Add a long follow-up history (10 contacts), identically on both sides."""
    from instate.core.projection import fold_events

    now = datetime.now(UTC)
    for i in range(n):
        await record_event(
            setup.session,
            merchant_id=setup.merchant_id,
            entity_id=entity_id,
            entity_type="subscription",
            event_type="CustomerContacted",
            occurred_at=now - timedelta(days=25 - i * 2),
            payload={"channel": "email", "note": f"follow-up {i}"},
            source_event_id=f"{entity_id}_fat_{i}",
        )
    await setup.session.commit()
    await fold_events(setup.session)
    await setup.session.commit()


class _VariantReasoner:
    """Stamps variant onto proposals for per-variant conversion measurement."""

    def __init__(self, inner, variant: str):
        self._inner = inner
        self._variant = variant

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def propose(self, context: dict) -> dict | None:
        raw = await self._inner.propose(context)
        if isinstance(raw, dict):
            return {**raw, "variant": self._variant}
        return raw


async def run_comparison(
    *,
    entities: int = 10,
    seed: int = 42,
    now: datetime | None = None,
    due_scan_after: timedelta = timedelta(hours=72),
    link_variant: str = "A",
) -> dict:
    """Run the identical batch through both agents; return {baseline, instate, table}."""
    now = now or datetime.now(UTC)

    base = await _fresh_setup(seed, entities, now)
    inst = await _fresh_setup(seed, entities, now)
    for setup in (base, inst):
        if hasattr(setup.gateway, "link_variant"):
            setup.gateway.link_variant = link_variant

    # Identical fattening on both sides.
    for eid in ("sub_000", "sub_004"):
        await _fatten_history(base, eid)
        await _fatten_history(inst, eid)

    batch_base = await generate_failure_batch(
        base.session,
        merchant_id=base.merchant_id,
        entity_ids=BATCH_ENTITIES,
        codes=BATCH_CODES,
        now=now,
        prefix="batch",
    )
    batch_inst = await generate_failure_batch(
        inst.session,
        merchant_id=inst.merchant_id,
        entity_ids=BATCH_ENTITIES,
        codes=BATCH_CODES,
        now=now,
        prefix="batch",
    )

    for setup, batch in ((base, batch_base), (inst, batch_inst)):
        for event in batch:
            code = (event.payload or {}).get("failure_code")
            _note_failure(setup, event.entity_id, code, now)

    # Baseline: no gates, full raw context.
    baseline_agent = StatelessBaselineAgent(
        _VariantReasoner(SharedScriptedReasoner(), link_variant), base.gateway
    )
    baseline_results = [
        await baseline_agent.process_failure(base.session, event=event, now=now)
        for event in batch_base
    ]
    await base.session.commit()

    # Instate: same model through the gated pipeline.
    instate_reasoner = _VariantReasoner(SharedScriptedReasoner(), link_variant)
    instate_results = await drain_pending(
        inst.session, reasoner=instate_reasoner, gateway=inst.gateway, now=now
    )
    await inst.session.commit()

    # Advance to payday; run due scheduled retries on both sides.
    later = now + due_scan_after
    for setup in (base, inst):
        _set_now(setup, later)
        await run_due_scheduled(setup.session, gateway=setup.gateway, now=later)
        await setup.session.commit()

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
        "instate_reasoner": instate_reasoner,
        "table": table,
    }


async def run_ab_test(
    *,
    entities: int = 10,
    seed: int = 42,
    now: datetime | None = None,
) -> dict:
    """A/B link wording (A vs B) on identical seeded batches; returns conversion and table."""
    now = now or datetime.now(UTC)
    conv = {}
    for variant in ("A", "B"):
        arm = await _conversion_for_run(entities=entities, seed=seed, now=now, link_variant=variant)
        conv[variant] = arm.get(variant, {"sent": 0, "converted": 0})

    table_lines = [
        f"{'variant':<12}{'links sent':>12}{'converted':>12}{'rate':>10}",
        "-" * 46,
    ]
    for variant in ("A", "B"):
        sent = conv[variant].get("sent", 0)
        conv_n = conv[variant].get("converted", 0)
        rate = f"{conv_n / sent:.0%}" if sent else "—"
        table_lines.append(f"{variant:<12}{sent:>12}{conv_n:>12}{rate:>10}")
    table_lines.append("-" * 46)
    winner = "B" if conv["B"].get("converted", 0) > conv["A"].get("converted", 0) else "A"
    table_lines.append(
        "winner: variant " + winner + " (scripted lift — production reads real link conversions)"
    )
    return {"conversion": conv, "table": "\n".join(table_lines)}


async def _conversion_for_run(
    *,
    entities: int,
    seed: int,
    now: datetime,
    link_variant: str,
) -> dict:
    """One gated-agent run returning link conversion (separate session)."""
    from instate.replay.metrics import link_conversion_by_variant

    inst = await _fresh_setup(seed, entities, now)
    if hasattr(inst.gateway, "link_variant"):
        inst.gateway.link_variant = link_variant
    for eid in ("sub_000", "sub_004"):
        await _fatten_history(inst, eid)

    batch = await generate_failure_batch(
        inst.session,
        merchant_id=inst.merchant_id,
        entity_ids=BATCH_ENTITIES,
        codes=BATCH_CODES,
        now=now,
        prefix="batch",
    )
    for event in batch:
        code = (event.payload or {}).get("failure_code")
        _note_failure(inst, event.entity_id, code, now)

    reasoner = _VariantReasoner(SharedScriptedReasoner(), link_variant)
    await drain_pending(inst.session, reasoner=reasoner, gateway=inst.gateway, now=now)
    await inst.session.commit()
    later = now + timedelta(hours=72)
    _set_now(inst, later)
    await run_due_scheduled(inst.session, gateway=inst.gateway, now=later)
    await inst.session.commit()

    conv = await link_conversion_by_variant(inst.session, merchant_id=inst.merchant_id)
    await inst.session.close()
    await inst.engine.dispose()
    return conv
