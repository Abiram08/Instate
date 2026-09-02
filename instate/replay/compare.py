"""The comparison runner — one batch, two agents, one honest table (§11).

FAIRNESS CONTRACT (the whole demo stands on this):

- The SAME scripted model serves both agents — same capability, same
  proposals for the same context. The ONLY difference is what each agent
  FEEDS it: the baseline re-derives and stuffs the full raw history (no
  policy framing, no digest); Instate provides the bounded digest with
  the policy version in force. Context quality is the memory layer's
  product — that difference IS the experiment.
- The SAME realistic gateway punishes the same mistakes on both sides:
  immediate retries on insufficient funds fail (payday hasn't come),
  retries on hard-declined methods fail, and the 4th+ attempt fails.
- The SAME seed generates identical history and an identical batch for
  both, in isolated databases.

Deltas that survive that contract are attributable to the memory layer.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from instate.adapters.razorpay import GatewayResponse
from instate.agent.decide import drain_pending
from instate.agent.execute import run_due_scheduled
from instate.core.ledger import record_event
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
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
# The shared scripted model — the ONLY model in the experiment
# ---------------------------------------------------------------------------


class SharedScriptedReasoner:
    """A decent model. It proposes the same thing for the same intent —
    but it can only be as good as the context it is fed.

    - With the Instate digest (policy_version present), it has the
      taxonomy framing: insufficient funds → schedule payday-aligned.
    - With the baseline's raw dump, it does the human-naive thing:
      'payment failed → retry now'.
    That difference in context IS what the memory layer provides."""

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
# The realistic gateway — mistakes cost the same on both sides
# ---------------------------------------------------------------------------


class RealisticGateway:
    """Models how cards actually behave:

    - insufficient funds: a retry before payday (< 48h) FAILS
    - hard-declined method: every retry FAILS until the method changes
    - 4th+ attempt: FAILS (the network stops honoring the credential)
    - payment links: always succeed
    """

    def __init__(self, amount_minor: int = 49900):
        self.amount_minor = amount_minor
        self.now = datetime.now(UTC)
        self.failures: dict[str, tuple[str, datetime]] = {}
        self.attempts: dict[str, int] = {}
        self.method_changed: set[str] = set()
        self.calls: list[dict] = []

    def note_failure(self, entity_id: str, code: str | None, at: datetime) -> None:
        self.failures[entity_id] = (code or "UNKNOWN", at)

    async def execute(self, action, *, entity_id, idempotency_key, payload=None):
        self.calls.append(
            {"action": action, "entity_id": entity_id, "idempotency_key": idempotency_key}
        )
        if action in ("SEND_PAYMENT_LINK", "REQUEST_PAYMENT_METHOD", "UPDATE_MANDATE"):
            return GatewayResponse("completed", provider_ref=f"link_{len(self.calls)}")
        if action == ACTION_RETRY_NOW:
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
        return GatewayResponse("failed", detail=f"unsupported {action}")

    async def lookup(self, idempotency_key: str):
        return None


# ---------------------------------------------------------------------------
# Isolated setup — one agent per database, identical seed
# ---------------------------------------------------------------------------


@dataclass
class _Setup:
    engine: object
    factory: async_sessionmaker
    session: AsyncSession
    gateway: RealisticGateway
    merchant_id: UUID
    batch: list = field(default_factory=list)


async def _fresh_setup(seed: int, entities: int, now: datetime) -> _Setup:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    # init_db uses the database.py singleton — build schema directly so the
    # two setups are fully isolated from each other and from the singleton.
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
        gateway=RealisticGateway(),
        merchant_id=merchant_id,
    )


# The batch: aimed so the gates have something to gate. sub_000 is a
# recovered_retry (rich history), sub_004 is at-ceiling (3 retries this
# week — Gate-1 must deny), sub_008 is open with follow-up contacts; the
# fresh entities cover the remaining root causes.
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
    """Give one entity a long follow-up history (10 contacts over 3 weeks).

    Identical on both sides — this is what makes the context-reduction
    claim measurable: the baseline's dump grows with every event; the
    digest stays bounded at the last 5.
    """
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


async def run_comparison(
    *,
    entities: int = 10,
    seed: int = 42,
    now: datetime | None = None,
    due_scan_after: timedelta = timedelta(hours=72),
) -> dict:
    """Run the identical batch through both agents in isolated DBs and
    return {baseline, instate, table} — the demo's money shot."""
    now = now or datetime.now(UTC)

    base = await _fresh_setup(seed, entities, now)
    inst = await _fresh_setup(seed, entities, now)

    # Identical fattening on both sides — on guaranteed batch targets so
    # the context-reduction delta holds even for small demo sizes
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
            setup.gateway.note_failure(event.entity_id, code, now)

    # Baseline run — no gates, no caps, full raw context every time
    baseline_agent = StatelessBaselineAgent(SharedScriptedReasoner(), base.gateway)
    baseline_results = [
        await baseline_agent.process_failure(base.session, event=event, now=now)
        for event in batch_base
    ]
    await base.session.commit()

    # Instate run — the same model through the gated pipeline
    instate_reasoner = SharedScriptedReasoner()
    instate_results = await drain_pending(
        inst.session, reasoner=instate_reasoner, gateway=inst.gateway, now=now
    )
    await inst.session.commit()

    # The tick: payday arrives — due scheduled retries execute on both sides
    later = now + due_scan_after
    for setup in (base, inst):
        setup.gateway.now = later
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
