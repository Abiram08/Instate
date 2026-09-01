"""Instate models — L0 (events ledger), L1 (entity_state projection),
L2 (policy) and the decisions audit object.

Design notes (architecture.md §1-6):
- L0 is append-only: no UPDATE, no DELETE, ever.
- The hash chain is per-entity: prev_hash links to the previous event
  for the SAME (merchant_id, entity_id), not a global chain.
- Bi-temporal: occurred_at (valid time) vs recorded_at (transaction time).
- Windowed counters are NOT in L1 — they're computed at gate-check
  as indexed L0 counts (see projection.py:get_windowed_count).
- L2 policy is declarative versioned rows; every rule cites its source.
- A gate never returns a bare boolean — it returns the reason chain.
"""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    Text,
    TypeDecorator,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from instate.core.crypto import EncryptedJSONType

# BigInteger PK that works on both backends:
# PostgreSQL: BIGSERIAL, SQLite: INTEGER (autoincrement requires INTEGER PRIMARY KEY)
BigIntPK = BigInteger().with_variant(Integer(), "sqlite")


# ---------------------------------------------------------------------------
# Cross-backend types (PostgreSQL in production, SQLite in dev/test)
# ---------------------------------------------------------------------------


class JSONType(TypeDecorator[dict[str, Any]]):
    """JSONB on PostgreSQL, JSON text on SQLite. Transparent to the caller."""

    impl = Text  # type: ignore[assignment]
    cache_ok = True

    def load_dialect_impl(self, dialect, **kwargs):  # type: ignore[no-untyped-def]
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value  # asyncpg handles dict → JSONB natively
        return json.dumps(value)

    def process_result_value(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if isinstance(value, dict):
            return value  # PostgreSQL returns dict already
        return json.loads(value)


class UTCTimestamp(TypeDecorator[datetime]):
    """Store timestamps as UTC. TIMESTAMPTZ on PostgreSQL, DATETIME on SQLite."""

    impl = DateTime  # type: ignore[assignment]
    cache_ok = True

    def process_bind_param(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


# BigInteger primary key works on both backends
# (SQLite maps autoincrement BIGINT to INTEGER internally; SQLAlchemy handles this)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# L0: The Ledger (§1 of architecture.md)
# ---------------------------------------------------------------------------


class Event(Base):
    """An immutable, hash-chained event in the ledger.

    Append-only. The application NEVER issues UPDATE or DELETE on this table.
    """

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(UTCTimestamp, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )
    # Encrypted at rest when INSTATE_ENCRYPTION_KEY is set, transparent
    # otherwise (behaves exactly like JSONType). The chain hashes the
    # PLAINTEXT payload_hash, so encryption can never break verification.
    payload: Mapped[dict[str, Any] | None] = mapped_column(EncryptedJSONType(), nullable=True)
    payload_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_event_id: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)
    decision_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    prev_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    def __repr__(self) -> str:
        return (
            f"Event(id={self.id}, entity={self.entity_id!r}, "
            f"type={self.event_type!r}, occurred_at={self.occurred_at})"
        )


Index("idx_events_merchant_entity_occurred", Event.merchant_id, Event.entity_id, Event.occurred_at)
Index("idx_events_recorded_at", Event.recorded_at)


# ---------------------------------------------------------------------------
# L1: The Projection (§2 of architecture.md)
# ---------------------------------------------------------------------------

# State machine positions (architecture.md §6):
#   ACTIVE → DIAGNOSED → RETRY_SCHEDULED → AWAITING_PROMISE → ESCALATED → RECOVERED
#                                                                      ↘ WRITTEN_OFF
STATUS_ACTIVE = "ACTIVE"
STATUS_DIAGNOSED = "DIAGNOSED"
STATUS_RETRY_SCHEDULED = "RETRY_SCHEDULED"
STATUS_AWAITING_PROMISE = "AWAITING_PROMISE"
STATUS_ESCALATED = "ESCALATED"
STATUS_RECOVERED = "RECOVERED"
STATUS_WRITTEN_OFF = "WRITTEN_OFF"

VALID_STATUSES = {
    STATUS_ACTIVE,
    STATUS_DIAGNOSED,
    STATUS_RETRY_SCHEDULED,
    STATUS_AWAITING_PROMISE,
    STATUS_ESCALATED,
    STATUS_RECOVERED,
    STATUS_WRITTEN_OFF,
}


class EntityState(Base):
    """Derived state for an entity — a pure fold over L0.

    Holds only what folds cleanly: the state-machine position and
    point-in-time scalars. Windowed counters (retry_count_7d, contacts_24h)
    are deliberately NOT here — they're computed at gate-check as
    indexed L0 counts (projection.py:get_windowed_count).
    """

    __tablename__ = "entity_state"

    merchant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    entity_id: Mapped[str] = mapped_column(Text, primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False, default="payment")
    status: Mapped[str] = mapped_column(Text, nullable=False, default=STATUS_ACTIVE)
    last_contact_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    last_failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    open_ptp_due_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    amount_at_risk_minor: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    def __repr__(self) -> str:
        return (
            f"EntityState(entity={self.entity_id!r}, status={self.status!r}, "
            f"last_event_id={self.last_event_id})"
        )


def new_merchant_id() -> UUID:
    """Generate a merchant UUID (for seeding/testing)."""
    return uuid4()


# ---------------------------------------------------------------------------
# L2: Policy (§3 of architecture.md)
# ---------------------------------------------------------------------------

# Gate verdicts. A policy row can only DENY or REQUIRE_HUMAN — it can never
# *force* an action. ALLOW is computed (no rule fired), never stored in L2.
VERDICT_DENY = "DENY"
VERDICT_REQUIRE_HUMAN = "REQUIRE_HUMAN"
VERDICT_ALLOW = "ALLOW"
GATE_VERDICTS = {VERDICT_DENY, VERDICT_REQUIRE_HUMAN}


class Policy(Base):
    """A declarative, versioned policy rule — what is allowed (§3).

    Rows, not code. Every rule cites its source (regulation or internal
    policy) so compliance questions have a concrete answer.

    Two rule shapes:
    - Counter rules (metric is set): observed := indexed L0 count over
      `window_seconds`; fires when observed >= limit_value.
    - Context rules (metric is None): fire purely on `applies_when`
      matching the decision context (e.g. {"root_cause": "fraud_block"}).
    """

    __tablename__ = "policy"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_id: Mapped[str] = mapped_column(Text, primary_key=True)
    metric: Mapped[str | None] = mapped_column(Text, nullable=True)
    limit_value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    window_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    verdict: Mapped[str] = mapped_column(Text, nullable=False)
    applies_when: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return (
            f"Policy(v{self.version}, {self.entity_type!r}, {self.rule_id!r}, "
            f"verdict={self.verdict!r})"
        )


# ---------------------------------------------------------------------------
# Decisions — the audit object (§5 of architecture.md)
# ---------------------------------------------------------------------------

# The closed action space (§6 taxonomy). The model cannot invent an action;
# the worst it can do is pick a legal one badly — Gate-2 checks that pick.
ACTION_RETRY_NOW = "RETRY_NOW"
ACTION_RETRY_SCHEDULED = "RETRY_SCHEDULED"
ACTION_SEND_PAYMENT_LINK = "SEND_PAYMENT_LINK"
ACTION_UPDATE_MANDATE = "UPDATE_MANDATE"
ACTION_REQUEST_PAYMENT_METHOD = "REQUEST_PAYMENT_METHOD"
ACTION_AWAIT_PROMISE = "AWAIT_PROMISE"
ACTION_ESCALATE_HUMAN = "ESCALATE_HUMAN"

LEGAL_ACTIONS = {
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_AWAIT_PROMISE,
    ACTION_ESCALATE_HUMAN,
}

# Actions that reach the customer (frequency caps + DNC apply to these)
CUSTOMER_CONTACT_ACTIONS = {
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_AWAIT_PROMISE,
}

# Actions that attempt money (retry caps apply to these)
MONEY_ATTEMPT_ACTIONS = {ACTION_RETRY_NOW, ACTION_RETRY_SCHEDULED}

# Confidence below this routes to a human at Gate-2
CONFIDENCE_FLOOR = 0.6

# Hard declines (§6, Stripe lesson): a retry is guaranteed to fail until a
# NEW payment method exists. Scheduled retries may stay queued, but they
# execute only after a PaymentMethodChanged event unblocks the path.
HARD_DECLINE_ROOT_CAUSES = {
    "card_expired",
    "lost_card",
    "stolen_card",
    "fraud_block",
    "mandate_inactive",
}


class Decision(Base):
    """The audit record that ties L0-L2 together (§5 of architecture.md).

    A gate never returns true/false — it returns the reason chain, and the
    chain is persisted here. `policy_version` records which rules were in
    force when the call was made; `inputs_hash`/`prompt_text` make the
    decision reproducible rather than merely logged.
    """

    __tablename__ = "decisions"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    root_cause: Mapped[str | None] = mapped_column(Text, nullable=True)
    gate1: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONType, nullable=True)
    policy_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    inputs_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    prompt_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    precedent_ids: Mapped[list[int] | None] = mapped_column(JSONType, nullable=True)
    proposal: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    gate2: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONType, nullable=True)
    executed_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)
    model: Mapped[str | None] = mapped_column(Text, nullable=True)
    tokens_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_micros: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )

    def __repr__(self) -> str:
        return f"Decision(id={self.id}, entity={self.entity_id!r}, root_cause={self.root_cause!r})"


# ---------------------------------------------------------------------------
# Scheduled actions — the durable queue (§14 of architecture.md)
# ---------------------------------------------------------------------------


class ScheduledAction(Base):
    """A future action, persisted (§14). Do NOT reach for Celery or Temporal —
    the ledger is the durable queue; this table is its ticking index.

    `executed_at` is set when the action fires, so a due scan never
    double-executes; the `idempotency_key` protects the execution itself.
    """

    __tablename__ = "scheduled_actions"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False, default="subscription")
    action: Mapped[str] = mapped_column(Text, nullable=False)
    due_at: Mapped[datetime] = mapped_column(UTCTimestamp, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    decision_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )
    executed_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)

    def __repr__(self) -> str:
        return (
            f"ScheduledAction(id={self.id}, entity={self.entity_id!r}, "
            f"action={self.action!r}, due_at={self.due_at})"
        )


# ---------------------------------------------------------------------------
# Diagnosis map + action taxonomy — versioned data, not code (§6, build item 5)
# ---------------------------------------------------------------------------

ROOT_CAUSE_UNKNOWN = "UNKNOWN"


class DiagnosisRule(Base):
    """Razorpay failure code → root-cause class, as versioned data.

    The map lives in a table (not a dict in code) so it is auditable and
    updatable without a deploy — an L2-style fact, evaluated at a version.
    """

    __tablename__ = "diagnosis_rules"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    failure_code: Mapped[str] = mapped_column(Text, primary_key=True)
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"DiagnosisRule(v{self.version}, {self.failure_code!r} → {self.root_cause!r})"


class TaxonomyRule(Base):
    """Root cause → default action, keyed to the decline reason (§6).

    `deterministic=True` marks the fixed-action routes (fraud_block,
    mandate_inactive, UNKNOWN) that skip the model entirely: the policy
    default IS the decision, zero tokens.
    """

    __tablename__ = "taxonomy_rules"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    root_cause: Mapped[str] = mapped_column(Text, primary_key=True)
    default_action: Mapped[str] = mapped_column(Text, nullable=False)
    deterministic: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return (
            f"TaxonomyRule(v{self.version}, {self.root_cause!r} → "
            f"{self.default_action!r}, det={self.deterministic})"
        )


# ---------------------------------------------------------------------------
# L3: Precedent (§4 of architecture.md) — the ONLY tier that embeds
# ---------------------------------------------------------------------------

EMBEDDING_DIMS = 1024


class Case(Base):
    """A resolved case summary: {situation → action → outcome} (§4).

    Embedded case SUMMARIES, never raw events; resolved cases only (an
    unresolved case has nothing to teach). The embedding is stored as a
    JSON vector — cross-backend by construction; on production Postgres
    this becomes a pgvector column with an HNSW index (§15 backlog) and
    ranking moves to the `<=>` operator.
    """

    __tablename__ = "cases"

    case_id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    scope: Mapped[str] = mapped_column(Text, nullable=False, default="private")
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    situation: Mapped[str] = mapped_column(Text, nullable=False)
    action_taken: Mapped[str] = mapped_column(Text, nullable=False)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    recovered_minor: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    embedding: Mapped[list[float] | None] = mapped_column(JSONType, nullable=True)
    source_entity_id: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)

    def __repr__(self) -> str:
        return (
            f"Case({self.case_id}, {self.entity_type!r}/{self.root_cause!r}, "
            f"outcome={self.outcome!r})"
        )


# ---------------------------------------------------------------------------
# Watchers — memory that initiates (§2, Context.dev monitor pattern)
# ---------------------------------------------------------------------------


class Watcher(Base):
    """A condition over L1/L2 facts that pushes a signed webhook when tripped.

    Watchers fire on integers and timestamps — never on embeddings (the
    same trust rule as the gates). Cooldown prevents re-firing spam; the
    tick loop owns the checks.
    """

    __tablename__ = "watchers"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)  # null = all
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    condition: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    # e.g. {"metric": "retry_count_7d", "op": ">=", "threshold": 2}
    #      {"metric": "open_ptp_due", "op": "<", "threshold": 0}  # overdue
    #      {"metric": "stale_awaiting", "op": ">=", "threshold": 3}  # days
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[str] = mapped_column(Text, nullable=False, default="")
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=3600)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )

    def __repr__(self) -> str:
        return f"Watcher({self.id}, {self.condition}, target={self.target_url!r})"


# ---------------------------------------------------------------------------
# L1 Snapshots — checkpoint for sublinear rebuild (§15)
# ---------------------------------------------------------------------------


class L1Snapshot(Base):
    """A watermarked checkpoint of L1. Rebuild replays only since the
    last snapshot — same guarantee, sublinear cost."""

    __tablename__ = "l1_snapshots"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_at_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state_json: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )

    def __repr__(self) -> str:
        return f"L1Snapshot({self.merchant_id}:{self.entity_id} @ {self.snapshot_at_event_id})"


Index("idx_l1snap_merchant_entity", L1Snapshot.merchant_id, L1Snapshot.entity_id)


# ---------------------------------------------------------------------------
# HITL Queue — working escalation loop (§15)
# ---------------------------------------------------------------------------


class HitlTask(Base):
    """A human task that writes back to L0, so L3 learns."""

    __tablename__ = "hitl_tasks"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False, default="subscription")
    decision_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    assignee: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="open")
    # open → claimed → resolved → closed  |  sla breached → escalated
    sla_due_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    resolution_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )

    def __repr__(self) -> str:
        return f"HitlTask({self.id}, {self.entity_id}:{self.status})"


# ---------------------------------------------------------------------------
# Archive anchors — chain-walkable cold storage (§15)
# ---------------------------------------------------------------------------


class ArchiveAnchor(Base):
    """Per-entity anchor surviving archival: the last hash before cold cut,
    so the chain still verifies via exported rows + anchor."""

    __tablename__ = "archive_anchors"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    merchant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    anchor_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    anchor_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    archived_through_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCTimestamp, nullable=False, default=lambda: datetime.now(UTC)
    )
