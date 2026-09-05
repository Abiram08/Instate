"""L0 events ledger, L1 entity state, L2 policy, and decisions audit. §1-6.

L0 is append-only: no UPDATE or DELETE. Hash chain is per (merchant_id, entity_id).
Windowed counters are computed at gate-check, not stored in L1."""

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

# BigInteger PK that works on both backends (BIGSERIAL on PG, INTEGER on SQLite)
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


class Base(DeclarativeBase):
    pass


# L0: The Ledger (§1)


class Event(Base):
    """Immutable hash-chained ledger event. Append-only: no UPDATE or DELETE."""

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
    # Encrypted at rest when INSTATE_ENCRYPTION_KEY is set. Chain hashes plaintext payload_hash.
    payload: Mapped[dict[str, Any] | None] = mapped_column(EncryptedJSONType(), nullable=True)
    payload_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_event_id: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)
    decision_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Denormalized from payload["channel"] for indexed per-channel counts. None for non-contact events.
    channel: Mapped[str | None] = mapped_column(Text, nullable=True)
    prev_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    def __repr__(self) -> str:
        return (
            f"Event(id={self.id}, entity={self.entity_id!r}, "
            f"type={self.event_type!r}, occurred_at={self.occurred_at})"
        )


Index("idx_events_merchant_entity_occurred", Event.merchant_id, Event.entity_id, Event.occurred_at)
Index("idx_events_recorded_at", Event.recorded_at)
Index(
    "idx_events_merchant_entity_channel_occurred",
    Event.merchant_id,
    Event.entity_id,
    Event.channel,
    Event.occurred_at,
)


# L1: The Projection (§2)

# State machine positions (§6):
#   ACTIVE → DIAGNOSED → RETRY_SCHEDULED → AWAITING_PROMISE → ESCALATED → RECOVERED
#                                                                      ↘ WRITTEN_OFF
STATUS_ACTIVE = "ACTIVE"
STATUS_DIAGNOSED = "DIAGNOSED"
STATUS_RETRY_SCHEDULED = "RETRY_SCHEDULED"
STATUS_AWAITING_PROMISE = "AWAITING_PROMISE"
STATUS_ESCALATED = "ESCALATED"
STATUS_RECOVERED = "RECOVERED"
STATUS_WRITTEN_OFF = "WRITTEN_OFF"
# PAUSED parks inattention failures; no auto money moves while parked.
STATUS_PAUSED = "PAUSED"

VALID_STATUSES = {
    STATUS_ACTIVE,
    STATUS_DIAGNOSED,
    STATUS_RETRY_SCHEDULED,
    STATUS_AWAITING_PROMISE,
    STATUS_ESCALATED,
    STATUS_RECOVERED,
    STATUS_WRITTEN_OFF,
    STATUS_PAUSED,
}

TERMINAL_STATUSES = {STATUS_RECOVERED, STATUS_WRITTEN_OFF}


class EntityState(Base):
    """Derived L1 state: fold over L0. Windowed counters live in L0 counts, not here."""

    __tablename__ = "entity_state"

    merchant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    entity_id: Mapped[str] = mapped_column(Text, primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False, default="payment")
    status: Mapped[str] = mapped_column(Text, nullable=False, default=STATUS_ACTIVE)
    last_contact_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    last_failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    open_ptp_due_at: Mapped[datetime | None] = mapped_column(UTCTimestamp, nullable=True)
    amount_at_risk_minor: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Customer timezone (IANA). Set from event payloads; invalid names are ignored.
    timezone: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Card expiry from CardExpiring events for pre-expiry watchers.
    card_exp_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    card_exp_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    def __repr__(self) -> str:
        return (
            f"EntityState(entity={self.entity_id!r}, status={self.status!r}, "
            f"last_event_id={self.last_event_id})"
        )


def new_merchant_id() -> UUID:
    return uuid4()


# L2: Policy (§3)

# Gate verdicts. A policy row can only DENY or REQUIRE_HUMAN — it can never
# *force* an action. ALLOW is computed (no rule fired), never stored in L2.
VERDICT_DENY = "DENY"
VERDICT_REQUIRE_HUMAN = "REQUIRE_HUMAN"
VERDICT_ALLOW = "ALLOW"
GATE_VERDICTS = {VERDICT_DENY, VERDICT_REQUIRE_HUMAN}


class Policy(Base):
    """Declarative versioned policy rule (§3). Counter rules use metric+window; context rules match applies_when."""

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


# Decisions — the audit object (§5)

# Closed action space (§6): model picks only from LEGAL_ACTIONS.
ACTION_RETRY_NOW = "RETRY_NOW"
ACTION_RETRY_SCHEDULED = "RETRY_SCHEDULED"
ACTION_SEND_PAYMENT_LINK = "SEND_PAYMENT_LINK"
ACTION_UPDATE_MANDATE = "UPDATE_MANDATE"
ACTION_REQUEST_PAYMENT_METHOD = "REQUEST_PAYMENT_METHOD"
ACTION_AWAIT_PROMISE = "AWAIT_PROMISE"
ACTION_ESCALATE_HUMAN = "ESCALATE_HUMAN"
# Retry on a different instrument already on file.
ACTION_RETRY_BACKUP_METHOD = "RETRY_BACKUP_METHOD"
# Read-only check for an updated payment method since failure.
ACTION_CHECK_METHOD_UPDATED = "CHECK_METHOD_UPDATED"

LEGAL_ACTIONS = {
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_AWAIT_PROMISE,
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_BACKUP_METHOD,
    ACTION_CHECK_METHOD_UPDATED,
}

# Actions that reach the customer (frequency caps + DNC apply to these)
CUSTOMER_CONTACT_ACTIONS = {
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_AWAIT_PROMISE,
}

# Actions that attempt money (retry caps apply to these). A backup-method
# retry IS a money attempt — it counts toward the same budgets.
MONEY_ATTEMPT_ACTIONS = {ACTION_RETRY_NOW, ACTION_RETRY_SCHEDULED, ACTION_RETRY_BACKUP_METHOD}

# Contact channels; free-form strings are rejected at the gate.
ALLOWED_CHANNELS = {
    "email",
    "sms",
    "push",
    "payment_link",
    "whatsapp",
    "mandate_update",
    "message",
    "upi",
}

# Confidence below this routes to a human at Gate-2
CONFIDENCE_FLOOR = 0.6

# Hard declines (§6): retries execute only after PaymentMethodChanged.
HARD_DECLINE_ROOT_CAUSES = {
    "card_expired",
    "lost_card",
    "stolen_card",
    "fraud_block",
    "mandate_inactive",
}


class Decision(Base):
    """Audit record tying L0-L2 together (§5). Stores reason chains and policy_version for reproducibility."""

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


# Scheduled actions — the durable queue (§14)


class ScheduledAction(Base):
    """Future action persisted as durable queue index (§14). executed_at prevents double-execution."""

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


# Diagnosis map + action taxonomy as versioned data (§6)

ROOT_CAUSE_UNKNOWN = "UNKNOWN"


class DiagnosisRule(Base):
    """Failure code → root-cause map as versioned rows. Auditable without a deploy."""

    __tablename__ = "diagnosis_rules"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    failure_code: Mapped[str] = mapped_column(Text, primary_key=True)
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"DiagnosisRule(v{self.version}, {self.failure_code!r} → {self.root_cause!r})"


class TaxonomyRule(Base):
    """Root cause → default action (§6). deterministic=True skips the model."""

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


class DunningSequence(Base):
    """Per-root-cause outreach step as versioned data. Advisory to the model; never gates."""

    __tablename__ = "dunning_sequences"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    root_cause: Mapped[str] = mapped_column(Text, primary_key=True)
    step_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str | None] = mapped_column(Text, nullable=True)
    delay_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=24)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return (
            f"DunningSequence(v{self.version}, {self.root_cause!r}#{self.step_index} → "
            f"{self.action!r} via {self.channel!r})"
        )


# L3: Precedent (§4)

EMBEDDING_DIMS = 1024


class Case(Base):
    """Resolved case summary {situation → action → outcome} (§4). Embedded summaries only."""

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


# Watchers (§2)


class Watcher(Base):
    """Condition over L1/L2 facts pushing a signed webhook. Integers/timestamps only, never embeddings."""

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


# L1 Snapshots — checkpoint for incremental rebuild (§15)


class L1Snapshot(Base):
    """Watermarked L1 checkpoint. Rebuild replays only since the last snapshot."""

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


# HITL Queue (§15)


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


# Archive anchors — cold storage (§15)


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
