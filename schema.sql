-- Instage Stage 1: Memory Core (L0 + L1)
-- Compatible with PostgreSQL 16+ and SQLite (for dev/test)

-- L0: The Ledger (immutable, hash-chained, bi-temporal)
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    merchant_id     UUID        NOT NULL,
    entity_id       TEXT        NOT NULL,
    entity_type     TEXT        NOT NULL,   -- subscription|payment|checkout|invoice
    event_type      TEXT        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,   -- valid time: when it happened
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- transaction time
    payload         JSONB,                  -- nullable: redaction target
    payload_hash    BYTEA       NOT NULL,   -- survives redaction
    source_event_id TEXT UNIQUE,            -- Razorpay webhook id → exactly-once
    decision_id     BIGINT,                 -- references decisions(id) when implemented
    prev_hash       BYTEA,                  -- hash of previous event for SAME entity
    hash            BYTEA       NOT NULL    -- sha256(prev_hash || merchant || entity || type || time || payload_hash)
);

CREATE INDEX idx_events_merchant_entity_occurred
    ON events (merchant_id, entity_id, occurred_at);

CREATE INDEX idx_events_recorded_at
    ON events (recorded_at);

-- L1: The Projection (pure fold over L0, rebuildable)
CREATE TABLE entity_state (
    merchant_id          UUID        NOT NULL,
    entity_id            TEXT        NOT NULL,
    entity_type          TEXT        NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'ACTIVE',  -- state-machine position
    last_contact_at      TIMESTAMPTZ,
    last_failure_reason  TEXT,
    open_ptp_due_at      TIMESTAMPTZ,   -- promise-to-pay
    amount_at_risk_minor BIGINT,
    last_event_id        BIGINT      NOT NULL DEFAULT 0,    -- fold watermark
    PRIMARY KEY (merchant_id, entity_id)
);

-- Windowed counters are deliberately NOT stored here.
-- They are computed at gate-check time as indexed L0 counts:
--   SELECT count(*) FROM events
--   WHERE merchant_id=$1 AND entity_id=$2
--     AND occurred_at > NOW() - INTERVAL '7 days'
-- This is sub-ms on idx_events_merchant_entity_occurred and never drifts.
