"""Instate webhook surface — ledger-first, signature-verified (§6 step 0).

The webhook handler does exactly four things and returns:
  1. verify `X-Razorpay-Signature` (HMAC-SHA256 of the raw body with the
     webhook secret) — an unverified event never touches the ledger
  2. validate the body (bounded size, parseable JSON, known event shape)
  3. dedupe + append to L0 (`UNIQUE(source_event_id)` — redelivery inert)
  4. return 200

Diagnosis, gates, the LLM call, and Razorpay calls NEVER run inside the
webhook request — Razorpay times out slow handlers and redelivers, which
the dedupe would then eat for no reason. The tick loop drains the
pipeline asynchronously (agent.decide.drain_pending).

The handler is framework-free (`handle_webhook`); `create_app` is a thin
FastAPI wrapper (imported lazily — the core runs without it).
"""

import hashlib
import hmac
import json
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import DuplicateEventError, record_event
from instate.core.sanitize import check_entity_id, sanitize_payload

# Bounded payloads — junk never reaches the ledger (§10)
MAX_BODY_BYTES = 64 * 1024

# Razorpay event name → (our event_type, entity extractor).
# NOTE: payload shapes must be verified against real test-mode deliveries;
# the extractors are deliberately defensive (missing fields → rejected,
# not guessed — an event without an entity has nothing to act on).
RAZORPAY_EVENT_MAP: dict[str, str] = {
    "payment.failed": "PaymentFailed",
    "subscription.charged.failed": "PaymentFailed",
}


class WebhookRejected(Exception):
    """The webhook was refused before touching the ledger."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def verify_signature(raw_body: bytes, signature: str | None, secret: str) -> bool:
    """HMAC-SHA256 hex digest of the RAW body vs `X-Razorpay-Signature`.

    Raw bytes, not parsed-then-reserialized — a single re-serialization
    difference would fail the digest, and the digest is the point.
    Constant-time compare.
    """
    if not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


def extract_failure(raw: dict) -> tuple[str, str, str, str | None, int | None]:
    """(razorpay_event, entity_id, entity_type, failure_code, amount_minor).

    Raises WebhookRejected(400) for unknown events or unextractable
    entities — an event without an entity has nothing to act on.
    Merchant-controlled strings are bounded here (entity id length);
    the stored payload additionally passes `sanitize_payload`, so
    hostile keys can never reach the ledger.
    """
    event_name = raw.get("event")
    our_type = RAZORPAY_EVENT_MAP.get(event_name or "")
    if our_type is None:
        raise WebhookRejected(400, f"unsupported event: {event_name!r}")

    inner = raw.get("payload") or {}
    # payment.failed → payload.payment; subscription.charged.failed → payload.subscription
    entity_kind = "subscription" if event_name == "subscription.charged.failed" else "payment"
    entity = inner.get(entity_kind) or inner.get("payment") or inner.get("subscription")
    if not isinstance(entity, dict) or not entity.get("id"):
        raise WebhookRejected(400, "missing entity id in payload")

    entity_id = str(entity["id"])
    bad_id = check_entity_id(entity_id)
    if bad_id is not None:
        raise WebhookRejected(400, bad_id)
    entity_type = "subscription" if entity_kind == "subscription" else "payment"
    failure_code = (
        entity.get("error_reason") or entity.get("error_description") or entity.get("error_source")
    )
    # Razorpay amounts are minor units already; non-int/negative → absent
    # (a missing amount is honest; a forged amount would poison metrics)
    raw_amount = entity.get("amount")
    amount_minor = raw_amount if type(raw_amount) is int and raw_amount >= 0 else None
    return event_name, entity_id, entity_type, failure_code, amount_minor


async def handle_webhook(
    session: AsyncSession,
    *,
    raw_body: bytes,
    signature: str | None,
    secret: str,
    merchant_id: UUID,
    now: datetime | None = None,
) -> tuple[int, str]:
    """The four things, in order, then return (status_code, message).

    Ledger-first: the ONLY durable side effect is one append to L0.
    Returns 200 for both fresh events and redeliveries — a redelivery is
    a success story (dedupe), not an error Razorpay must retry.
    """
    # 1 · Authenticity — before ANY parsing
    if not verify_signature(raw_body, signature, secret):
        raise WebhookRejected(401, "invalid or missing signature")

    # Tenant context for the session: on Postgres this drives RLS
    # (fail-closed without it); on SQLite it is a no-op — the WHERE
    # clauses below are the guard in dev.
    from instate.core.tenant import set_tenant

    await set_tenant(session, merchant_id)

    # 2 · Bounded, parseable body
    if len(raw_body) > MAX_BODY_BYTES:
        raise WebhookRejected(413, "body too large")
    try:
        raw = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise WebhookRejected(400, "malformed JSON")
    if not isinstance(raw, dict):
        raise WebhookRejected(400, "body must be a JSON object")

    event_name, entity_id, entity_type, failure_code, amount_minor = extract_failure(raw)
    source_event_id = (
        raw.get("id")  # Razorpay delivery id — the exactly-once anchor
        or raw.get("request_id")
        or f"{event_name}:{entity_id}:{raw.get('created_at', '')}"
    )

    # 3 · Dedupe + append (the ONLY write; everything else is the drain).
    # The stored payload is built from extracted scalars only, then
    # sanitized — merchant-controlled keys can never reach the ledger.
    payload, _dropped = sanitize_payload(
        {
            "failure_code": failure_code,
            "razorpay_event": event_name,
            "source_event_id": source_event_id,
            "amount_minor": amount_minor,
        }
    )
    now = now or datetime.now(UTC)
    try:
        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="PaymentFailed",
            occurred_at=now,
            payload=payload,
            source_event_id=source_event_id,
        )
        await session.commit()
    except DuplicateEventError:
        await session.rollback()  # redelivery: inert, and still a 200
        return 200, "duplicate — already captured"

    # 4 · Ack
    return 200, "captured"


def create_app(
    *,
    session_factory,
    secret: str,
    merchant_id: UUID,
):
    """Thin FastAPI wrapper. `session_factory` is an async_sessionmaker.

    The receiver holds NO business logic — every line of the pipeline
    lives behind the drain, not behind this route.
    """
    from fastapi import FastAPI, Request, Response

    app = FastAPI(title="instate-webhook", version="0.1.0")

    @app.post("/webhook")
    async def webhook(request: Request) -> Response:
        raw_body = await request.body()
        signature = request.headers.get("X-Razorpay-Signature")
        async with session_factory() as session:
            try:
                status, message = await handle_webhook(
                    session,
                    raw_body=raw_body,
                    signature=signature,
                    secret=secret,
                    merchant_id=merchant_id,
                )
            except WebhookRejected as rejected:
                return Response(content=rejected.detail, status_code=rejected.status_code)
        return Response(content=message, status_code=status)

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

    return app
