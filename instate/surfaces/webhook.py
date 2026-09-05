"""Ledger-first webhook receiver: verify HMAC, validate, dedupe+append, ack 200.
Diagnosis and gates run in the tick loop, never in the request.
Framework-free `handle_webhook`; `create_app` is a thin FastAPI wrapper.
"""

import hashlib
import hmac
import json
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import DuplicateEventError, record_event
from instate.core.sanitize import check_entity_id, sanitize_payload

# Bounded payloads — unverified or oversize bodies never reach the ledger.
MAX_BODY_BYTES = 64 * 1024

# Razorpay event name → our event_type.
# Extractors are defensive: missing entity → rejected. Verify shapes against test-mode deliveries.
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
    """HMAC-SHA256 of the raw body vs `X-Razorpay-Signature` (constant-time).
    Must use raw bytes, not reserialized JSON.
    """
    if not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


def extract_failure(raw: dict) -> tuple[str, str, str, str | None, int | None]:
    """Return (event, entity_id, entity_type, failure_code, amount_minor).
    Raises WebhookRejected(400) for unknown events or missing entity.

    Accepts both shapes: the flat demo shape (`payload.payment` IS the
    entity) and the real Razorpay shape (`payload.payment.entity` nests the
    entity with error_code/error_description/error_source/error_step,
    method, order_id, status — see razorpay.com/docs/webhooks/payments).
    """
    event_name = raw.get("event")
    our_type = RAZORPAY_EVENT_MAP.get(event_name or "")
    if our_type is None:
        raise WebhookRejected(400, f"unsupported event: {event_name!r}")

    inner = raw.get("payload") or {}
    # payment.failed → payload.payment; subscription.charged.failed → payload.subscription
    entity_kind = "subscription" if event_name == "subscription.charged.failed" else "payment"
    entity = inner.get(entity_kind) or inner.get("payment") or inner.get("subscription")
    if isinstance(entity, dict) and isinstance(entity.get("entity"), dict):
        entity = entity["entity"]  # real Razorpay nesting
    if not isinstance(entity, dict) or not entity.get("id"):
        raise WebhookRejected(400, "missing entity id in payload")

    entity_id = str(entity["id"])
    bad_id = check_entity_id(entity_id)
    if bad_id is not None:
        raise WebhookRejected(400, bad_id)
    entity_type = "subscription" if entity_kind == "subscription" else "payment"
    failure_code = (
        entity.get("error_reason") or entity.get("error_code")
        or entity.get("error_description") or entity.get("error_source")
    )
    # Real-shape context, kept as flat scalars for the ledger timeline.
    extras = {
        key: entity.get(key)
        for key in ("method", "order_id", "status", "error_code", "error_source")
        if isinstance(entity.get(key), str) and entity.get(key)
    }
    # Razorpay amounts are minor units; non-int/negative → absent.
    raw_amount = entity.get("amount")
    amount_minor = raw_amount if type(raw_amount) is int and raw_amount >= 0 else None
    return event_name, entity_id, entity_type, failure_code, amount_minor, extras


async def handle_webhook(
    session: AsyncSession,
    *,
    raw_body: bytes,
    signature: str | None,
    secret: str,
    merchant_id: UUID,
    now: datetime | None = None,
) -> tuple[int, str]:
    """Verify, validate, dedupe+append to L0, ack 200.
    Ledger-first: one L0 append is the only write; redeliveries return 200 via dedupe.
    """
    # 1 · Authenticity — before ANY parsing
    if not verify_signature(raw_body, signature, secret):
        raise WebhookRejected(401, "invalid or missing signature")

    # Tenant context for RLS (fail-closed on Postgres; no-op on SQLite).
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

    event_name, entity_id, entity_type, failure_code, amount_minor, extras = extract_failure(raw)
    source_event_id = (
        raw.get("id")  # Razorpay delivery id — the exactly-once anchor
        or raw.get("request_id")
        or f"{event_name}:{entity_id}:{raw.get('created_at', '')}"
    )

    # 3 · Dedupe + append (only write). Payload built from scalars, then sanitized.
    payload, _dropped = sanitize_payload(
        {
            "failure_code": failure_code,
            "razorpay_event": event_name,
            "source_event_id": source_event_id,
            "amount_minor": amount_minor,
            **extras,
        }
    )
    now = now or datetime.now(UTC)
    try:
        event = await record_event(
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
        from instate.core.ledger import get_event_by_source_id

        prior = await get_event_by_source_id(session, source_event_id)
        if prior is not None:
            return 200, f"duplicate — already captured {_bookmark(prior)}"
        return 200, "duplicate — already captured"
    except IntegrityError:
        # Check-then-insert race: a concurrent delivery won the same
        # source_event_id between our pre-check and our flush. Same
        # contract as a redelivery — one event, still a 200.
        await session.rollback()
        from instate.core.ledger import get_event_by_source_id

        prior = await get_event_by_source_id(session, source_event_id)
        if prior is not None:
            return 200, f"duplicate — already captured {_bookmark(prior)}"
        return 200, "duplicate — already captured"

    # 4 · Ack with a bookmark: the ledger position this delivery owns.
    return 200, f"captured {_bookmark(event)}"


def _bookmark(event) -> str:
    """Read-your-write token: event id + chain head. A later `timeline`
    showing this head proves the write is visible to reads (HydraDB calls
    this a causal bookmark; ours is just the hash chain doing its job)."""
    head = (event.hash or b"").hex()[:12] if isinstance(event.hash, (bytes, bytearray)) else "?"
    return f"#{event.id} head={head}"


def create_app(
    *,
    session_factory,
    secret: str,
    merchant_id: UUID,
):
    """Thin FastAPI wrapper over `handle_webhook`; no business logic here."""
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
