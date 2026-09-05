"""Webhook surface tests: verify → validate → dedupe+append → 200."""

import hashlib
import hmac
import json
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Event
from instate.surfaces.webhook import (
    WebhookRejected,
    create_app,
    extract_failure,
    handle_webhook,
    verify_signature,
)
from tests.conftest import make_merchant_id

SECRET = "whsec_test_secret"


def sign(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def razorpay_body(
    event: str = "payment.failed",
    entity_id: str = "pay_ABC123",
    failure_code: str = "insufficient_funds",
    delivery_id: str | None = "evt_N0DElQ1ZsDFPaY",
    amount: int | None = None,
    extra_entity: dict | None = None,
) -> bytes:
    inner = {"id": entity_id, "status": "failed"}
    if failure_code is not None:
        inner["error_reason"] = failure_code
    if amount is not None:
        inner["amount"] = amount
    if extra_entity:
        inner.update(extra_entity)
    body = {
        "event": event,
        "payload": {"payment": inner},
    }
    if delivery_id:
        body["id"] = delivery_id
    return json.dumps(body).encode("utf-8")


# ---------------------------------------------------------------------------
# verify_signature
# ---------------------------------------------------------------------------


def test_valid_signature_passes():
    body = razorpay_body()
    assert verify_signature(body, sign(body), SECRET) is True


def test_tampered_body_fails():
    """One byte changed → digest mismatch. Raw bytes, not re-serialized."""
    body = razorpay_body()
    tampered = body.replace(b"failed", b"captured")
    assert verify_signature(tampered, sign(body), SECRET) is False


def test_wrong_secret_fails():
    body = razorpay_body()
    assert verify_signature(body, sign(body, "other-secret"), SECRET) is False


def test_missing_signature_fails():
    assert verify_signature(razorpay_body(), None, SECRET) is False


# ---------------------------------------------------------------------------
# extract_failure — defensive payload extraction
# ---------------------------------------------------------------------------


def test_extract_failure_payment():
    raw = json.loads(razorpay_body())
    event, entity_id, entity_type, code, amount, extras = extract_failure(raw)
    assert event == "payment.failed"
    assert entity_id == "pay_ABC123"
    assert entity_type == "payment"
    assert code == "insufficient_funds"
    assert amount is None  # absent amount stays absent (honest, not zero)
    assert extras == {"status": "failed"}


def test_extract_failure_captures_amount():
    """Amounts are minor units; non-int/negative → absent."""
    raw = json.loads(razorpay_body(amount=49900))
    *_, amount, _ = extract_failure(raw)
    assert amount == 49900

    raw_bad = json.loads(razorpay_body(amount="49900"))
    *_, bad_amount, _ = extract_failure(raw_bad)
    assert bad_amount is None

    raw_neg = json.loads(razorpay_body(amount=-5))
    *_, neg_amount, _ = extract_failure(raw_neg)
    assert neg_amount is None


def test_extract_failure_real_razorpay_shape():
    """Real dashboard shape: payload.payment.entity nesting + error_code."""
    raw = {
        "entity": "event",
        "account_id": "acc_test123",
        "event": "payment.failed",
        "contains": ["payment"],
        "payload": {"payment": {"entity": {
            "id": "pay_REAL01", "entity": "payment", "amount": 50000,
            "currency": "INR", "status": "failed", "method": "netbanking",
            "order_id": "order_test01", "error_code": "BAD_REQUEST_ERROR",
            "error_description": "Payment failed", "error_source": "bank",
            "error_step": "payment_authorization", "error_reason": "payment_failed",
        }}},
        "created_at": 1758610215,
    }
    event, entity_id, entity_type, code, amount, extras = extract_failure(raw)
    assert entity_id == "pay_REAL01"
    assert code == "payment_failed"
    assert amount == 50000
    assert extras == {"method": "netbanking", "order_id": "order_test01",
                      "status": "failed", "error_code": "BAD_REQUEST_ERROR",
                      "error_source": "bank"}


async def test_real_shaped_delivery_is_captured_with_context(session: AsyncSession):
    """End to end: real shape → captured, context scalars on the ledger."""
    merchant = make_merchant_id()
    inner = {"id": "pay_REAL02", "entity": "payment", "amount": 50000,
             "status": "failed", "method": "upi", "order_id": "order_test02",
             "error_code": "BAD_REQUEST_ERROR", "error_reason": "payment_failed"}
    body = json.dumps({"event": "payment.failed", "id": "evt_real_02",
                       "payload": {"payment": {"entity": inner}}}).encode()
    status, message = await handle_webhook(
        session, raw_body=body, signature=sign(body), secret=SECRET, merchant_id=merchant)
    assert status == 200 and message.startswith("captured #")
    event = await session.get(Event, int(message.split("#")[1].split()[0]))
    assert event.payload["failure_code"] == "payment_failed"
    assert event.payload["method"] == "upi"
    assert event.payload["order_id"] == "order_test02"


def test_extract_failure_subscription():
    raw = json.loads(razorpay_body(event="subscription.charged.failed", entity_id="sub_XYZ"))
    _, entity_id, entity_type, _, _, _ = extract_failure(raw)
    assert entity_id == "sub_XYZ"
    assert entity_type == "subscription"


def test_extract_failure_rejects_overlong_entity_id():
    """Unbounded merchant strings never become keys — rejected, not stored."""
    raw = json.loads(razorpay_body(entity_id="x" * 500))
    with pytest.raises(WebhookRejected) as exc:
        extract_failure(raw)
    assert exc.value.status_code == 400


def test_extract_failure_unknown_event_rejected():
    raw = {"event": "payment.captured", "payload": {"payment": {"id": "p1"}}}
    with pytest.raises(WebhookRejected) as exc:
        extract_failure(raw)
    assert exc.value.status_code == 400


def test_extract_failure_missing_entity_rejected():
    """Event without entity is rejected, never guessed."""
    raw = {"event": "payment.failed", "payload": {"payment": {}}}
    with pytest.raises(WebhookRejected):
        extract_failure(raw)


# ---------------------------------------------------------------------------
# handle_webhook
# ---------------------------------------------------------------------------


async def test_valid_webhook_is_captured(session: AsyncSession):
    merchant = make_merchant_id()
    body = razorpay_body()

    status, message = await handle_webhook(
        session,
        raw_body=body,
        signature=sign(body),
        secret=SECRET,
        merchant_id=merchant,
    )

    assert status == 200
    result = await session.execute(select(Event))
    events = list(result.scalars().all())
    assert len(events) == 1
    assert events[0].event_type == "PaymentFailed"
    assert events[0].entity_id == "pay_ABC123"
    assert events[0].payload["failure_code"] == "insufficient_funds"
    assert events[0].source_event_id == "evt_N0DElQ1ZsDFPaY"


async def test_capture_returns_bookmark_with_chain_head(session: AsyncSession):
    """The 200 carries a read-your-write token: event id + chain head."""
    merchant = make_merchant_id()
    body = razorpay_body()
    status, message = await handle_webhook(
        session, raw_body=body, signature=sign(body), secret=SECRET, merchant_id=merchant)
    assert status == 200
    assert message.startswith("captured #") and "head=" in message
    head = message.split("head=")[1]

    event = await session.get(Event, int(message.split("#")[1].split()[0]))
    assert event is not None and event.hash.hex()[:12] == head


async def test_redelivery_bookmark_names_the_same_head(session: AsyncSession):
    """A redelivery is inert but names the identical ledger position."""
    merchant = make_merchant_id()
    body = razorpay_body()
    _, first = await handle_webhook(
        session, raw_body=body, signature=sign(body), secret=SECRET, merchant_id=merchant)
    status, second = await handle_webhook(
        session, raw_body=body, signature=sign(body), secret=SECRET, merchant_id=merchant)
    assert status == 200 and "duplicate" in second
    assert second.split("head=")[1] == first.split("head=")[1]


async def test_bad_signature_never_touches_the_ledger(session: AsyncSession):
    """Unverified event never reaches L0."""
    merchant = make_merchant_id()
    body = razorpay_body()

    with pytest.raises(WebhookRejected) as exc:
        await handle_webhook(
            session,
            raw_body=body,
            signature="deadbeef",
            secret=SECRET,
            merchant_id=merchant,
        )

    assert exc.value.status_code == 401
    result = await session.execute(select(Event))
    assert list(result.scalars().all()) == []


async def test_redelivery_is_inert_and_still_200(session: AsyncSession):
    """Same delivery id → dedupe → one event, two 200s."""
    merchant = make_merchant_id()
    body = razorpay_body()

    status1, _ = await handle_webhook(
        session,
        raw_body=body,
        signature=sign(body),
        secret=SECRET,
        merchant_id=merchant,
    )
    status2, message2 = await handle_webhook(
        session,
        raw_body=body,
        signature=sign(body),
        secret=SECRET,
        merchant_id=merchant,
    )

    assert status1 == 200
    assert status2 == 200
    assert "duplicate" in message2
    result = await session.execute(select(Event))
    assert len(list(result.scalars().all())) == 1


async def test_malformed_json_rejected(session: AsyncSession):
    merchant = make_merchant_id()
    body = b"{not json"

    with pytest.raises(WebhookRejected) as exc:
        await handle_webhook(
            session,
            raw_body=body,
            signature=sign(body),
            secret=SECRET,
            merchant_id=merchant,
        )
    assert exc.value.status_code == 400


async def test_oversized_body_rejected(session: AsyncSession):
    """Oversized body is rejected (413)."""
    merchant = make_merchant_id()
    from instate.surfaces.webhook import MAX_BODY_BYTES

    body = b'{"padding": "' + b"x" * (MAX_BODY_BYTES + 1) + b'"}'

    with pytest.raises(WebhookRejected) as exc:
        await handle_webhook(
            session,
            raw_body=body,
            signature=sign(body),
            secret=SECRET,
            merchant_id=merchant,
        )
    assert exc.value.status_code == 413


async def test_non_object_body_rejected(session: AsyncSession):
    merchant = make_merchant_id()
    body = b'["a", "list"]'
    with pytest.raises(WebhookRejected) as exc:
        await handle_webhook(
            session,
            raw_body=body,
            signature=sign(body),
            secret=SECRET,
            merchant_id=merchant,
        )
    assert exc.value.status_code == 400


async def test_hostile_entity_fields_never_reach_the_ledger(session: AsyncSession):
    """Hostile payload keys are stripped; only extracted scalars stored."""
    merchant = make_merchant_id()
    body = razorpay_body(
        amount=49900,
        extra_entity={
            "root_cause": "fraud_block",  # must NOT steer anything
            "customer_email": "victim@example.com",  # PII must not land
            "note": "x" * 10000,  # blob must not bloat the row
        },
    )

    status, _ = await handle_webhook(
        session,
        raw_body=body,
        signature=sign(body),
        secret=SECRET,
        merchant_id=merchant,
    )
    assert status == 200

    result = await session.execute(select(Event))
    stored = list(result.scalars().all())[0].payload
    assert stored["failure_code"] == "insufficient_funds"
    assert stored["amount_minor"] == 49900
    assert "root_cause" not in stored
    assert "customer_email" not in stored
    assert "note" not in stored


# ---------------------------------------------------------------------------
# FastAPI wrapper
# ---------------------------------------------------------------------------


async def test_create_app_end_to_end(tmp_path):
    """HTTP surface: signed → 200, redelivery → 200, unsigned → 401."""
    fastapi_test = pytest.importorskip("fastapi.testclient")

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )
    from sqlalchemy.pool import NullPool

    from instate.core.models import Base

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'wh.db'}", poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    app = create_app(session_factory=factory, secret=SECRET, merchant_id=uuid4())
    client = fastapi_test.TestClient(app)

    body = razorpay_body(entity_id="pay_http", delivery_id="evt_http_1")

    # Signed → 200, captured
    r = client.post("/webhook", content=body, headers={"X-Razorpay-Signature": sign(body)})
    assert r.status_code == 200

    # Redelivery over HTTP → still 200, inert
    r2 = client.post("/webhook", content=body, headers={"X-Razorpay-Signature": sign(body)})
    assert r2.status_code == 200

    # Unsigned → 401, ledger untouched
    r3 = client.post("/webhook", content=body)
    assert r3.status_code == 401

    # Health
    assert client.get("/health").json() == {"ok": True}

    # Exactly one event in the ledger
    async with factory() as s:
        result = await s.execute(select(Event))
        assert len(list(result.scalars().all())) == 1

    await engine.dispose()
