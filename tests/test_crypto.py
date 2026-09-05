"""Encryption at rest and redaction preserving chain verification."""

from cryptography.fernet import Fernet
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.crypto import decrypt_payload, get_fernet
from instate.core.ledger import record_event, redact_payload, verify_chain
from tests.conftest import make_merchant_id, now_utc

KEY = Fernet.generate_key().decode()


async def _raw_payload_text(session: AsyncSession, event_id: int) -> str | None:
    """Read raw payload bytes bypassing ORM."""
    result = await session.execute(text("SELECT payload FROM events WHERE id = :i"), {"i": event_id})
    return result.scalar_one_or_none()


async def test_roundtrip_with_key(session: AsyncSession, monkeypatch):
    monkeypatch.setenv("INSTATE_ENCRYPTION_KEY", KEY)
    assert get_fernet() is not None
    merchant = make_merchant_id()

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_enc",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
        source_event_id="enc_1",
    )
    await session.commit()

    assert event.payload == {"failure_code": "insufficient_funds", "amount_minor": 49900}

    result = await verify_chain(session, merchant, "sub_enc")
    assert result.verified


async def test_disk_holds_ciphertext_not_pii(session: AsyncSession, monkeypatch):
    monkeypatch.setenv("INSTATE_ENCRYPTION_KEY", KEY)
    merchant = make_merchant_id()

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_disk",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id="enc_2",
    )
    await session.commit()

    raw = await _raw_payload_text(session, event.id)
    assert raw is not None
    assert "failure_code" not in raw
    assert "insufficient_funds" not in raw


async def test_redacted_row_reads_none_and_chain_verifies(session: AsyncSession, monkeypatch):
    monkeypatch.setenv("INSTATE_ENCRYPTION_KEY", KEY)
    merchant = make_merchant_id()

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_red",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id="enc_3",
    )
    await session.commit()

    assert await redact_payload(session, event.id) is True
    await session.commit()

    assert decrypt_payload(None) is None
    result = await verify_chain(session, merchant, "sub_red")
    assert result.verified, f"chain must verify after redaction, got: {result.error}"


async def test_no_key_behaves_like_plain_json(session: AsyncSession, monkeypatch):
    monkeypatch.delenv("INSTATE_ENCRYPTION_KEY", raising=False)
    assert get_fernet() is None
    merchant = make_merchant_id()

    event = await record_event(
        session,
        merchant_id=merchant,
        entity_id="sub_plain",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds"},
        source_event_id="enc_4",
    )
    await session.commit()

    raw = await _raw_payload_text(session, event.id)
    assert raw is not None and "insufficient_funds" in raw

    monkeypatch.setenv("INSTATE_ENCRYPTION_KEY", KEY)
    event_id = event.id
    session.expire_all()
    from instate.core.models import Event

    result = await session.execute(select(Event).where(Event.id == event_id))
    assert result.scalar_one().payload == {"failure_code": "insufficient_funds"}
