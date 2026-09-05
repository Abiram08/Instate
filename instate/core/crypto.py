"""Encrypted event payloads at rest (§15). Chain hashes plaintext payload_hash; no key behaves as plain JSON."""

import base64
import hashlib
import json
import os
from typing import Any

from sqlalchemy import Text, TypeDecorator

try:
    from cryptography.fernet import Fernet, InvalidToken
    from sqlalchemy.dialects.postgresql import JSONB
except ImportError:  # pragma: no cover — graceful fallback if not installed
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore
    JSONB = None  # type: ignore


def _key_from_env() -> bytes | None:
    raw = os.environ.get("INSTATE_ENCRYPTION_KEY")
    if not raw:
        return None
    # accept a base64 Fernet key, else derive one from any passphrase
    try:
        Fernet(raw.encode())  # validates
        return raw.encode()
    except Exception:
        digest = hashlib.sha256(raw.encode()).digest()
        return base64.urlsafe_b64encode(digest)


def get_fernet():
    if Fernet is None:
        return None
    key = _key_from_env()
    if not key:
        return None
    return Fernet(key)


class EncryptedJSONType(TypeDecorator[dict[str, Any]]):
    """JSON with optional encryption. Duplicates JSONType dialect handling to avoid a models import cycle."""

    impl = Text  # type: ignore[assignment]
    cache_ok = True

    def load_dialect_impl(self, dialect, **kwargs):  # type: ignore[no-untyped-def]
        if dialect.name == "postgresql" and JSONB is not None:
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(Text())

    def _plain_bind(self, value, dialect):  # type: ignore[no-untyped-def]
        if dialect.name == "postgresql":
            return value  # asyncpg handles dict → JSONB natively
        return json.dumps(value)

    def process_bind_param(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        f = get_fernet()
        if f is None:
            return self._plain_bind(value, dialect)
        plaintext = json.dumps(value, sort_keys=True)
        token = f.encrypt(plaintext.encode()).decode()
        if dialect.name == "postgresql":
            return token  # asyncpg handles str → JSONB string natively
        return token

    def process_result_value(self, value, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if isinstance(value, dict):
            return value  # unencrypted (no key at write time, or PG dict)
        f = get_fernet()
        if f is None:
            return value if not isinstance(value, str) else json.loads(value)
        # Key configured: ciphertext token expected — but tolerate plain
        # JSON rows (written before the key existed, or by another writer).
        if not isinstance(value, str):
            return value
        try:
            plain = f.decrypt(value.encode()).decode()
            return json.loads(plain)
        except (InvalidToken, ValueError):
            return json.loads(value)


def decrypt_payload(stored) -> dict | None:
    """Read stored payload; None (redacted/absent) → None. Chain verifies off payload_hash."""
    if stored is None:
        return None
    if isinstance(stored, dict) and "_enc" not in stored:
        return stored
    # Legacy {"_enc": token} shape or raw token string
    f = get_fernet()
    if f is None:
        return stored if isinstance(stored, dict) else None
    token = stored["_enc"] if isinstance(stored, dict) else stored
    try:
        return json.loads(f.decrypt(str(token).encode()).decode())
    except (InvalidToken, ValueError):
        return None
