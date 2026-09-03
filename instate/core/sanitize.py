"""Payload sanitation at ingestion (§10: schema-in, junk never reaches L0).

`record_event` itself stays unsanitized on purpose: the hash is a pure
function of the exact payload, and the determinism test pins that
contract. Sanitation lives at the two INGRESS points instead — the
webhook receiver and the MCP write tool — where merchant-controlled
bytes first touch the system.

Why: an untrusted payload key can poison downstream logic. `diagnose`
reads `failure_code`; precedent summaries embed situation text; the
console renders payloads. A hostile `{"root_cause": "fraud_block"}`
must never steer a gate, and a 10 MB `{"note": ...}` must never bloat
a stored row. Note `root_cause` is deliberately ABSENT from the
whitelist: it is derived by `diagnose`, never asserted by callers.

Returns (clean_payload, dropped_keys) — callers surface the dropped
keys so stripping is transparent, not silent.
"""

MAX_STR_LEN = 256
MAX_ENTITY_ID_LEN = 128

# key -> (expected type, max length for str). amount_minor must be a real
# int (bools rejected explicitly — isinstance(True, int) is a classic trap).
INGEST_PAYLOAD_KEYS: dict[str, tuple[type, int | None]] = {
    "failure_code": (str, 128),
    "error_reason": (str, 256),
    "error_description": (str, 256),
    "error_source": (str, 128),
    "amount_minor": (int, None),
    "channel": (str, 64),
    "razorpay_event": (str, 64),
    "source_event_id": (str, 256),
    "success": (bool, None),
}


def sanitize_payload(payload: dict | None) -> tuple[dict | None, list[str]]:
    """Whitelist + type-check an ingress payload.

    Unknown keys are dropped. Wrong types are dropped (no coercion —
    coercion is how "0" becomes 0 and bypasses a check). Overlong
    strings are dropped, not truncated (truncation can silently change
    a code's meaning). Negative amounts are dropped.
    """
    if payload is None:
        return None, []
    if not isinstance(payload, dict):
        return None, ["<non-dict payload>"]

    clean: dict = {}
    dropped: list[str] = []
    for key, value in payload.items():
        spec = INGEST_PAYLOAD_KEYS.get(key)
        if spec is None:
            dropped.append(key)
            continue
        expected, max_len = spec
        if expected is bool:
            if type(value) is not bool:
                dropped.append(key)
                continue
        elif expected is int:
            if type(value) is not int or value < 0:
                dropped.append(key)
                continue
        elif expected is str:
            if not isinstance(value, str) or len(value) > (max_len or 0):
                dropped.append(key)
                continue
        clean[key] = value

    return (clean or None), dropped


def check_entity_id(entity_id: str) -> str | None:
    """Bounded entity ids — returns an error string, or None if fine."""
    if not isinstance(entity_id, str) or not entity_id:
        return "missing entity id in payload"
    if len(entity_id) > MAX_ENTITY_ID_LEN:
        return f"entity id exceeds {MAX_ENTITY_ID_LEN} chars"
    return None
