"""Payload sanitation at ingress; record_event stays unsanitized (§10).

Hash is over the exact payload (determinism); root_cause is derived by
diagnose, never asserted — hence absent from the whitelist.
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
    # Real Razorpay entity context (docs/webhooks/payments) — flat scalars
    # for the ledger timeline; all bounded, all optional.
    "method": (str, 32),
    "order_id": (str, 64),
    "status": (str, 32),
    "error_code": (str, 128),
}


def sanitize_payload(payload: dict | None) -> tuple[dict | None, list[str]]:
    """Whitelist and type-check an ingress payload; no coercion.

    Unknown keys, wrong types, overlong strings, and negative amounts drop.
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
