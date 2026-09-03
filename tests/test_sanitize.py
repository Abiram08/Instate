"""Payload sanitation at ingestion (§10: schema-in, junk never reaches L0).

`record_event` itself stays unsanitized on purpose — the hash is a pure
function of the exact payload. These tests pin the ingress boundary:
the webhook receiver and the MCP write tool.
"""

from instate.core.sanitize import check_entity_id, sanitize_payload


def test_whitelisted_keys_pass_through():
    payload = {
        "failure_code": "insufficient_funds",
        "amount_minor": 49900,
        "channel": "email",
        "success": True,
    }
    clean, dropped = sanitize_payload(payload)
    assert clean == payload
    assert dropped == []


def test_unknown_keys_are_dropped():
    """Injected root_cause, PII, and blobs never reach the ledger."""
    clean, dropped = sanitize_payload(
        {
            "failure_code": "insufficient_funds",
            "root_cause": "fraud_block",  # derived by diagnose, never asserted
            "customer_email": "victim@example.com",
            "note": "x" * 10000,
        }
    )
    assert clean == {"failure_code": "insufficient_funds"}
    assert sorted(dropped) == ["customer_email", "note", "root_cause"]


def test_wrong_types_are_dropped_not_coerced():
    """No coercion — coercion is how "0" becomes 0 and bypasses a check."""
    clean, dropped = sanitize_payload(
        {"amount_minor": "49900", "success": "yes", "channel": 42}
    )
    assert clean is None
    assert sorted(dropped) == ["amount_minor", "channel", "success"]


def test_bool_is_not_an_int():
    """isinstance(True, int) is the classic trap — amounts must be real ints."""
    clean, dropped = sanitize_payload({"amount_minor": True})
    assert clean is None
    assert dropped == ["amount_minor"]


def test_negative_amount_dropped():
    """A forged negative amount would poison the money metric."""
    clean, dropped = sanitize_payload({"amount_minor": -5})
    assert clean is None
    assert dropped == ["amount_minor"]


def test_overlong_strings_dropped_not_truncated():
    """Truncation can silently change a code's meaning — drop instead."""
    clean, dropped = sanitize_payload({"failure_code": "x" * 129})
    assert clean is None
    assert dropped == ["failure_code"]


def test_none_and_non_dict():
    assert sanitize_payload(None) == (None, [])
    clean, dropped = sanitize_payload(["not", "a", "dict"])
    assert clean is None
    assert dropped == ["<non-dict payload>"]


def test_empty_after_strip_returns_none():
    clean, dropped = sanitize_payload({"evil": 1})
    assert clean is None
    assert dropped == ["evil"]


def test_check_entity_id():
    assert check_entity_id("pay_ABC123") is None
    assert check_entity_id("") is not None
    assert check_entity_id("x" * 129) is not None
    assert check_entity_id("x" * 128) is None
