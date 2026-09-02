"""Tests for the gates — Gate-1 (evaluate) and Gate-2 (check_proposal).

The correctness-critical edges (build-order item 4: "unit-test the
stopping-rule boundaries hard"): at-limit vs below-limit, window edges,
entity isolation, dedupe not double-counting, context rules, the
closed action enum, state-machine legality, DNC, and the confidence
floor. Every gate result carries its reason chain — the evidence,
not a boolean.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.gate import check_proposal, evaluate, lock_entity_state
from instate.core.ledger import DuplicateEventError, record_event
from instate.core.models import (
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    Decision,
    EntityState,
    STATUS_ACTIVE,
    VERDICT_ALLOW,
    VERDICT_DENY,
    VERDICT_REQUIRE_HUMAN,
)
from instate.core.policy import seed_default_policy
from instate.core.projection import fold_events
from tests.conftest import days_ago, hours_ago, make_merchant_id, now_utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def seed_policy(session: AsyncSession):
    await seed_default_policy(session)
    await session.commit()


async def seed_events(
    session: AsyncSession,
    merchant,
    entity_id: str,
    event_specs: list[tuple[str, object, dict | None]],
    entity_type: str = "subscription",
):
    """Append events (each with a unique source id) and fold them."""
    for i, (event_type, occurred_at, payload) in enumerate(event_specs):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type=event_type,
            occurred_at=occurred_at,
            payload=payload,
            source_event_id=f"{entity_id}_{i}",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()


def retries(count: int, since) -> list[tuple[str, object, dict | None]]:
    """`count` retry attempts, one per day going back from `since`."""
    from datetime import timedelta

    return [("RetryAttempted", since - timedelta(days=i), None) for i in range(count)]


def chain_entry(result, rule_id: str) -> dict:
    return next(e for e in result.reason_chain if e["rule_id"] == rule_id)


GOOD_PROPOSAL = {
    "action": ACTION_RETRY_SCHEDULED,
    "timing": "T_PLUS_48H",
    "rationale": "payday-aligned retry",
    "confidence": 0.81,
}


# ---------------------------------------------------------------------------
# Gate-1 — the stopping-rule boundaries
# ---------------------------------------------------------------------------


async def test_gate1_allows_empty_history(session: AsyncSession):
    """No history → every counter rule reports observed=0 → ALLOW."""
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_new",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    assert result.verdict == VERDICT_ALLOW
    assert result.allowed is True
    entry = chain_entry(result, "retry_ceiling_7d")
    assert entry["observed"] == 0
    assert entry["limit"] == 3
    assert entry["verdict"] == VERDICT_ALLOW


async def test_gate1_denies_at_ceiling(session: AsyncSession):
    """THE boundary: observed == limit (3 retries in 7d) → DENY.

    An entity at its ceiling never reaches the model — zero tokens.
    """
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_ceiling",
        retries(3, now_utc()),
    )

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_ceiling",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    assert result.verdict == VERDICT_DENY
    assert result.allowed is False
    entry = chain_entry(result, "retry_ceiling_7d")
    assert entry["observed"] == 3
    assert entry["limit"] == 3
    assert entry["verdict"] == VERDICT_DENY


async def test_gate1_allows_below_ceiling(session: AsyncSession):
    """2 of 3 retries → ALLOW (the boundary holds on the safe side).

    The attempts sit at 6 and 5 days back: inside the 7d ceiling window,
    clear of the 24h spacing window — isolating the ceiling boundary.
    """
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_below",
        [("RetryAttempted", days_ago(6), None), ("RetryAttempted", days_ago(5), None)],
    )

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_below",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    assert result.verdict == VERDICT_ALLOW
    assert chain_entry(result, "retry_ceiling_7d")["observed"] == 2
    assert chain_entry(result, "retry_spacing_24h")["observed"] == 0


async def test_gate1_window_edge(session: AsyncSession):
    """A retry 8 days ago is outside the 7-day window; 6 days is inside."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_edge",
        [("RetryAttempted", days_ago(8), None), ("RetryAttempted", days_ago(6), None)],
    )

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_edge",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    entry = chain_entry(result, "retry_ceiling_7d")
    assert entry["observed"] == 1  # only the 6-day-old attempt counts
    assert result.verdict == VERDICT_ALLOW


async def test_gate1_entity_isolation(session: AsyncSession):
    """A's retries never count for B — the stopping rule is per entity."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(session, merchant, "sub_A", retries(3, now_utc()))

    a = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_A",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    b = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_B",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    assert a.verdict == VERDICT_DENY
    assert b.verdict == VERDICT_ALLOW


async def test_gate1_dedupe_never_double_counts(session: AsyncSession):
    """A webhook redelivery is inert — it cannot push an entity over its
    ceiling by counting twice."""
    merchant = make_merchant_id()
    await seed_policy(session)

    for i, at in enumerate([days_ago(3), days_ago(2)]):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_dup",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=at,
            source_event_id=f"evt_retry_{i}",
        )
    await session.commit()
    # The redelivery: same source_event_id → inert
    with pytest.raises(DuplicateEventError):
        await record_event(
            session,
            merchant_id=merchant,
            entity_id="sub_dup",
            entity_type="subscription",
            event_type="RetryAttempted",
            occurred_at=days_ago(3),
            source_event_id="evt_retry_0",
        )
    await session.commit()
    await fold_events(session)
    await session.commit()

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_dup",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    # 2 counted (not 3) → below the ceiling; spacing window clear
    assert chain_entry(result, "retry_ceiling_7d")["observed"] == 2
    assert chain_entry(result, "retry_spacing_24h")["observed"] == 0
    assert result.verdict == VERDICT_ALLOW


async def test_gate1_contact_cap_fires_for_contact_actions(session: AsyncSession):
    """2 contacts in 24h → contact-frequency cap fires for contact actions."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_contact",
        [
            ("CustomerContacted", hours_ago(5), {"channel": "email"}),
            ("PaymentLinkSent", hours_ago(2), {"channel": "sms"}),
        ],
    )

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_contact",
        entity_type="subscription",
        action_class=ACTION_SEND_PAYMENT_LINK,
    )
    await session.commit()

    assert result.verdict == VERDICT_DENY
    entry = chain_entry(result, "contact_freq_24h")
    assert entry["observed"] == 2
    assert entry["limit"] == 2


async def test_gate1_retry_caps_dont_fire_for_contacts(session: AsyncSession):
    """3 retries on the entity do NOT block a customer contact action —
    the caps are action-class-relevant."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(session, merchant, "sub_mixed", retries(3, now_utc()))

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_mixed",
        entity_type="subscription",
        action_class=ACTION_SEND_PAYMENT_LINK,
    )
    await session.commit()

    assert result.verdict == VERDICT_ALLOW


async def test_gate1_contact_caps_dont_fire_for_retries(session: AsyncSession):
    """Contacts on the entity do NOT consume the retry budget."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_mixed2",
        [("CustomerContacted", hours_ago(1), {"channel": "email"})],
    )

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_mixed2",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    assert result.verdict == VERDICT_ALLOW


# ---------------------------------------------------------------------------
# Gate-1 — context rules (keyed on the situation, not counts)
# ---------------------------------------------------------------------------


async def test_gate1_mandate_inactive_requires_human(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_mand",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
        root_cause="mandate_inactive",
    )
    await session.commit()

    assert result.verdict == VERDICT_REQUIRE_HUMAN
    entry = chain_entry(result, "mandate_inactive_require_human")
    assert entry["detail"] == "context rule matched"


async def test_gate1_fraud_block_requires_human(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_fraud",
        entity_type="subscription",
        action_class=ACTION_RETRY_NOW,
        root_cause="fraud_block",
    )
    await session.commit()

    assert result.verdict == VERDICT_REQUIRE_HUMAN


async def test_gate1_india_emandate_rule_denies(session: AsyncSession):
    """India-issued instrument → auto-retry denied by policy row, cited
    to the e-mandate regime."""
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_in",
        entity_type="subscription",
        action_class=ACTION_RETRY_NOW,
        context={"issuer_country": "IN"},
    )
    await session.commit()

    assert result.verdict == VERDICT_DENY
    assert chain_entry(result, "india_emandate_no_autoretry") is not None


async def test_gate1_deny_beats_require_human(session: AsyncSession):
    """At the ceiling AND fraud-blocked → DENY wins the aggregation."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(session, merchant, "sub_both", retries(3, now_utc()))

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_both",
        entity_type="subscription",
        action_class=ACTION_RETRY_NOW,
        root_cause="fraud_block",
    )
    await session.commit()

    assert result.verdict == VERDICT_DENY


# ---------------------------------------------------------------------------
# Gate-1 — decision records and the per-entity lock
# ---------------------------------------------------------------------------


async def test_gate1_writes_decision_row(session: AsyncSession):
    """The reason chain, policy version, and root cause are persisted —
    'explainable' as a data structure, not a log line."""
    merchant = make_merchant_id()
    await seed_policy(session)

    result = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_dec",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
        root_cause="insufficient_funds",
    )
    await session.commit()

    assert result.decision_id is not None
    decision = await session.get(Decision, result.decision_id)
    assert decision is not None
    assert decision.entity_id == "sub_dec"
    assert decision.root_cause == "insufficient_funds"
    assert decision.policy_version == 1
    assert decision.gate1 is not None
    assert any(e["rule_id"] == "retry_ceiling_7d" for e in decision.gate1)
    assert decision.gate2 is None  # gate-2 hasn't run yet


async def test_gate1_creates_state_row_and_locks(session: AsyncSession):
    """The gate get-or-creates the entity_state row under lock — the same
    row lock spans gate-check → intent-write (TOCTOU closed on Postgres;
    on SQLite FOR UPDATE is a dialect no-op, correctness by single-writer)."""
    merchant = make_merchant_id()
    await seed_policy(session)

    first = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_lock",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    second = await evaluate(
        session,
        merchant_id=merchant,
        entity_id="sub_lock",
        entity_type="subscription",
        action_class=ACTION_RETRY_SCHEDULED,
    )
    await session.commit()

    state = await session.get(EntityState, (merchant, "sub_lock"))
    assert state is not None
    assert state.status == STATUS_ACTIVE
    assert first.decision_id != second.decision_id


async def test_lock_entity_state_is_get_or_create(session: AsyncSession):
    merchant = make_merchant_id()
    state = await lock_entity_state(session, merchant, "sub_l1", "subscription")
    await session.flush()
    assert state.status == STATUS_ACTIVE
    again = await lock_entity_state(session, merchant, "sub_l1", "subscription")
    assert again.entity_id == "sub_l1"


async def test_gate1_unseeded_policy_is_a_hard_error(session: AsyncSession):
    """No policy rows for the entity type → hard error. An unseeded gate
    must never behave as an implicit allow-all."""
    merchant = make_merchant_id()
    with pytest.raises(LookupError):
        await evaluate(
            session,
            merchant_id=merchant,
            entity_id="sub_nopolicy",
            entity_type="checkout",
            action_class=ACTION_RETRY_SCHEDULED,
        )


# ---------------------------------------------------------------------------
# Gate-2 — the concrete proposal
# ---------------------------------------------------------------------------


async def _gate1(session, merchant, entity_id, action_class=ACTION_RETRY_SCHEDULED, **kw):
    return await evaluate(
        session,
        merchant_id=merchant,
        entity_id=entity_id,
        entity_type="subscription",
        action_class=action_class,
        **kw,
    )


async def test_gate2_allows_legal_proposal_and_persists(session: AsyncSession):
    """A legal proposal passes, and the gate2 chain lands on the SAME
    decision row — one row carries the full evidence of both gates."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_ok")
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_ok",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal=GOOD_PROPOSAL,
    )
    await session.commit()

    assert g2.verdict == VERDICT_ALLOW
    decision = await session.get(Decision, g1.decision_id)
    assert decision.gate2 is not None
    assert decision.proposal == GOOD_PROPOSAL
    assert any(e["rule_id"] == "retry_ceiling_7d" for e in decision.gate2)


async def test_gate2_denies_actions_outside_the_enum(session: AsyncSession):
    """The model cannot invent an action — and even if decoding ever
    leaked one, the enum backstop denies it here."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_enum")
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_enum",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": "CHARGE_THEM_AGAIN", "confidence": 0.99},
    )
    await session.commit()

    assert g2.verdict == VERDICT_DENY
    entry = chain_entry(g2, "action_enum")
    assert "outside the closed action space" in entry["detail"]


async def test_gate2_low_confidence_requires_human(session: AsyncSession):
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_conf")
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_conf",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_RETRY_SCHEDULED, "confidence": 0.4},
    )
    await session.commit()

    assert g2.verdict == VERDICT_REQUIRE_HUMAN
    assert chain_entry(g2, "confidence_floor") is not None


async def test_gate2_dnc_denies_contact_actions(session: AsyncSession):
    """Do-Not-Contact is what makes 'compliant' true, not asserted."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_dnc")
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_dnc",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_SEND_PAYMENT_LINK, "confidence": 0.9},
        context={"dnc": True},
    )
    await session.commit()

    assert g2.verdict == VERDICT_DENY
    assert chain_entry(g2, "dnc_consent") is not None


async def test_gate2_dnc_does_not_block_money_attempts(session: AsyncSession):
    """DNC guards *contact*; a retry attempt is not a contact."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_dnc2")
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_dnc2",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_RETRY_SCHEDULED, "confidence": 0.9},
        context={"dnc": True},
    )
    await session.commit()

    assert g2.verdict == VERDICT_ALLOW


async def test_gate2_rechecks_contact_cap_for_concrete_action(session: AsyncSession):
    """Gate-1 passed at class level earlier; by gate-2 time the entity has
    2 contacts in 24h — the concrete SEND_PAYMENT_LINK is denied."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(session, merchant, "sub_cap")
    await session.commit()
    # Contacts happen between the two gates (e.g. another workflow reached out)
    await seed_events(
        session,
        merchant,
        "sub_cap",
        [
            ("CustomerContacted", hours_ago(3), {"channel": "email"}),
            ("RecoveryActionSent", hours_ago(1), {"channel": "sms"}),
        ],
    )
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_cap",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_SEND_PAYMENT_LINK, "confidence": 0.9},
    )
    await session.commit()

    assert g2.verdict == VERDICT_DENY
    entry = chain_entry(g2, "contact_freq_24h")
    assert entry["observed"] == 2


async def test_gate2_state_machine_legality(session: AsyncSession):
    """From ESCALATED, a human owns the case: RETRY_NOW is illegal,
    ESCALATE_HUMAN is the only legal move."""
    merchant = make_merchant_id()
    await seed_policy(session)
    await seed_events(
        session,
        merchant,
        "sub_esc",
        [("EscalatedToHuman", now_utc(), {"reason": "manual"})],
    )

    state = await session.get(EntityState, (merchant, "sub_esc"))
    assert state.status == "ESCALATED"

    g1 = await _gate1(session, merchant, "sub_esc", action_class=ACTION_RETRY_NOW)
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_esc",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_RETRY_NOW, "confidence": 0.95},
    )
    await session.commit()

    assert g2.verdict == VERDICT_DENY
    assert chain_entry(g2, "state_machine") is not None


async def test_gate2_context_rule_refires(session: AsyncSession):
    """A fraud-blocked entity doesn't get a pass just because the model
    proposed something concrete — context rules re-fire at gate-2."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g1 = await _gate1(
        session,
        merchant,
        "sub_fraud2",
        action_class=ACTION_REQUEST_PAYMENT_METHOD,
        root_cause="fraud_block",
    )
    await session.commit()
    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_fraud2",
        entity_type="subscription",
        decision_id=g1.decision_id,
        proposal={"action": ACTION_REQUEST_PAYMENT_METHOD, "confidence": 0.95},
        root_cause="fraud_block",
    )
    await session.commit()

    assert g2.verdict == VERDICT_REQUIRE_HUMAN
    assert chain_entry(g2, "fraud_block_require_human") is not None


async def test_gate2_missing_decision_is_tolerated(session: AsyncSession):
    """A gate-2 without its gate-1 row (e.g. record=False upstream) still
    evaluates and returns a verdict — persistence is best-effort, the
    verdict is not."""
    merchant = make_merchant_id()
    await seed_policy(session)

    g2 = await check_proposal(
        session,
        merchant_id=merchant,
        entity_id="sub_orphan",
        entity_type="subscription",
        decision_id=999999,
        proposal=GOOD_PROPOSAL,
    )
    await session.commit()

    assert g2.verdict == VERDICT_ALLOW
