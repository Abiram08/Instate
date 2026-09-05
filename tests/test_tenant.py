"""Tenant isolation across ledger and surfaces."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.ledger import record_event
from instate.core.models import Decision
from instate.core.tenant import assert_tenant_scope, rls_ddl, set_tenant
from instate.core.models import Event
from instate.surfaces.mcp_server import read_resource, tool_explain, tool_timeline
from tests.conftest import make_merchant_id, now_utc


async def test_set_tenant_is_safe_noop_on_sqlite(session: AsyncSession):
    merchant = make_merchant_id()
    await set_tenant(session, merchant)
    await set_tenant(session, str(merchant))


async def test_rls_ddl_is_fail_closed():
    ddl = rls_ddl()
    assert ddl, "RLS DDL must not be empty"
    joined = "\n".join(ddl)
    assert "ENABLE ROW LEVEL SECURITY" in joined
    assert "tenant_isolation" in joined
    assert "current_setting('app.current_merchant')" in joined
    assert ", true)" not in joined.replace("set_config", ""), (
        "fail-closed: no permissive default in the USING clause"
    )


async def _seed_merchant_a(session: AsyncSession):
    merchant_a = make_merchant_id()
    await record_event(
        session,
        merchant_id=merchant_a,
        entity_id="sub_secret",
        entity_type="subscription",
        event_type="PaymentFailed",
        occurred_at=now_utc(),
        payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
        source_event_id="a_wh_1",
    )
    decision = Decision(merchant_id=merchant_a, entity_id="sub_secret", root_cause="x")
    session.add(decision)
    await session.commit()
    return merchant_a, decision.id


async def test_merchant_b_sees_nothing_of_a(session: AsyncSession):
    from instate.core.gate import evaluate
    from instate.core.ledger import get_timeline
    from instate.core.policy import seed_default_policy

    merchant_a, _ = await _seed_merchant_a(session)
    merchant_b = make_merchant_id()
    await seed_default_policy(session)
    await session.commit()

    events = await get_timeline(session, merchant_b, "sub_secret")
    assert events == []

    result = await evaluate(
        session,
        merchant_id=merchant_b,
        entity_id="sub_secret",
        entity_type="subscription",
        action_class="RETRY_NOW",
        root_cause="insufficient_funds",
        record=False,
    )
    assert result.verdict == "ALLOW"
    entry = next(e for e in result.reason_chain if e["rule_id"] == "retry_ceiling_7d")
    assert entry["observed"] == 0


async def test_mcp_timeline_as_other_merchant_is_empty(session: AsyncSession):
    merchant_a, _ = await _seed_merchant_a(session)
    merchant_b = make_merchant_id()

    out = await tool_timeline(
        session, {"merchant_id": str(merchant_b), "entity_id": "sub_secret"}
    )
    assert out["events"] == []
    assert out["count"] == 0


async def test_mcp_explain_rejects_mismatched_merchant(session: AsyncSession):
    merchant_a, decision_id = await _seed_merchant_a(session)
    merchant_b = make_merchant_id()

    with pytest.raises(KeyError):
        await tool_explain(
            session,
            {"decision_id": decision_id, "merchant_id": str(merchant_b)},
        )

    out = await tool_explain(session, {"decision_id": decision_id})
    assert out["decision"]["id"] == decision_id


async def test_mcp_resource_state_as_other_merchant_is_unknown(session: AsyncSession):
    from instate.core.projection import fold_events

    merchant_a, _ = await _seed_merchant_a(session)
    await fold_events(session)
    await session.commit()
    merchant_b = make_merchant_id()

    with pytest.raises(KeyError, match="unknown entity"):
        await read_resource(session, f"instate://entity/{merchant_b}/sub_secret/state")


async def test_assert_tenant_scope_helper(session: AsyncSession):
    merchant_a, _ = await _seed_merchant_a(session)
    await assert_tenant_scope(session, merchant_a, Event.__table__)
    with pytest.raises(AssertionError):
        await assert_tenant_scope(session, make_merchant_id(), Event.__table__)
