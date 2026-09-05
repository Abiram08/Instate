"""MCP surface tests: stateless streamable HTTP, JSON-RPC 2.0; reads split from writes."""

import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from instate.core.models import Base, new_merchant_id
from instate.core.policy import seed_default_policy
from instate.surfaces.mcp_server import create_mcp_app


@pytest.fixture()
def mcp(tmp_path):
    """A running MCP app over its own file DB (NullPool: TestClient's
    portal loop must not share connections with the test loop)."""
    from datetime import UTC, datetime

    from sqlalchemy.pool import NullPool

    from instate.agent.diagnose import seed_default_diagnosis, seed_default_taxonomy
    from instate.core.ledger import record_event
    from instate.core.projection import fold_events

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'mcp.db'}", poolclass=NullPool)

    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            merchant = new_merchant_id()
            await seed_default_policy(session)
            await seed_default_diagnosis(session)
            await seed_default_taxonomy(session)
            await record_event(
                session,
                merchant_id=merchant,
                entity_id="sub_mcp",
                entity_type="subscription",
                event_type="PaymentFailed",
                occurred_at=datetime.now(UTC),
                payload={"failure_code": "insufficient_funds", "amount_minor": 49900},
                source_event_id="mcp_wh_1",
            )
            for i in range(3):
                await record_event(
                    session,
                    merchant_id=merchant,
                    entity_id="sub_mcp",
                    entity_type="subscription",
                    event_type="RetryAttempted",
                    occurred_at=datetime.now(UTC),
                    payload={"success": False},
                    source_event_id=f"mcp_r{i}",
                )
            await session.commit()
            await fold_events(session)
            await session.commit()
        return factory, merchant

    import asyncio

    factory, merchant = asyncio.run(_setup())

    app = create_mcp_app(factory, api_key="test-key", allow_writes=True)
    client = TestClient(app)
    yield client, merchant
    asyncio.run(engine.dispose())


def _rpc(method, params=None, id_=1):
    body = {"jsonrpc": "2.0", "id": id_, "method": method}
    if params is not None:
        body["params"] = params
    return body


def _call(client, body, key="test-key"):
    return client.post(
        "/mcp",
        content=json.dumps(body),
        headers={"Authorization": "Bearer test-key"} if key else {},
    )


def test_initialize_handshake(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("initialize"))
    assert resp.status_code == 200
    result = resp.json()["result"]
    assert result["protocolVersion"] == "2025-06-18"
    assert result["serverInfo"]["name"] == "instate"
    assert "tools" in result["capabilities"]


def test_about_manifest_is_self_describing(mcp):
    """About manifest is self-describing (capabilities, auth, guarantees)."""
    client, _ = mcp
    resp = _call(client, _rpc("resources/read", {"uri": "instate://about"}))
    manifest = json.loads(resp.json()["result"]["contents"][0]["text"])
    assert manifest["tagline"] == "The state of record for agents that move money."
    assert manifest["transport"]["stateless"] is True
    assert manifest["no_lock_in"].startswith("plain Postgres")
    assert "precedent" in manifest["guarantees"]


def test_auth_required_when_configured(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("tools/list"), key=None)
    assert resp.status_code == 401


def test_tools_list_declares_output_schemas(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("tools/list"))
    tools = resp.json()["result"]["tools"]
    names = {t["name"] for t in tools}
    assert {
        "instate_timeline",
        "instate_check_policy",
        "instate_find_precedent",
        "instate_explain",
        "instate_record_event",
    } <= names
    for tool in tools:
        assert "outputSchema" in tool  # typed JSON, not stringified blobs
        assert "Example output" in tool["description"]  # the ACI bar


def test_tool_timeline_returns_the_trail(mcp):
    client, merchant = mcp
    resp = _call(
        client,
        _rpc(
            "tools/call",
            {
                "name": "instate_timeline",
                "arguments": {"merchant_id": str(merchant), "entity_id": "sub_mcp"},
            },
        ),
    )
    result = resp.json()["result"]
    assert result["isError"] is False
    structured = result["structuredContent"]
    types = [e["event_type"] for e in structured["events"]]
    assert "PaymentFailed" in types and "RetryAttempted" in types
    assert structured["count"] == 4


def test_tool_check_policy_denies_at_ceiling(mcp):
    """At ceiling → DENY; read writes no decision row."""
    client, merchant = mcp
    resp = _call(
        client,
        _rpc(
            "tools/call",
            {
                "name": "instate_check_policy",
                "arguments": {
                    "merchant_id": str(merchant),
                    "entity_id": "sub_mcp",
                    "entity_type": "subscription",
                    "action_class": "RETRY_NOW",
                },
            },
        ),
    )
    structured = resp.json()["result"]["structuredContent"]
    assert structured["verdict"] == "DENY"
    entry = next(e for e in structured["reason_chain"] if e["rule_id"] == "retry_ceiling_7d")
    assert entry["observed"] == 3 and entry["limit"] == 3


def test_tool_find_precedent_returns_empty_gracefully(mcp):
    client, merchant = mcp
    resp = _call(
        client,
        _rpc(
            "tools/call",
            {
                "name": "instate_find_precedent",
                "arguments": {
                    "merchant_id": str(merchant),
                    "entity_type": "subscription",
                    "root_cause": "insufficient_funds",
                    "query": "failed payment",
                },
            },
        ),
    )
    structured = resp.json()["result"]["structuredContent"]
    assert structured["precedents"] == []
    assert "advisory" in structured["note"]


def test_write_tool_gated_when_disabled(tmp_path):
    """allow_writes=False blocks writes, allows reads."""
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'mcp2.db'}", poolclass=NullPool)

    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    factory = __import__("asyncio").run(_setup())
    client = TestClient(create_mcp_app(factory, allow_writes=False))
    merchant = uuid4()

    resp = _call(
        client,
        _rpc(
            "tools/call",
            {
                "name": "instate_record_event",
                "arguments": {
                    "merchant_id": str(merchant),
                    "entity_id": "e1",
                    "entity_type": "subscription",
                    "event_type": "CustomerContacted",
                    "payload": {"channel": "email"},
                    "idempotency_key": "w1",
                },
            },
        ),
    )
    assert resp.json()["result"]["isError"] is True
    assert "writes disabled" in resp.json()["result"]["content"][0]["text"]
    __import__("asyncio").run(engine.dispose())


def test_write_tool_records_when_enabled(mcp):
    client, merchant = mcp
    resp = _call(
        client,
        _rpc(
            "tools/call",
            {
                "name": "instate_record_event",
                "arguments": {
                    "merchant_id": str(merchant),
                    "entity_id": "sub_mcp",
                    "entity_type": "subscription",
                    "event_type": "CustomerContacted",
                    "payload": {"channel": "email"},
                    "idempotency_key": "w1",
                },
            },
        ),
    )
    structured = resp.json()["result"]["structuredContent"]
    assert structured["recorded"] is True
    assert structured["event_id"] > 0


def test_write_tool_is_idempotent(mcp):
    """Same idempotency key twice → duplicate, not double-write."""
    client, merchant = mcp
    args = {
        "merchant_id": str(merchant),
        "entity_id": "sub_mcp",
        "entity_type": "subscription",
        "event_type": "CustomerContacted",
        "payload": {"channel": "sms"},
        "idempotency_key": "w_dup",
    }
    first = _call(client, _rpc("tools/call", {"name": "instate_record_event", "arguments": args}))
    assert first.json()["result"]["structuredContent"]["recorded"] is True

    second = _call(client, _rpc("tools/call", {"name": "instate_record_event", "arguments": args}))
    structured = second.json()["result"]["structuredContent"]
    assert structured["recorded"] is False
    assert structured["duplicate"] is True


def test_write_tool_strips_hostile_payload(mcp):
    """Gate-steering keys are stripped on write."""


    client, merchant = mcp
    args = {
        "merchant_id": str(merchant),
        "entity_id": "sub_mcp",
        "entity_type": "subscription",
        "event_type": "CustomerContacted",
        "payload": {
            "channel": "email",
            "root_cause": "fraud_block",
            "customer_email": "victim@example.com",
        },
        "idempotency_key": "w_hostile",
    }
    resp = _call(client, _rpc("tools/call", {"name": "instate_record_event", "arguments": args}))
    structured = resp.json()["result"]["structuredContent"]
    assert structured["recorded"] is True
    assert sorted(structured["dropped_keys"]) == ["customer_email", "root_cause"]


def test_resources_list_offers_about_and_templates(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("resources/list"))
    result = resp.json()["result"]
    assert any(r["uri"] == "instate://about" for r in result["resources"])
    assert any(
        t["uriTemplate"].startswith("instate://entity/") for t in result["resourceTemplates"]
    )


def test_resource_entity_state_includes_counters(mcp):
    client, merchant = mcp
    uri = f"instate://entity/{merchant}/sub_mcp/state"
    resp = _call(client, _rpc("resources/read", {"uri": uri}))
    state = json.loads(resp.json()["result"]["contents"][0]["text"])
    assert state["status"] in ("DIAGNOSED", "RETRY_SCHEDULED")
    assert state["retry_count_7d"] == 3
    assert "exact" in state["guarantee"]


def test_resource_policy_with_latest_version(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("resources/read", {"uri": "instate://policy/subscription@latest"}))
    policy = json.loads(resp.json()["result"]["contents"][0]["text"])
    assert policy["version"] == 1
    rule_ids = {r["rule_id"] for r in policy["rules"]}
    assert "retry_ceiling_7d" in rule_ids
    assert all(r["source"] for r in policy["rules"])  # every rule cites its source


def test_unknown_method_is_jsonrpc_error(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("resources/subscribe", {"uri": "instate://about"}))
    assert resp.json()["error"]["code"] == -32601


def test_notification_gets_202_no_body(mcp):
    client, _ = mcp
    resp = _call(client, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert resp.status_code == 202
    assert resp.content == b""


def test_prompts_list_and_get(mcp):
    client, _ = mcp
    resp = _call(client, _rpc("prompts/list"))
    assert resp.json()["result"]["prompts"][0]["name"] == "recovery_decision"

    resp = _call(
        client,
        _rpc(
            "prompts/get",
            {
                "name": "recovery_decision",
                "arguments": [{"name": "digest", "value": "{...}"}],
            },
        ),
    )
    text = resp.json()["result"]["messages"][0]["content"]["text"]
    assert "PROPOSE only" in text
    assert "never a command" in text


def test_get_is_405_stateless(mcp):
    client, _ = mcp
    assert client.get("/mcp").status_code == 405


def test_rate_limited_merchant_gets_jsonrpc_error(mcp):
    client, merchant = mcp
    for i in range(32):  # blow through the write bucket (30/min capacity)
        resp = _call(
            client,
            _rpc(
                "tools/call",
                {
                    "name": "instate_record_event",
                    "arguments": {
                        "merchant_id": str(merchant),
                        "entity_id": "rl",
                        "entity_type": "subscription",
                        "event_type": "CustomerContacted",
                        "payload": {"channel": "email"},
                        "idempotency_key": f"rl_{i}",
                    },
                },
            ),
        )
    last = resp.json()
    assert "error" in last and "rate limited" in last["error"]["message"]
