"""Stateless MCP server (Streamable HTTP, JSON-RPC 2.0) over the ledger.
Reads split from writes (write tool capability-gated); every tool declares
an output schema; read tools never write decisions.
"""

import json
import uuid as uuid_mod
from typing import Any

from instate.core.ops import RateLimits

JSONRPC = "2.0"
PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "instate", "version": "0.1.0"}

# -32700 parse error · -32600 invalid request · -32601 method not found
# -32602 invalid params · -32603 internal error
PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS, INTERNAL = (
    -32700,
    -32600,
    -32601,
    -32602,
    -32603,
)


# ---------------------------------------------------------------------------
# Self-describing manifest
# ---------------------------------------------------------------------------


def about_manifest() -> dict[str, Any]:
    """Capabilities, auth, and guarantees for agents."""
    return {
        "name": "instate",
        "version": SERVER_INFO["version"],
        "tagline": "The state of record for agents that move money.",
        "thesis": (
            "Authority decreases as uncertainty increases. Timeline and "
            "policy answers are exact integers over an immutable ledger; "
            "precedent is probabilistic and never gates an action."
        ),
        "transport": {
            "type": "streamable-http",
            "stateless": True,
            "session_affinity": False,
            "note": "no Mcp-Session-Id — safe behind a plain load balancer",
        },
        "auth": {
            "type": "bearer",
            "required": False,
            "scope_note": "reads and writes are separate capabilities; "
            "writes are refused unless the server enables them",
        },
        "capabilities": ["tools", "resources", "prompts"],
        "guarantees": {
            "timeline": "deterministic, hash-chained (tamper-evident)",
            "policy": "exact, versioned, every rule cites its source",
            "precedent": "probabilistic — advisory only, never gates",
        },
        "no_lock_in": "plain Postgres rows + JSONB — your memory exports with pg_dump",
        "tools_example_note": "every tool below includes one example output",
    }


TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "instate_timeline",
        "description": (
            "Ordered event history for an entity, oldest first, from the "
            "immutable hash-chained ledger. DETERMINISTIC: this is the "
            "exact answer to 'what have we already tried on this "
            "account?'. Example output: "
            '{"events": [{"event_type": "RetryAttempted", "occurred_at": '
            '"2026-08-30T09:00:00+00:00", "payload": {"success": false}}], '
            '"count": 1}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "merchant_id": {"type": "string"},
                "entity_id": {"type": "string"},
                "limit": {"type": "integer", "default": 20, "maximum": 100},
            },
            "required": ["merchant_id", "entity_id"],
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "events": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "count": {"type": "integer"},
            },
            "required": ["events", "count"],
        },
    },
    {
        "name": "instate_check_policy",
        "description": (
            "Evaluate whether an action CLASS is allowed for an entity "
            "right now — the deterministic gate. Returns the verdict AND "
            "the reason chain (evidence, not a boolean). Example output: "
            '{"verdict": "DENY", "policy_version": 1, "reason_chain": '
            '[{"rule_id": "retry_ceiling_7d", "observed": 3, "limit": 3, '
            '"verdict": "DENY"}]}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "merchant_id": {"type": "string"},
                "entity_id": {"type": "string"},
                "entity_type": {"type": "string"},
                "action_class": {"type": "string"},
                "root_cause": {"type": "string"},
                "context": {"type": "object"},
            },
            "required": ["merchant_id", "entity_id", "entity_type", "action_class"],
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string"},
                "policy_version": {"type": "integer"},
                "reason_chain": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["verdict", "reason_chain"],
        },
    },
    {
        "name": "instate_find_precedent",
        "description": (
            "Top-k resolved cases with this root cause and shape — "
            "PROBABILISTIC, advisory only: this answer may inform a "
            "proposal but can NEVER gate an action. Returns [] when the "
            "store is cold. Example output: "
            '{"precedents": [{"situation": "sub_003: subscription charge '
            'failed (insufficient_funds)...", "action_taken": "retry", '
            '"outcome": "recovered", "similarity": 0.81}]}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "merchant_id": {"type": "string"},
                "entity_type": {"type": "string"},
                "root_cause": {"type": "string"},
                "query": {"type": "string"},
                "top_k": {"type": "integer", "default": 3, "maximum": 10},
            },
            "required": ["merchant_id", "entity_type", "root_cause", "query"],
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "precedents": {"type": "array", "items": {"type": "object"}},
                "note": {"type": "string"},
            },
            "required": ["precedents"],
        },
    },
    {
        "name": "instate_explain",
        "description": (
            "Reproduce one decision: the reason chains both gates fired, "
            "the model's proposal, and what executed. This is the "
            "'explainable' requirement as a read. Example output: "
            '{"decision": {"id": 42, "executed_action": "RETRY_SCHEDULED", '
            '"gate1": [{"rule_id": "retry_ceiling_7d", "verdict": "ALLOW"}]}}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"decision_id": {"type": "integer"},
                           "merchant_id": {"type": "string"}},
            "required": ["decision_id"],
        },
        "outputSchema": {
            "type": "object",
            "properties": {"decision": {"type": "object"}},
            "required": ["decision"],
        },
    },
    {
        "name": "instate_record_event",
        "description": (
            "WRITE — append an event to the ledger (idempotent by "
            "idempotency_key). Separately capability-gated: refused unless "
            "the server was started with writes enabled. Ingress payloads "
            "are schema-filtered (unknown keys stripped, reported in "
            "`dropped_keys`). Example output: "
            '{"recorded": true, "event_id": 1337}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "merchant_id": {"type": "string"},
                "entity_id": {"type": "string"},
                "entity_type": {"type": "string"},
                "event_type": {"type": "string"},
                "payload": {"type": "object"},
                "idempotency_key": {"type": "string"},
            },
            "required": [
                "merchant_id",
                "entity_id",
                "entity_type",
                "event_type",
                "idempotency_key",
            ],
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "recorded": {"type": "boolean"},
                "event_id": {"type": "integer"},
                "duplicate": {"type": "boolean"},
                "dropped_keys": {"type": "array", "items": {"type": "string"}},
                "note": {"type": "string"},
            },
            "required": ["recorded"],
        },
    },
]

RESOURCE_TEMPLATES: list[dict[str, Any]] = [
    {
        "uriTemplate": "instate://entity/{merchant_id}/{entity_id}/state",
        "name": "entity state",
        "description": "Current L1 state + windowed counters (exact, derived, rebuildable). Cacheable.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "instate://policy/{entity_type}@{version}",
        "name": "policy in force",
        "description": "Every rule at a version, each citing its source. `latest` resolves to the active version.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "instate://decision/{decision_id}",
        "name": "decision record",
        "description": "The full audit object: both gates' reason chains, proposal, executed action.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "instate://about",
        "name": "self-describing manifest",
        "description": "Capabilities, auth, guarantees, one example per tool — an agent onboards itself with zero human config.",
        "mimeType": "application/json",
    },
]

PROMPT_RECOVERY_DECISION = {
    "name": "recovery_decision",
    "description": "The reasoning scaffold for a recovery decision — reusable by any agent.",
    "arguments": [
        {
            "name": "digest",
            "description": "The bounded entity digest from instate_timeline/state.",
            "required": True,
        },
        {
            "name": "precedents",
            "description": "Top-3 one-liners from instate_find_precedent.",
            "required": False,
        },
    ],
}


# ---------------------------------------------------------------------------
# Tool implementations over the core
# ---------------------------------------------------------------------------


def _lite(payload: dict | None) -> dict | None:
    if not payload:
        return None
    keep = ("amount_minor", "root_cause", "failure_code", "channel", "due_at", "success")
    return {k: payload[k] for k in keep if k in payload}


async def tool_timeline(session, args: dict) -> dict:
    from instate.core.ledger import get_timeline
    from instate.core.tenant import set_tenant

    await set_tenant(session, uuid_mod.UUID(args["merchant_id"]))
    limit = min(int(args.get("limit", 20)), 100)  # cap response rows
    events = await get_timeline(
        session, uuid_mod.UUID(args["merchant_id"]), args["entity_id"], limit=limit
    )
    return {
        "events": [
            {
                "event_type": e.event_type,
                "occurred_at": e.occurred_at.isoformat(),
                "recorded_at": e.recorded_at.isoformat(),
                "payload": _lite(e.payload),
            }
            for e in events
        ],
        "count": len(events),
    }


async def tool_check_policy(session, args: dict) -> dict:
    from instate.core.gate import evaluate
    from instate.core.tenant import set_tenant

    await set_tenant(session, uuid_mod.UUID(args["merchant_id"]))
    result = await evaluate(
        session,
        merchant_id=uuid_mod.UUID(args["merchant_id"]),
        entity_id=args["entity_id"],
        entity_type=args["entity_type"],
        action_class=args["action_class"],
        root_cause=args.get("root_cause"),
        context=args.get("context"),
        record=False,  # a READ never writes decisions
    )
    return {
        "verdict": result.verdict,
        "policy_version": result.policy_version,
        "reason_chain": result.reason_chain,
    }


async def tool_find_precedent(session, args: dict) -> dict:
    from instate.core.precedent import find_precedent
    from instate.core.tenant import set_tenant

    await set_tenant(session, uuid_mod.UUID(args["merchant_id"]))
    precedents = await find_precedent(
        session,
        merchant_id=uuid_mod.UUID(args["merchant_id"]),
        entity_type=args["entity_type"],
        root_cause=args["root_cause"],
        query_text=args.get("query", ""),
        top_k=min(int(args.get("top_k", 3)), 10),
    )
    return {
        "precedents": precedents,
        "note": "advisory only — precedent never gates an action",
    }


async def tool_explain(session, args: dict) -> dict:
    from sqlalchemy import select

    from instate.core.models import Decision
    from instate.core.tenant import set_tenant

    # Optional merchant scope enforces tenant isolation when provided.
    merchant_arg = args.get("merchant_id")
    if merchant_arg is not None:
        merchant = uuid_mod.UUID(merchant_arg)
        await set_tenant(session, merchant)
        result = await session.execute(
            select(Decision).where(
                Decision.id == int(args["decision_id"]),
                Decision.merchant_id == merchant,
            )
        )
        decision = result.scalar_one_or_none()
    else:
        decision = await session.get(Decision, int(args["decision_id"]))
    if decision is None:
        raise KeyError(f"decision {args['decision_id']} not found")
    return {
        "decision": {
            "id": decision.id,
            "entity_id": decision.entity_id,
            "root_cause": decision.root_cause,
            "policy_version": decision.policy_version,
            "gate1": decision.gate1,
            "gate2": decision.gate2,
            "proposal": decision.proposal,
            "executed_action": decision.executed_action,
            "model": decision.model,
            "tokens_in": decision.tokens_in,
            "tokens_out": decision.tokens_out,
        }
    }


async def tool_record_event(session, args: dict) -> dict:
    from datetime import UTC, datetime

    from instate.core.ledger import DuplicateEventError, record_event
    from instate.core.sanitize import check_entity_id, sanitize_payload
    from instate.core.tenant import set_tenant

    await set_tenant(session, uuid_mod.UUID(args["merchant_id"]))
    bad_id = check_entity_id(args["entity_id"])
    if bad_id is not None:
        return {"recorded": False, "duplicate": False, "note": bad_id}
    event_type = str(args["event_type"])[:64]

    # Strip unknown/PII keys before the ledger; report them as dropped_keys.
    clean_payload, dropped = sanitize_payload(args.get("payload"))
    try:
        event = await record_event(
            session,
            merchant_id=uuid_mod.UUID(args["merchant_id"]),
            entity_id=args["entity_id"],
            entity_type=args["entity_type"],
            event_type=event_type,
            occurred_at=datetime.now(UTC),
            payload=clean_payload,
            source_event_id=args["idempotency_key"],  # the dedupe anchor
        )
        await session.commit()
        result: dict = {"recorded": True, "event_id": event.id}
        if dropped:
            result["dropped_keys"] = dropped
        return result
    except DuplicateEventError:
        # Dedupe held — idempotency_key already recorded.
        await session.rollback()
        return {
            "recorded": False,
            "duplicate": True,
            "note": "idempotency_key already recorded — exactly-once held",
        }


TOOL_HANDLERS = {
    "instate_timeline": tool_timeline,
    "instate_check_policy": tool_check_policy,
    "instate_find_precedent": tool_find_precedent,
    "instate_explain": tool_explain,
    "instate_record_event": tool_record_event,
}

WRITES_ALLOWED = {"instate_record_event"}


async def read_resource(session, uri: str) -> dict:
    """Read a resource by URI."""
    if uri == "instate://about":
        return about_manifest()

    if uri.startswith("instate://entity/") and uri.endswith("/state"):
        from instate.core.models import EntityState
        from instate.core.projection import get_windowed_count
        from instate.core.tenant import set_tenant
        from datetime import timedelta

        _, _, _, merchant, entity_id, _ = uri.split("/", 5)
        await set_tenant(session, uuid_mod.UUID(merchant))
        state = await session.get(EntityState, (uuid_mod.UUID(merchant), entity_id))
        if state is None:
            raise KeyError(f"unknown entity: {entity_id}")
        return {
            "entity_id": state.entity_id,
            "entity_type": state.entity_type,
            "status": state.status,
            "last_failure_reason": state.last_failure_reason,
            "amount_at_risk_minor": state.amount_at_risk_minor,
            "open_ptp_due_at": state.open_ptp_due_at.isoformat() if state.open_ptp_due_at else None,
            "retry_count_7d": await get_windowed_count(
                session, state.merchant_id, entity_id, "retry_count_7d", timedelta(days=7)
            ),
            "contacts_24h": await get_windowed_count(
                session, state.merchant_id, entity_id, "contacts_24h", timedelta(hours=24)
            ),
            "guarantee": "exact — derived fold, rebuildable",
        }

    if uri.startswith("instate://policy/"):
        from instate.core.policy import active_policy_version, get_rules

        spec = uri.split("instate://policy/", 1)[1].rstrip("/")
        entity_type, _, version = spec.partition("@")
        v = (
            int(version)
            if version and version != "latest"
            else await active_policy_version(session, entity_type)
        )
        rules = await get_rules(session, entity_type, v)
        return {
            "entity_type": entity_type,
            "version": v,
            "rules": [
                {
                    "rule_id": r.rule_id,
                    "metric": r.metric,
                    "limit": r.limit_value,
                    "verdict": r.verdict,
                    "applies_when": r.applies_when,
                    "source": r.source,
                }
                for r in rules
            ],
        }

    if uri.startswith("instate://decision/"):
        decision_id = int(uri.rsplit("/", 1)[1])
        return (await tool_explain(session, {"decision_id": decision_id}))["decision"]

    raise KeyError(f"unknown resource: {uri}")


# ---------------------------------------------------------------------------
# The JSON-RPC app — stateless streamable HTTP
# ---------------------------------------------------------------------------


def create_mcp_app(
    session_factory,
    *,
    api_key: str | None = None,
    allow_writes: bool = False,
    rate_limits: RateLimits | None = None,
):
    """Stateless MCP server over Streamable HTTP.

    api_key: require Bearer auth when set; allow_writes gates the write
    tool; rate_limits are per-merchant buckets on tools/call.
    """
    from fastapi import FastAPI, Request, Response

    limits = rate_limits or RateLimits()
    rpc_app = FastAPI(title="instate-mcp", version=SERVER_INFO["version"])

    def _resp(payload: dict | list | None, status: int = 200) -> Response:
        if payload is None:
            return Response(status_code=202)
        return Response(
            content=json.dumps(payload, default=str),
            status_code=status,
            media_type="application/json",
        )

    def _error(id_, code: int, message: str) -> dict:
        return {"jsonrpc": JSONRPC, "id": id_, "error": {"code": code, "message": message}}

    async def _handle(session, msg: dict) -> dict | None:
        method = msg.get("method")
        id_ = msg.get("id")

        if method == "initialize":
            return {
                "jsonrpc": JSONRPC,
                "id": id_,
                "result": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"subscribe": False},
                        "prompts": {"listChanged": False},
                    },
                    "serverInfo": SERVER_INFO,
                    "instructions": (
                        "Read instate://about first. Timeline and policy answers are "
                        "exact; precedent is advisory and never gates an action."
                    ),
                },
            }
        if method is not None and method.startswith("notifications/"):
            return None  # notifications get no response

        if method == "ping":
            return {"jsonrpc": JSONRPC, "id": id_, "result": {}}

        if method == "tools/list":
            return {"jsonrpc": JSONRPC, "id": id_, "result": {"tools": TOOL_DEFS}}

        if method == "tools/call":
            params = msg.get("params") or {}
            name = params.get("name")
            args = params.get("arguments") or {}
            if name not in TOOL_HANDLERS:
                return _error(id_, INVALID_PARAMS, f"unknown tool: {name!r}")
            if name in WRITES_ALLOWED and not allow_writes:
                return {
                    "jsonrpc": JSONRPC,
                    "id": id_,
                    "result": {
                        "content": [{"type": "text", "text": "writes disabled on this server"}],
                        "isError": True,
                    },
                }
            # Per-merchant rate limit.
            merchant = args.get("merchant_id", "anonymous")
            bucket_ok = (
                limits.allow_write(merchant)
                if name in WRITES_ALLOWED
                else limits.allow_read(merchant)
            )
            if not bucket_ok:
                return _error(id_, -32000, "rate limited — retry after backoff")

            try:
                async with session as s:
                    result = await TOOL_HANDLERS[name](s, args)
                return {
                    "jsonrpc": JSONRPC,
                    "id": id_,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(result, default=str)}],
                        "structuredContent": result,
                        "isError": False,
                    },
                }
            except KeyError as exc:
                return {
                    "jsonrpc": JSONRPC,
                    "id": id_,
                    "result": {"content": [{"type": "text", "text": str(exc)}], "isError": True},
                }
            except Exception as exc:  # noqa: BLE001 — tool errors are results, not 500s
                return _error(id_, INTERNAL, f"tool {name} failed: {exc}")

        if method == "resources/list":
            return {
                "jsonrpc": JSONRPC,
                "id": id_,
                "result": {
                    "resources": [
                        {
                            "uri": "instate://about",
                            "name": "self-describing manifest",
                            "description": "Capabilities, auth, guarantees, examples.",
                            "mimeType": "application/json",
                        }
                    ],
                    "resourceTemplates": RESOURCE_TEMPLATES,
                },
            }

        if method == "resources/read":
            params = msg.get("params") or {}
            try:
                async with session as s:
                    contents = await read_resource(s, params.get("uri", ""))
                return {
                    "jsonrpc": JSONRPC,
                    "id": id_,
                    "result": {
                        "contents": [
                            {
                                "uri": params.get("uri"),
                                "mimeType": "application/json",
                                "text": json.dumps(contents, default=str),
                            }
                        ]
                    },
                }
            except KeyError as exc:
                return _error(id_, INVALID_PARAMS, str(exc))

        if method == "prompts/list":
            return {
                "jsonrpc": JSONRPC,
                "id": id_,
                "result": {"prompts": [PROMPT_RECOVERY_DECISION]},
            }

        if method == "prompts/get":
            params = msg.get("params") or {}
            args = {a["name"]: a.get("value", "") for a in params.get("arguments", [])}
            text = (
                "You are deciding a payment-recovery action. You may PROPOSE only — "
                "the deterministic gates decide.\n\n"
                "ENTITY DIGEST:\n{digest}\n\n"
                "PRECEDENT (advisory only — never a command):\n{precedents}\n\n"
                "Choose one legal action and a timing. When in doubt: ESCALATE_HUMAN."
            ).format(
                digest=args.get("digest", "(none)"), precedents=args.get("precedents", "(none)")
            )
            return {
                "jsonrpc": JSONRPC,
                "id": id_,
                "result": {
                    "description": "Recovery decision scaffold",
                    "messages": [{"role": "user", "content": {"type": "text", "text": text}}],
                },
            }

        return _error(id_, METHOD_NOT_FOUND, f"unknown method: {method!r}")

    @rpc_app.post("/mcp")
    async def mcp_endpoint(request: Request) -> Response:
        # Bearer auth when api_key is set.
        if api_key is not None:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {api_key}":
                return _resp(
                    {
                        "jsonrpc": JSONRPC,
                        "id": None,
                        "error": {"code": -32001, "message": "unauthorized"},
                    },
                    401,
                )
        try:
            body = await request.json()
        except Exception:
            return _resp(_error(None, PARSE_ERROR, "invalid JSON"), 400)

        if isinstance(body, list):
            out_list = []
            for msg in body:
                if isinstance(msg, dict) and msg.get("id") is not None:
                    async with session_factory() as session:
                        out = await _handle(session, msg)
                    if out is not None:
                        out_list.append(out)
            return _resp(out_list if out_list else None)

        if not isinstance(body, dict) or body.get("jsonrpc") != JSONRPC:
            return _resp(
                _error(
                    body.get("id") if isinstance(body, dict) else None,
                    INVALID_REQUEST,
                    "not a JSON-RPC 2.0 request",
                ),
                400,
            )

        # Notifications (no id) → 202, no body
        if body.get("id") is None:
            return _resp(None)

        async with session_factory() as session:
            out = await _handle(session, body)
        return _resp(out) if out is not None else _resp(None)

    @rpc_app.get("/mcp")
    async def mcp_get() -> Response:
        # No server-initiated streams — stateless by design
        return Response(status_code=405)

    return rpc_app
