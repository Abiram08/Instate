"""LLM adapter — the ONE model call in the runtime loop (§7 of architecture.md).

`Reasoner` is the protocol the agent depends on; `GeminiReasoner` is the
concrete implementation. The google-genai SDK is imported lazily so the
core (and every test) runs without it — tests inject fakes.

Config is exactly §7:
- `gemini-3-flash`-class model (a bounded, classification-shaped decision
  doesn't need a frontier model); model id kept in config
- thinking OFF (`thinking_budget=0`) — the gates already narrowed the choice
- temperature 0 — a decision, not a creative-writing exercise
- structured output enforced by decoding: `response_schema` from a Pydantic
  model whose `action` field is a closed Literal enum — the model physically
  cannot emit an action outside the taxonomy
- schema validated AGAIN in code (`validate_proposal`) before the proposal
  is trusted — defense in depth, and it catches SDK drift
- any failure → None → the caller falls back to the deterministic policy
  default. No retries, no fallback gymnastics, no drama.
"""

from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field, ValidationError


# The closed action space — also enforced at decoding time via this Literal
ActionEnum = Literal[
    "RETRY_NOW",
    "RETRY_SCHEDULED",
    "SEND_PAYMENT_LINK",
    "UPDATE_MANDATE",
    "REQUEST_PAYMENT_METHOD",
    "AWAIT_PROMISE",
    "ESCALATE_HUMAN",
]


class Proposal(BaseModel):
    """The model's structured output — never prose (§6 step 3)."""

    action: ActionEnum
    timing: str = Field(default="T_PLUS_24H")
    rationale: str = Field(default="")
    confidence: float = Field(ge=0.0, le=1.0)


# Human-readable timing values the agent understands downstream.
KNOWN_TIMINGS = {"IMMEDIATE", "T_PLUS_1H", "T_PLUS_24H", "T_PLUS_48H", "NEXT_PAYDAY"}


def validate_proposal(raw: Any) -> dict | None:
    """Schema-validate a proposal in code — defense in depth (§7).

    Returns the validated dict, or None if the payload is not a legal
    proposal. The caller treats None exactly like a model failure:
    fall back to the deterministic policy default.
    """
    if isinstance(raw, Proposal):
        return raw.model_dump()
    if not isinstance(raw, dict):
        return None
    try:
        return Proposal.model_validate(raw).model_dump()
    except (ValidationError, TypeError):
        return None


class Reasoner(Protocol):
    """The agent depends on this, not on any SDK."""

    async def propose(self, context: dict) -> dict | None:
        """Return a validated proposal dict, or None on any failure."""
        ...


class GeminiReasoner:
    """The one call, decoding-constrained (§7). Requires `google-genai`."""

    def __init__(
        self,
        model: str = "gemini-3-flash",
        api_key: str | None = None,
    ):
        from google import genai  # lazy — core runs without the SDK

        self._model = model
        self._client = genai.Client(api_key=api_key) if api_key else genai.Client()

    async def propose(self, context: dict) -> dict | None:
        system_instruction = (
            "You are a payment-recovery decision module. Given the entity "
            "digest and precedent one-liners, choose exactly one legal "
            "action and a timing. Be conservative: when in doubt, prefer "
            "ESCALATE_HUMAN. Output only the structured proposal."
        )
        prompt = f"{system_instruction}\n\nDECISION CONTEXT:\n{_render_context(context)}"
        try:
            from google.genai import types

            response = await self._client.aio.models.generate_content(
                model=self._model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    thinking_config=types.ThinkingConfig(thinking_budget=0),
                    response_mime_type="application/json",
                    response_schema=Proposal,
                ),
            )
        except Exception:
            # Timeout, quota, refusal, SDK drift — the failure path is the
            # deterministic policy default, not a retry loop (§7)
            return None

        return validate_proposal(
            getattr(response, "parsed", None) or getattr(response, "text", None)
        )


def _render_context(context: dict) -> str:
    """Compact, deterministic rendering of the decision context.

    Bounded by construction: state scalars + last-5 events + exactly
    `top_k` precedent one-liners (§7 token accounting). Sorted keys so
    identical inputs render byte-identically (cache-friendly).
    """
    import json

    return json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)
