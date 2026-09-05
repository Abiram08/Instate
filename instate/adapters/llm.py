"""Single model call; decoding-constrained.
- gemini-3-flash-class model, id in config
- thinking off, temperature 0
- Literal-enum response_schema + validate_proposal in code
- failure → None → deterministic policy default
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
    """Structured model output."""

    action: ActionEnum
    timing: str = Field(default="T_PLUS_24H")
    rationale: str = Field(default="")
    confidence: float = Field(ge=0.0, le=1.0)
    # Optional contact channel; validated against ALLOWED_CHANNELS downstream.
    channel: str | None = Field(default=None)
    # Optional A/B variant; assigned by harness, never by the model.
    variant: str | None = Field(default=None)


KNOWN_TIMINGS = {"IMMEDIATE", "T_PLUS_1H", "T_PLUS_24H", "T_PLUS_48H", "NEXT_PAYDAY"}


def validate_proposal(raw: Any) -> dict | None:
    """Validate proposal against schema; None means fall back to policy default."""
    if isinstance(raw, Proposal):
        return raw.model_dump(exclude_none=True)
    if not isinstance(raw, dict):
        return None
    try:
        return Proposal.model_validate(raw).model_dump(exclude_none=True)
    except (ValidationError, TypeError):
        return None


class Reasoner(Protocol):
    """The agent depends on this, not on any SDK."""

    async def propose(self, context: dict) -> dict | None:
        """Return a validated proposal dict, or None on any failure."""
        ...


class GeminiReasoner:
    """Decoding-constrained single call. Requires google-genai."""

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
            # Any failure → None; caller uses policy default.
            return None

        return validate_proposal(
            getattr(response, "parsed", None) or getattr(response, "text", None)
        )


def _render_context(context: dict) -> str:
    import json

    return json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)
