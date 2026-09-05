"""Razorpay test-mode gateway.
Called only after outbox intent is committed; every call carries an idempotency key.
Timeout 10s, ≤3 attempts with backoff+jitter honoring Retry-After.
"""

import asyncio
import random
from typing import Protocol

import httpx

from instate.core.models import (
    ACTION_CHECK_METHOD_UPDATED,
    ACTION_REQUEST_PAYMENT_METHOD,
    ACTION_RETRY_BACKUP_METHOD,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    ACTION_SEND_PAYMENT_LINK,
    ACTION_UPDATE_MANDATE,
)

TEST_MODE_BASE_URL = "https://api.razorpay.com/v1"
TIMEOUT_SECONDS = 10.0
MAX_ATTEMPTS = 3


class GatewayResponse:
    """Execution result: completed | failed | unknown (reconciled later)."""

    def __init__(
        self,
        status: str,
        provider_ref: str | None = None,
        detail: str = "",
        amount_minor: int | None = None,
        data: dict | None = None,
    ):
        self.status = status
        self.provider_ref = provider_ref
        self.detail = detail
        self.amount_minor = amount_minor
        self.data = data or {}

    def __repr__(self) -> str:
        return f"GatewayResponse({self.status!r}, ref={self.provider_ref!r})"


class PaymentGateway(Protocol):
    """The agent depends on this, never on Razorpay specifics."""

    async def execute(
        self,
        action: str,
        *,
        entity_id: str,
        idempotency_key: str,
        payload: dict | None = None,
    ) -> GatewayResponse: ...

    async def lookup(self, idempotency_key: str) -> GatewayResponse | None:
        """Query by stored key for the boot reconciler."""
        ...


class RazorpayGateway:
    """Test-mode client. Keys come from config/env — never hardcoded."""

    def __init__(
        self,
        key_id: str,
        key_secret: str,
        base_url: str = TEST_MODE_BASE_URL,
    ):
        self._auth = (key_id, key_secret)
        self._base_url = base_url

    async def execute(
        self,
        action: str,
        *,
        entity_id: str,
        idempotency_key: str,
        payload: dict | None = None,
    ) -> GatewayResponse:
        headers = {"X-Razorpay-Idempotency-Key": idempotency_key}
        try:
            if action == ACTION_RETRY_NOW:
                return await self._post(
                    f"/payments/{entity_id}/retry",
                    body=payload or {},
                    headers=headers,
                )
            if action == ACTION_SEND_PAYMENT_LINK:
                return await self._post(
                    "/payment_links",
                    body={"reference_id": entity_id, **(payload or {})},
                    headers=headers,
                )
            if action == ACTION_REQUEST_PAYMENT_METHOD:
                return await self._post(
                    "/payment_links",
                    body={
                        "reference_id": entity_id,
                        "purpose": "method_update",
                        **(payload or {}),
                    },
                    headers=headers,
                )
            if action == ACTION_UPDATE_MANDATE:
                return await self._post(
                    f"/subscriptions/{entity_id}/update",
                    body=payload or {},
                    headers=headers,
                )
            if action == ACTION_RETRY_BACKUP_METHOD:
                # Same endpoint with the stored backup instrument.
                return await self._post(
                    f"/payments/{entity_id}/retry",
                    body={
                        "instrument": "backup",
                        "use_backup_instrument": True,
                        "zero_customer_action": True,
                        **(payload or {}),
                    },
                    headers=headers,
                )
            if action == ACTION_RETRY_SCHEDULED:
                # Scheduling is local; the due scan executes it later as RETRY_NOW.
                return GatewayResponse(
                    "completed",
                    provider_ref=idempotency_key,
                    detail="scheduled locally",
                )
            if action == ACTION_CHECK_METHOD_UPDATED:
                # Read-only probe; method_updated=True unblocks retries without burning an attempt.
                return await self._get_method_status(entity_id, headers=headers)
            return GatewayResponse("failed", detail=f"unsupported action {action!r}")
        except Exception as exc:  # noqa: BLE001 — failures become facts for the ledger
            return GatewayResponse("unknown", detail=str(exc))

    async def _get_method_status(self, entity_id: str, headers: dict) -> GatewayResponse:
        """Read-only method probe; never moves money or counts as an attempt."""
        async with httpx.AsyncClient(
            base_url=self._base_url, auth=self._auth, timeout=TIMEOUT_SECONDS
        ) as client:
            resp = await client.get(f"/payments/{entity_id}", headers=headers)
            if resp.status_code != 200:
                return GatewayResponse("failed", detail=f"method probe HTTP {resp.status_code}")
            body = resp.json() if resp.content else {}
            return GatewayResponse(
                "completed",
                provider_ref=body.get("id"),
                data={"method_updated": bool(body.get("method_updated", False))},
            )

    async def lookup(self, idempotency_key: str) -> GatewayResponse | None:
        """Probe for prior work by idempotency key."""
        try:
            async with httpx.AsyncClient(
                base_url=self._base_url, auth=self._auth, timeout=TIMEOUT_SECONDS
            ) as client:
                resp = await client.get("/payment_links", params={"reference_id": idempotency_key})
                if resp.status_code == 200:
                    items = resp.json().get("items", []) or []
                    if items:
                        return GatewayResponse("completed", provider_ref=items[0].get("id"))
                return None
        except Exception:
            return None

    async def _post(self, path: str, *, body: dict, headers: dict) -> GatewayResponse:
        """POST with timeout, ≤3 attempts, backoff+jitter honoring Retry-After. 4xx (except 429) is non-retryable."""
        last_detail = ""
        retry_after = 0.0
        async with httpx.AsyncClient(
            base_url=self._base_url, auth=self._auth, timeout=TIMEOUT_SECONDS
        ) as client:
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    resp = await client.post(path, json=body, headers=headers)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    last_detail = str(exc)
                    if attempt == MAX_ATTEMPTS:
                        return GatewayResponse("unknown", detail=last_detail)
                else:
                    if resp.status_code in (200, 201):
                        result = resp.json() if resp.content else {}
                        return GatewayResponse(
                            "completed",
                            provider_ref=result.get("id"),
                            amount_minor=result.get("amount"),
                        )
                    if resp.status_code == 429:
                        retry_after = float(resp.headers.get("Retry-After", 0) or 0)
                        last_detail = "rate limited"
                    elif resp.status_code >= 500:
                        last_detail = f"HTTP {resp.status_code}"
                    else:
                        return GatewayResponse(
                            "failed",
                            detail=f"HTTP {resp.status_code}: {resp.text[:200]}",
                        )
                if attempt < MAX_ATTEMPTS:
                    delay = max(retry_after, 2**attempt) + random.uniform(0, 0.5)
                    retry_after = 0.0
                    await asyncio.sleep(delay)
        return GatewayResponse("unknown", detail=last_detail)
