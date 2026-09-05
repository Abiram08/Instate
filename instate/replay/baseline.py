"""Stateless baseline for comparison: same diagnosis map, model, taxonomy, gateway, ledger.
Omits Gate-1/Gate-2, precedent, and bounded context; re-derives full raw history each decision."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.adapters.llm import validate_proposal
from instate.adapters.razorpay import PaymentGateway
from instate.agent.diagnose import diagnose
from instate.agent.execute import (
    escalate_to_human,
    execute_action,
    schedule_retry,
)
from instate.core.ledger import record_event
from instate.core.locks import get_entity_lock
from instate.core.models import (
    ACTION_ESCALATE_HUMAN,
    ACTION_RETRY_NOW,
    ACTION_RETRY_SCHEDULED,
    Decision,
    Event,
)
from instate.core.projection import fold_events


@dataclass
class BaselineResult:
    entity_id: str
    root_cause: str
    executed_action: str | None
    llm_called: bool
    gateway_called: bool
    tokens: int
    context_chars: int


class StatelessBaselineAgent:
    """Same model, no memory: re-derives full raw history every decision."""

    def __init__(self, reasoner, gateway: PaymentGateway):
        self.reasoner = reasoner
        self.gateway = gateway

    async def process_failure(
        self,
        session: AsyncSession,
        *,
        event: Event,
        now: datetime | None = None,
    ) -> BaselineResult:
        """Process one failure with no memory, same per-entity serialization."""
        async with get_entity_lock(event.merchant_id, event.entity_id):
            return await self._process_failure_inner(session, event=event, now=now)

    async def _process_failure_inner(
        self,
        session: AsyncSession,
        *,
        event: Event,
        now: datetime | None = None,
    ) -> BaselineResult:
        now = now or datetime.now(UTC)
        merchant_id = event.merchant_id
        entity_id = event.entity_id
        entity_type = event.entity_type

        # Same diagnosis map as the real pipeline.
        failure_code = (event.payload or {}).get("failure_code")
        root_cause = await diagnose(session, failure_code=failure_code)

        # Unbounded raw context, re-derived every time.
        result = await session.execute(
            select(Event)
            .where(Event.merchant_id == merchant_id, Event.entity_id == entity_id)
            .order_by(Event.id.asc())
        )
        history = [
            {
                "event_type": e.event_type,
                "occurred_at": e.occurred_at.isoformat(),
                "payload": e.payload,
            }
            for e in result.scalars().all()
        ]
        context = {"entity_id": entity_id, "history": history}
        rendered = json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)

        raw = await self.reasoner.propose(context)
        proposal = validate_proposal(raw)
        llm_called = proposal is not None
        if proposal is None:
            # Default with no memory: retry immediately.
            proposal = {
                "action": ACTION_RETRY_NOW,
                "timing": "IMMEDIATE",
                "rationale": "baseline default — no memory, no policy fallback",
                "confidence": None,
            }

        usage = getattr(self.reasoner, "last_usage", None)
        # Input tokens = full rendered context; grows with history depth.
        tokens_in = len(rendered) // 4
        tokens_out = usage[1] if usage else 60

        # Decision row for metrics parity (no gate chains).
        decision = Decision(
            merchant_id=merchant_id,
            entity_id=entity_id,
            root_cause=root_cause,
            prompt_text=rendered,
            proposal=proposal,
            model=getattr(self.reasoner, "model_name", None),
            tokens_in=tokens_in,
            tokens_out=tokens_out,
        )
        session.add(decision)
        await session.flush()

        await record_event(
            session,
            merchant_id=merchant_id,
            entity_id=entity_id,
            entity_type=entity_type,
            event_type="FailureDiagnosed",
            occurred_at=now,
            payload={
                "root_cause": root_cause,
                "failure_code": failure_code,
                "trigger_event_id": event.id,
            },
            source_event_id=f"{event.id}:diag",
            decision_id=decision.id,
        )

        action = proposal["action"]
        if action == ACTION_ESCALATE_HUMAN:
            await escalate_to_human(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                decision=decision,
                reason="model_choice",
                now=now,
            )
            gateway_called = False
        elif action == ACTION_RETRY_SCHEDULED:
            await schedule_retry(
                session,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                decision=decision,
                timing=proposal.get("timing"),
                root_cause=root_cause,
                now=now,
            )
            gateway_called = False
        else:
            await execute_action(
                session,
                gateway=self.gateway,
                merchant_id=merchant_id,
                entity_id=entity_id,
                entity_type=entity_type,
                decision=decision,
                action=action,
                proposal=proposal,
                now=now,
            )
            gateway_called = True

        await fold_events(session)
        return BaselineResult(
            entity_id=entity_id,
            root_cause=root_cause,
            executed_action=action,
            llm_called=llm_called,
            gateway_called=gateway_called,
            tokens=tokens_in + tokens_out,
            context_chars=len(rendered),
        )
