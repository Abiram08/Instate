"""Instate replay — the counterfactual policy simulator (§9).

Because L0 is immutable and L1 is derived, replaying decisions under a
DIFFERENT policy comes nearly free — roughly the promised forty lines on
top of event sourcing:

    $ instate replay --policy v2 --set retry_ceiling_7d=2
      vs v1:   recovered -₹X   compliance violations -Y   LLM calls -Z%

Mechanics: build policy v_(n+1) as the current version with the given
limits overridden, then re-evaluate every historical decision AT ITS
ORIGINAL DECISION TIME (bi-temporal — `now=decision.created_at`) and
diff the verdicts. Money impact is computed from what actually recovered
per decision: if the new policy would have DENIED an action that in fact
recovered money, that amount is projected-lost; DENYs that would have
prevented a doomed attempt are projected-saved (nothing recovered on
that decision anyway).

It answers the question every collections team has and none can
currently produce: "what does tightening the retry ceiling actually cost
us in recovered revenue?"
"""

from dataclasses import dataclass, field
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.gate import aggregate_verdict
from instate.core.models import Decision, EntityState, Event, Policy
from instate.core.policy import get_rules, metric_event_types
from instate.core.projection import get_windowed_count


@dataclass
class CounterfactualReport:
    policy_version_from: int
    policy_version_to: int
    decisions_replayed: int = 0
    verdict_changes: int = 0
    stricter: int = 0  # ALLOW/DENY → DENY/REQUIRE_HUMAN
    looser: int = 0
    projected_recovered_lost_minor: int = 0  # money new policy would have blocked
    projected_violations_avoided: int = 0  # doomed attempts the new policy stops
    examples: list[str] = field(default_factory=list)

    def summary_line(self) -> str:
        return (
            f"policy v{self.policy_version_from} → v{self.policy_version_to}: "
            f"{self.decisions_replayed} decisions replayed, "
            f"{self.verdict_changes} verdict changes "
            f"({self.stricter} stricter / {self.looser} looser), "
            f"projected recovered delta -₹{self.projected_recovered_lost_minor / 100:,.0f}, "
            f"~{self.projected_violations_avoided} doomed attempts avoided"
        )


async def replay_with_policy(
    session: AsyncSession,
    *,
    overrides: dict[str, int],
    merchant_id: UUID | None = None,
) -> CounterfactualReport:
    """Re-decide history under an overridden policy. Read-only: it
    creates the shadow policy row(s) and reads — it never re-writes the
    ledger or the real decisions."""
    # Take the version in force for the entity types present (demo: one
    # type; the override applies at every type the rule exists on)
    q = select(Policy.entity_type, func.max(Policy.version)).group_by(Policy.entity_type)
    versions = dict((await session.execute(q)).all())
    if not versions:
        raise LookupError("no policy rows — seed policy first")
    from_version = max(versions.values())
    to_version = from_version + 1

    # Shadow policy: copy the in-force rows, apply the overrides
    rules = await session.execute(select(Policy).where(Policy.version == from_version))
    for rule in rules.scalars():
        new_limit = overrides.get(rule.rule_id, rule.limit_value)
        exists = await session.get(Policy, (to_version, rule.entity_type, rule.rule_id))
        if exists is None:
            session.add(
                Policy(
                    version=to_version,
                    entity_type=rule.entity_type,
                    rule_id=rule.rule_id,
                    metric=rule.metric,
                    limit_value=new_limit,
                    window_seconds=rule.window_seconds,
                    verdict=rule.verdict,
                    applies_when=rule.applies_when,
                    source=f"counterfactual shadow of v{from_version} ({rule.source})",
                )
            )
    await session.commit()

    # Replay every historical decision at its own decision time
    dq = select(Decision).where(Decision.policy_version == from_version)
    if merchant_id is not None:
        dq = dq.where(Decision.merchant_id == merchant_id)
    dq = dq.order_by(Decision.id.asc())
    decisions = list((await session.execute(dq)).scalars().all())

    report = CounterfactualReport(policy_version_from=from_version, policy_version_to=to_version)

    for decision in decisions:
        if decision.gate1 is None or decision.entity_id is None:
            continue
        old_verdict = aggregate_verdict(decision.gate1)
        if old_verdict == "ALLOW" and decision.executed_action is None:
            continue  # nothing was executed; the counterfactual is moot

        # What would the counterfactual chain look like, at that moment?
        new_chain: list[dict] = []
        state = await session.get(EntityState, (decision.merchant_id, decision.entity_id))
        entity_type = state.entity_type if state else "subscription"
        rules_cf = await get_rules(session, entity_type, to_version)
        # NOTE: rules_at() below re-derives observed counts at the decision
        # time using the stored rule shapes — the honest re-fold. BOTH the
        # window anchor and the knowledge cutoff sit at created_at, so a
        # late-recorded event cannot rewrite what was known then (§1b).
        for rule in rules_cf:
            if rule.metric is None:
                continue  # context rules can't be re-derived without context
            observed = await get_windowed_count(
                session,
                decision.merchant_id,
                decision.entity_id,
                rule.metric,
                timedelta(seconds=rule.window_seconds or 0),
                event_types=metric_event_types(rule.metric),
                now=decision.created_at,
                as_of=decision.created_at,
            )
            fired = observed >= rule.limit_value
            new_chain.append(
                {
                    "rule_id": rule.rule_id,
                    "metric": rule.metric,
                    "observed": observed,
                    "limit": rule.limit_value,
                    "verdict": rule.verdict if fired else "ALLOW",
                }
            )
        new_verdict = aggregate_verdict(new_chain) if new_chain else "ALLOW"

        report.decisions_replayed += 1
        if new_verdict == old_verdict:
            continue
        report.verdict_changes += 1
        if old_verdict == "ALLOW":
            report.stricter += 1
            # This action actually executed. Did it recover money?
            recovered = await _recovered_for_decision(session, decision.id)
            if recovered:
                report.projected_recovered_lost_minor += recovered
                report.examples.append(
                    f"decision {decision.id} ({decision.entity_id}): ALLOW→{new_verdict}, "
                    f"-₹{recovered / 100:,.0f} recovered under v{from_version}"
                )
            else:
                report.projected_violations_avoided += 1
                report.examples.append(
                    f"decision {decision.id} ({decision.entity_id}): ALLOW→{new_verdict}, "
                    f"attempt recovered nothing — budget saved"
                )
        else:
            report.looser += 1
            report.examples.append(
                f"decision {decision.id} ({decision.entity_id}): "
                f"{old_verdict}→ALLOW — new policy is looser here"
            )

    return report


async def _recovered_for_decision(session: AsyncSession, decision_id: int) -> int:
    """Money this decision actually recovered (RetrySucceeded amounts)."""
    result = await session.execute(
        select(Event.payload).where(
            Event.decision_id == decision_id,
            Event.event_type.in_(["RetrySucceeded", "PromiseHonored", "HumanResolved"]),
        )
    )
    total = 0
    for (payload,) in result.all():
        total += (payload or {}).get("amount_minor") or 0
    return total
