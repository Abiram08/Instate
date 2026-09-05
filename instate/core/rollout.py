"""Staged rollout — canary a policy version before full rollout (§15)."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Policy


async def create_canary_version(
    session: AsyncSession,
    *,
    entity_type: str,
    overrides: dict[str, int],
    canary_merchants: list,
) -> int:
    """Copy active version to v+1 with overrides."""
    from sqlalchemy import func

    v = (await session.execute(select(func.max(Policy.version)).where(Policy.entity_type == entity_type))).scalar_one() or 0
    new_v = v + 1
    rows = await session.execute(select(Policy).where(Policy.version == v, Policy.entity_type == entity_type))
    for r in rows.scalars():
        session.add(
            Policy(
                version=new_v,
                entity_type=r.entity_type,
                rule_id=r.rule_id,
                metric=r.metric,
                limit_value=overrides.get(r.rule_id, r.limit_value),
                window_seconds=r.window_seconds,
                verdict=r.verdict,
                applies_when=r.applies_when,
                source=f"canary v{new_v} of v{v}: {r.source}",
            )
        )
    # persist canary set as a sentinel policy row
    session.add(
        Policy(
            version=new_v,
            entity_type=entity_type,
            rule_id="_canary_merchants",
            metric=None,
            limit_value=0,
            verdict="ALLOW",
            applies_when={"merchants": [str(m) for m in canary_merchants]},
            source="canary routing set",
        )
    )
    await session.flush()
    return new_v


def is_canary(merchant_id, canary_list) -> bool:
    return str(merchant_id) in {str(m) for m in canary_list}
