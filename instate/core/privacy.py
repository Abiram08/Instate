"""Network-scope privacy: patterns shareable after k merchants, optional epsilon noise (§15)."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from instate.core.models import Case

# k-threshold: patterns shareable after k distinct merchants.
K_THRESHOLD = 3  # distinct merchants before a pattern is network-shareable
PRODUCTION_K = 10
EPSILON = None  # set to e.g. 1.0 to add Laplace noise to counts
PRODUCTION_EPSILON = 1.0


def laplace_noise(epsilon: float | None) -> float:
    if epsilon is None:
        return 0.0
    import random

    u = random.random() - 0.5
    return - (1 / epsilon) * (1 if u >= 0 else -1) * __import__("math").log(1 - 2 * abs(u))


async def publishable_patterns(
    session: AsyncSession,
    k: int = K_THRESHOLD,
    epsilon: float | None = None,
) -> list[dict]:
    """Return (root_cause, action_taken) patterns seen by >=k merchants. Only these are eligible for scope='network'."""
    epsilon = EPSILON if epsilon is None else epsilon
    q = (
        select(Case.root_cause, Case.action_taken, func.count(func.distinct(Case.merchant_id)).label("merchants"))
        .where(Case.scope == "private")
        .group_by(Case.root_cause, Case.action_taken)
        .having(func.count(func.distinct(Case.merchant_id)) >= k)
    )
    rows = await session.execute(q)
    out = []
    for rc, action, merchants in rows.all():
        noisy = merchants + laplace_noise(epsilon)
        out.append({"root_cause": rc, "action_taken": action, "merchants": int(noisy)})
    return out
