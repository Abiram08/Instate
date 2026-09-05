"""Continuous eval: scheduled runs, alert on regression."""

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from instate.replay.evaluate import evaluate_golden_set


async def run_continuous_eval(
    session: AsyncSession,
    *,
    merchant_id,
    gateway,
    reasoner_factory,
    threshold: float = 0.85,
) -> dict:
    results = await evaluate_golden_set(
        session, merchant_id=merchant_id, gateway=gateway, reasoner_factory=reasoner_factory
    )
    acc = sum(r.passed for r in results) / max(len(results), 1)
    alert = acc < threshold
    return {"accuracy": acc, "alert": alert, "results": results, "threshold": threshold}


async def eval_loop(session_factory, merchant_id, gateway, reasoner_factory, interval_seconds: int = 3600):
    while True:
        async with session_factory() as session:
            report = await run_continuous_eval(session, merchant_id=merchant_id, gateway=gateway, reasoner_factory=reasoner_factory)
            if report["alert"]:
                print(f"[ALERT] accuracy {report['accuracy']:.0%} below {report['threshold']:.0%}")
        await asyncio.sleep(interval_seconds)
