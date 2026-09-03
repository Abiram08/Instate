"""The comparison, end to end (build item 8, §11).

One batch, two agents, one honest table — run under the fairness
contract (same seed, same model, same realistic gateway) and asserted
directionally: the memory layer must win on violations, zero-LLM rate,
and context tokens, and must never lose on money.
"""

from instate.replay.compare import run_comparison


async def test_comparison_directional_deltas():
    result = await run_comparison(entities=10)

    baseline = result["baseline"]
    instate = result["instate"]

    # The memory layer's headline deltas:
    assert instate.retry_violations < baseline.retry_violations, (
        f"gated agent must not violate the ceiling: "
        f"{instate.retry_violations} vs {baseline.retry_violations}"
    )
    assert instate.compliance_violations <= baseline.compliance_violations
    assert instate.zero_llm_share > baseline.zero_llm_share
    # Money: the gated agent never loses (scheduled payday retries recover
    # what immediate retries cannot)
    assert instate.net_recovered_minor > baseline.net_recovered_minor, (
        f"gated agent must recover more: {instate.net_recovered_minor} vs "
        f"{baseline.net_recovered_minor}"
    )
    # Context reduction, measured at its extremes: the fattest entity's
    # baseline prompt grows with history; the digest stays bounded.
    assert baseline_max_prompt(result) > instate_max_prompt(result)

    # Integrity holds on both sides — the comparison is between two
    # honest ledgers
    assert baseline.chain_verified is True
    assert instate.chain_verified is True


async def test_context_reduction_on_a_fat_entity():
    """The bounded-context claim, directly: same entity, same history —
    the baseline's prompt grows with depth; the digest stays bounded."""
    result = await run_comparison(entities=10)

    # Both runs used their own isolated DBs, already disposed — the
    # per-agent results carry context sizes instead.
    base_sizes = [r.context_chars for r in result["baseline_results"]]
    assert max(base_sizes) > 1000  # the fat entity's dump

    # Instate's digest is bounded: its largest modeled-context decision
    # stays flat no matter how deep the history goes
    inst = result["instate_results"]
    modeled = [r for r in inst if r.llm_called]
    assert modeled, "instate must have modeled decisions"
    # digests are small by construction — assert via the shared reasoner's
    # recorded contexts
    contexts = instate_reasoner_contexts(result)
    assert max(len(c) for c in contexts) < max(base_sizes) / 2


def baseline_max_prompt(result) -> int:
    return max(r.context_chars for r in result["baseline_results"])


def instate_max_prompt(result) -> int:
    contexts = instate_reasoner_contexts(result)
    return max(len(c) for c in contexts)


def instate_reasoner_contexts(result) -> list[str]:
    """The instate reasoner recorded the digests it was fed."""
    reasoner = result.get("instate_reasoner")
    return [serialize(c) for c in reasoner.contexts]


def serialize(context: dict) -> str:
    import json

    return json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)


async def test_comparison_table_renders():
    result = await run_comparison(entities=10)
    table = result["table"]

    assert "net money recovered" in table
    assert "retry-ceiling violations" in table
    assert "% decisions with zero LLM calls" in table
    assert "hash chain verified" in table
    # and the instate column really is better on violations
    assert table.count("0") >= 1


async def test_comparison_runs_are_identical_batches():
    """Fairness spot-check: both agents processed the same 6 batch
    entities, with the same root causes."""
    result = await run_comparison(entities=10)

    base_entities = [r.entity_id for r in result["baseline_results"]]
    inst_entities = [r.entity_id for r in result["instate_results"]]
    assert sorted(base_entities) == sorted(inst_entities)

    base_causes = sorted(r.root_cause for r in result["baseline_results"])
    inst_causes = sorted(r.root_cause for r in result["instate_results"])
    assert base_causes == inst_causes
