"""A/B link wording conversion per variant."""

from instate.replay.compare import run_ab_test


async def test_ab_variant_b_wins_on_hard_declines():
    result = await run_ab_test(entities=10)
    conv = result["conversion"]

    assert set(conv) == {"A", "B"}
    assert conv["A"]["sent"] > 0
    assert conv["B"]["sent"] == conv["A"]["sent"]
    assert conv["B"]["converted"] > conv["A"]["converted"]
    assert "winner: variant B" in result["table"]


async def test_ab_table_renders():
    result = await run_ab_test(entities=10)
    table = result["table"]
    assert "variant" in table
    assert "links sent" in table
    assert "converted" in table
    assert "rate" in table
