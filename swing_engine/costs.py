"""
Execution cost model: slippage, spread, and commission estimation.

Adjusts theoretical R:R to net-of-cost R:R so that position sizing
and signal scoring reflect real-world execution economics.

No new dependencies — pure Python math.
"""
from __future__ import annotations

from . import config as cfg


def _liquidity_tier(avg_dollar_volume: float) -> str:
    """Classify a symbol into a liquidity tier by average dollar volume."""
    if avg_dollar_volume >= cfg.PREFERRED_AVG_DOLLAR_VOLUME:
        return "liquid"
    if avg_dollar_volume >= cfg.MIN_AVG_DOLLAR_VOLUME:
        return "semiliquid"
    return "illiquid"


def calc_round_trip_cost(
    entry: float,
    shares: int,
    avg_dollar_volume: float,
    stop: float | None = None,
    target_1: float | None = None,
    target_2: float | None = None,
) -> dict:
    """
    Estimate the full round-trip execution cost for a position.

    Models three cost components:
      1. Entry slippage — market impact on the way in (bps of entry price).
      2. Half-spread — effective cost of crossing the bid-ask spread.
      3. Commission — flat per-trade or per-share (broker-configurable).
      4. Exit slippage — same model applied on exit.

    The function also computes how much these costs erode the theoretical R:R
    so the checklist and sizing logic can present net-of-cost trade economics.

    Args:
        entry: Planned entry price.
        shares: Number of shares.
        avg_dollar_volume: Symbol's 20-day average dollar volume.
        stop: Stop loss price (used for net R:R calc).
        target_1: First profit target (used for net R:R calc).
        target_2: Second profit target (used for net R:R calc).

    Returns:
        Dict with cost breakdown in dollars, bps, and net R:R figures.
    """
    cost_model = cfg.COST_MODEL
    tier = _liquidity_tier(avg_dollar_volume)

    slippage_bps = {
        "liquid":     cost_model["slippage_bps_liquid"],
        "semiliquid": cost_model["slippage_bps_semiliquid"],
        "illiquid":   cost_model["slippage_bps_illiquid"],
    }[tier]

    spread_bps = {
        "liquid":     cost_model["spread_bps_liquid"],
        "semiliquid": cost_model["spread_bps_semiliquid"],
        "illiquid":   cost_model["spread_bps_illiquid"],
    }[tier]

    # One-way cost in bps: entry slippage + half the spread
    one_way_bps = slippage_bps + spread_bps / 2.0
    # Round-trip: pay once in, once out
    round_trip_bps = one_way_bps * 2.0

    dollar_value = shares * entry
    slippage_dollars = round(dollar_value * round_trip_bps / 10_000, 2)

    commission = (
        cost_model["commission_per_share"] * shares * 2  # both legs
        + cost_model["commission_per_trade"] * 2
    )

    total_cost_dollars = round(slippage_dollars + commission, 2)
    cost_per_share = round(total_cost_dollars / shares, 4) if shares > 0 else 0.0

    # Effective entry price after entry slippage (entry + one-way slippage)
    effective_entry = round(entry * (1 + one_way_bps / 10_000), 4)

    # Net R:R figures (cost reduces gain and increases loss)
    net_rr_t1 = None
    net_rr_t2 = None
    breakeven_move_pct = None

    if stop is not None and shares > 0:
        raw_risk_per_share = abs(entry - stop)
        if raw_risk_per_share > 0:
            # Cost per share is split across risk and reward
            net_risk_per_share = raw_risk_per_share + cost_per_share

            if target_1 is not None:
                raw_reward_t1 = abs(target_1 - entry)
                net_reward_t1 = max(0.0, raw_reward_t1 - cost_per_share)
                net_rr_t1 = round(net_reward_t1 / net_risk_per_share, 2)

            if target_2 is not None:
                raw_reward_t2 = abs(target_2 - entry)
                net_reward_t2 = max(0.0, raw_reward_t2 - cost_per_share)
                net_rr_t2 = round(net_reward_t2 / net_risk_per_share, 2)

    # Breakeven = how much price must move just to cover costs
    if dollar_value > 0:
        breakeven_move_pct = round(total_cost_dollars / dollar_value * 100, 3)

    return {
        "liquidity_tier": tier,
        "slippage_bps_one_way": round(one_way_bps, 1),
        "round_trip_bps": round(round_trip_bps, 1),
        "slippage_dollars": slippage_dollars,
        "commission_dollars": round(commission, 2),
        "total_cost_dollars": total_cost_dollars,
        "cost_per_share": cost_per_share,
        "effective_entry": effective_entry,
        "breakeven_move_pct": breakeven_move_pct,
        "net_rr_t1": net_rr_t1,
        "net_rr_t2": net_rr_t2,
        "cost_pct_of_risk": round(
            cost_per_share / abs(entry - stop) * 100, 1
        ) if stop and abs(entry - stop) > 0 else None,
    }


def cost_summary_line(cost: dict) -> str:
    """One-line cost summary for the dashboard/checklist."""
    bps = cost["round_trip_bps"]
    dollars = cost["total_cost_dollars"]
    tier = cost["liquidity_tier"]
    be = cost.get("breakeven_move_pct", "?")
    return (
        f"Est. round-trip cost: {bps:.0f} bps (${dollars:.0f}) | "
        f"Tier: {tier} | Breakeven move: {be}%"
    )
