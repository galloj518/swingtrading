"""
Position sizing with correlation group risk limits.

Prevents over-concentration in correlated names.
"""
from . import config as cfg


def get_group(symbol: str) -> str:
    """Find which correlation group a symbol belongs to."""
    for group, members in cfg.CORRELATION_GROUPS.items():
        if symbol.upper() in [m.upper() for m in members]:
            return group
    return "ungrouped"


def calc_position_size(entry: float, stop: float, symbol: str = "",
                       existing_group_risk: float = 0.0,
                       leverage: float = 1.0,
                       avg_volume: float = 0.0,
                       avg_dollar_volume: float = 0.0,
                       rvol: float = 1.0) -> dict:
    """
    Calculate position size based on risk parameters and group limits.

    Args:
        entry: planned entry price
        stop: stop loss price
        symbol: ticker for group lookup
        existing_group_risk: $ risk already allocated to this group
        leverage: 1.0 for normal, 3.0 for SOXL etc.
    """
    risk_per_share = abs(entry - stop) * leverage
    if risk_per_share <= 0:
        return {"shares": 0, "risk_dollars": 0, "note": "Invalid stop"}

    # Account risk limit
    max_risk = cfg.ACCOUNT_SIZE * (cfg.MAX_RISK_PCT / 100)

    # Group risk limit
    group = get_group(symbol)
    max_group_risk = cfg.ACCOUNT_SIZE * (cfg.MAX_GROUP_RISK_PCT / 100)
    remaining_group_risk = max(0, max_group_risk - existing_group_risk)

    # Use the tighter constraint
    effective_max_risk = min(max_risk, remaining_group_risk)

    if leverage > 1:
        # Reduce size for leveraged instruments
        effective_max_risk *= (1.0 / leverage)

    raw_shares = int(effective_max_risk / risk_per_share)

    liquidity_multiplier = 1.0
    liquidity_status = "ok"
    liquidity_note = ""

    if avg_dollar_volume and avg_dollar_volume < cfg.MIN_AVG_DOLLAR_VOLUME:
        liquidity_multiplier = 0.0
        liquidity_status = "blocked"
        liquidity_note = f"Avg dollar volume ${avg_dollar_volume:,.0f} below minimum"
    elif avg_volume and avg_volume < cfg.MIN_AVG_DAILY_VOLUME:
        liquidity_multiplier = min(liquidity_multiplier, 0.5)
        liquidity_status = "reduced"
        liquidity_note = f"Avg volume {avg_volume:,.0f} below preferred liquidity"
    elif avg_dollar_volume and avg_dollar_volume < cfg.PREFERRED_AVG_DOLLAR_VOLUME:
        liquidity_multiplier = min(liquidity_multiplier, 0.75)
        liquidity_status = "reduced"
        liquidity_note = f"Avg dollar volume ${avg_dollar_volume:,.0f} below preferred"

    if rvol and rvol < 0.7:
        liquidity_multiplier = min(liquidity_multiplier, 0.75)
        liquidity_status = "reduced" if liquidity_multiplier > 0 else liquidity_status
        liquidity_note = liquidity_note or f"RVol {rvol:.2f} suggests thin current participation"

    shares = int(raw_shares * liquidity_multiplier)

    max_liquidity_shares = None
    if avg_volume:
        max_liquidity_shares = int(avg_volume * cfg.MAX_DAILY_VOLUME_PARTICIPATION_PCT)
        shares = min(shares, max_liquidity_shares)

    actual_risk = round(shares * risk_per_share, 2)

    return {
        "shares": shares,
        "base_shares": raw_shares,
        "risk_dollars": actual_risk,
        "risk_pct": round(actual_risk / cfg.ACCOUNT_SIZE * 100, 2),
        "dollar_exposure": round(shares * entry, 2),
        "pct_of_account": round(shares * entry / cfg.ACCOUNT_SIZE * 100, 1),
        "group": group,
        "group_risk_used": round(existing_group_risk + actual_risk, 2),
        "group_risk_remaining": round(max_group_risk - existing_group_risk - actual_risk, 2),
        "leverage": leverage,
        "avg_volume": round(avg_volume, 0) if avg_volume else 0,
        "avg_dollar_volume": round(avg_dollar_volume, 0) if avg_dollar_volume else 0,
        "rvol": round(rvol, 2) if rvol else 0,
        "liquidity_multiplier": round(liquidity_multiplier, 2),
        "liquidity_status": liquidity_status,
        "max_liquidity_shares": max_liquidity_shares,
        "note": " | ".join(filter(None, [
            f"Group '{group}': ${existing_group_risk:.0f} existing risk" if existing_group_risk > 0 else "",
            liquidity_note,
        ])),
    }
