"""
Portfolio-level Greeks tracking for an equity swing book.

Computes:
  - Portfolio beta (SPY-relative market exposure)
  - Sector concentration breakdown
  - VIX sensitivity (estimated P&L per 1-point VIX rise)
  - Open risk dollars and % of account
  - Correlation-cluster concentration

All calculations use numpy (already present) — no new dependencies.

Usage in pipeline:
    exposure = portfolio.calc_portfolio_exposure(open_trades, packets, spy_daily)
    db.save_portfolio_snapshot(exposure)
    dashboard renders exposure["summary"] table
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from . import config as cfg
from . import db


# ---------------------------------------------------------------------------
# Beta calculation
# ---------------------------------------------------------------------------

def calc_beta(
    symbol_daily: pd.DataFrame,
    spy_daily: pd.DataFrame,
    window: int = 60,
) -> float:
    """
    Compute rolling beta of a symbol vs SPY using OLS regression.

    Beta = cov(symbol_returns, spy_returns) / var(spy_returns)
    Approximated via numpy.polyfit on aligned return series.

    Args:
        symbol_daily: Daily OHLCV DataFrame with 'date' and 'close' columns.
        spy_daily: SPY daily OHLCV DataFrame.
        window: Number of trading days for the regression.

    Returns:
        Beta coefficient. Returns 1.0 on insufficient data.
    """
    if symbol_daily.empty or spy_daily.empty:
        return 1.0

    try:
        sym_ret = (
            symbol_daily.set_index("date")["close"]
            .tail(window + 1)
            .pct_change()
            .dropna()
        )
        spy_ret = (
            spy_daily.set_index("date")["close"]
            .tail(window + 1)
            .pct_change()
            .dropna()
        )

        # Align on common dates
        combined = pd.concat([sym_ret, spy_ret], axis=1, join="inner").dropna()
        combined.columns = ["sym", "spy"]

        if len(combined) < 20:
            return 1.0

        # OLS via polyfit: beta is slope of sym ~ spy
        slope, _ = np.polyfit(combined["spy"].values, combined["sym"].values, deg=1)
        beta = float(slope)

        # Cap extreme betas — leveraged ETFs aside, > 4 is likely noise
        return round(max(-4.0, min(4.0, beta)), 3)

    except Exception:
        return 1.0


# ---------------------------------------------------------------------------
# Sector lookup
# ---------------------------------------------------------------------------

def _sector_for(symbol: str) -> str:
    """Return the correlation-group name for a symbol (used as sector proxy)."""
    for group, members in cfg.CORRELATION_GROUPS.items():
        if symbol.upper() in [m.upper() for m in members]:
            return group
    return "other"


# ---------------------------------------------------------------------------
# Main exposure calculation
# ---------------------------------------------------------------------------

def calc_portfolio_exposure(
    open_trades: list[dict],
    packets: dict,
    spy_daily: pd.DataFrame,
    data_store: Optional[dict] = None,
) -> dict:
    """
    Compute portfolio-level exposure metrics for all open positions.

    Args:
        open_trades: List of trade dicts from db.get_open_trades().
        packets: Dict of {symbol: packet} from the daily run.
        spy_daily: SPY daily DataFrame (for beta computation).
        data_store: Optional pre-loaded {symbol: {"daily": df, ...}} cache.

    Returns:
        Exposure dict with aggregated metrics and a per-symbol breakdown.
    """
    if not open_trades:
        return _empty_exposure()

    account_size = cfg.ACCOUNT_SIZE

    per_symbol = []
    total_dollar_exposure = 0.0
    total_beta_exposure = 0.0
    total_open_risk = 0.0
    sector_breakdown: dict[str, float] = {}

    for trade in open_trades:
        sym = trade.get("symbol", "")
        action = str(trade.get("action", "buy")).lower()
        is_long = action not in ("sell", "short")
        entry = float(trade.get("entry_price") or 0)
        stop = float(trade.get("current_stop") or 0) or float(
            trade.get("stop_price") or 0
        )
        shares = int(trade.get("shares") or 0)

        if not sym or entry <= 0 or shares <= 0:
            continue

        # Get current price from packet or trade entry as fallback
        pkt = packets.get(sym, {})
        current_price = float(
            (pkt.get("daily") or {}).get("last_close") or entry
        )

        dollar_exposure = current_price * shares
        open_risk = abs(current_price - stop) * shares if stop > 0 else 0.0

        # Beta
        sym_daily = pd.DataFrame()
        if data_store and sym in data_store:
            sym_daily = data_store[sym].get("daily", pd.DataFrame())

        beta = calc_beta(sym_daily, spy_daily, window=60)
        beta_exposure = dollar_exposure * beta * (1 if is_long else -1)

        sector = _sector_for(sym)
        sector_breakdown[sector] = sector_breakdown.get(sector, 0.0) + dollar_exposure

        per_symbol.append({
            "symbol": sym,
            "shares": shares,
            "entry": entry,
            "current_price": round(current_price, 2),
            "dollar_exposure": round(dollar_exposure, 0),
            "open_risk_dollars": round(open_risk, 2),
            "beta": beta,
            "beta_adjusted_exposure": round(beta_exposure, 0),
            "sector": sector,
            "unrealised_pnl": round(((current_price - entry) if is_long else (entry - current_price)) * shares, 2),
        })

        total_dollar_exposure += dollar_exposure
        total_beta_exposure += beta_exposure
        total_open_risk += open_risk

    if not per_symbol:
        return _empty_exposure()

    portfolio_beta = round(
        total_beta_exposure / total_dollar_exposure, 3
    ) if total_dollar_exposure > 0 else 1.0

    # VIX sensitivity: estimated P&L per 1-point VIX rise
    # Approximation: 1pt VIX ≈ -0.9% SPY (empirical long-run average)
    # Portfolio loss = net_beta_exposure * -0.009 * account_size
    vix_one_pt_impact = round(
        -total_beta_exposure * 0.009, 0
    )

    max_single_pct = 0.0
    if total_dollar_exposure > 0:
        max_single_pct = round(
            max(p["dollar_exposure"] for p in per_symbol) / account_size * 100, 1
        )

    # Highest-correlated cluster exposure
    cluster_exposure = max(sector_breakdown.values()) if sector_breakdown else 0.0

    net_beta_pct = round(total_beta_exposure / account_size * 100, 1)

    return {
        "snapshot_date": date.today().isoformat(),
        "open_positions": len(per_symbol),
        "total_dollar_exposure": round(total_dollar_exposure, 0),
        "net_beta_exposure": round(total_beta_exposure, 0),
        "net_beta_pct": net_beta_pct,
        "portfolio_beta": portfolio_beta,
        "open_risk_dollars": round(total_open_risk, 2),
        "open_risk_pct": round(total_open_risk / account_size * 100, 2),
        "vix_1pt_impact_dollars": vix_one_pt_impact,
        "max_single_position_pct": max_single_pct,
        "largest_sector_exposure": round(cluster_exposure, 0),
        "largest_sector_pct": round(cluster_exposure / account_size * 100, 1) if account_size > 0 else 0,
        "sector_breakdown": {k: round(v, 0) for k, v in sorted(
            sector_breakdown.items(), key=lambda x: x[1], reverse=True
        )},
        "per_symbol": per_symbol,
        "warnings": _exposure_warnings(
            net_beta_pct=net_beta_pct,
            open_risk_pct=round(total_open_risk / account_size * 100, 2),
            max_single_pct=max_single_pct,
            largest_sector_pct=round(cluster_exposure / account_size * 100, 1),
        ),
    }


def _exposure_warnings(
    net_beta_pct: float,
    open_risk_pct: float,
    max_single_pct: float,
    largest_sector_pct: float,
) -> list[str]:
    """Generate caution flags when exposure metrics exceed thresholds."""
    warnings = []
    if net_beta_pct > 80:
        warnings.append(f"High beta exposure: {net_beta_pct:.0f}% of account (>80% threshold)")
    if open_risk_pct > 6:
        warnings.append(f"Total open risk {open_risk_pct:.1f}% exceeds 6% of account")
    if max_single_pct > 20:
        warnings.append(f"Single position {max_single_pct:.0f}% of account (>20%)")
    if largest_sector_pct > 35:
        warnings.append(f"Sector concentration {largest_sector_pct:.0f}% of account (>35%)")
    return warnings


def _empty_exposure() -> dict:
    return {
        "snapshot_date": date.today().isoformat(),
        "open_positions": 0,
        "total_dollar_exposure": 0.0,
        "net_beta_exposure": 0.0,
        "net_beta_pct": 0.0,
        "portfolio_beta": 0.0,
        "open_risk_dollars": 0.0,
        "open_risk_pct": 0.0,
        "vix_1pt_impact_dollars": 0.0,
        "max_single_position_pct": 0.0,
        "largest_sector_exposure": 0.0,
        "largest_sector_pct": 0.0,
        "sector_breakdown": {},
        "per_symbol": [],
        "warnings": [],
    }


def print_exposure_report(exposure: dict) -> None:
    """Print a formatted portfolio exposure report."""
    print("\n  PORTFOLIO EXPOSURE")
    print("  " + "=" * 50)
    print(f"  Open positions:      {exposure['open_positions']}")
    print(f"  Dollar exposure:    ${exposure['total_dollar_exposure']:>10,.0f}")
    print(f"  Net beta exposure:  ${exposure['net_beta_exposure']:>10,.0f}  ({exposure['net_beta_pct']:.1f}% of account)")
    print(f"  Portfolio beta:      {exposure['portfolio_beta']:>8.2f}")
    print(f"  Open risk:          ${exposure['open_risk_dollars']:>10,.2f}  ({exposure['open_risk_pct']:.2f}%)")
    print(f"  VIX +1pt impact:    ${exposure['vix_1pt_impact_dollars']:>10,.0f}")
    print(f"  Max single pos:      {exposure['max_single_position_pct']:.1f}%")

    if exposure["sector_breakdown"]:
        print("\n  Sector breakdown:")
        for sector, dollars in list(exposure["sector_breakdown"].items())[:5]:
            pct = dollars / cfg.ACCOUNT_SIZE * 100
            print(f"    {sector:<20} ${dollars:>10,.0f}  ({pct:.1f}%)")

    warnings = exposure.get("warnings", [])
    if warnings:
        print("\n  WARNINGS:")
        for w in warnings:
            print(f"    ! {w}")

    print()
