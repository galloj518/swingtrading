"""
Automated exit recommendation engine.

Evaluates all open positions each morning and produces specific,
actionable recommendations: hold, adjust stop, take partial, or exit.

This is a decision-support tool, not an auto-execution system.
The trader reviews recommendations and acts manually.

CLI: python -m swing_engine exits
     python -m swing_engine exits NVDA    (single symbol)
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd

from . import config as cfg
from . import data as mdata
from . import db
from .constants import ThresholdRegistry as TR


# ---------------------------------------------------------------------------
# Core math
# ---------------------------------------------------------------------------

def calc_trailing_stop(
    entry: float,
    current_extreme: float,
    atr: float,
    atr_mult: float = None,
    is_long: bool = True,
) -> float:
    """
    ATR-based trailing stop.

    Stop = max(entry, current_high - atr_mult * atr)
    Never trails below entry (breakeven floor once in profit).

    Args:
        entry: Original entry price.
        current_extreme: Highest high since entry for longs, lowest low for shorts.
        atr: Current ATR value for the symbol.
        atr_mult: ATR multiplier (defaults to TR.EXIT_TRAILING_ATR_MULT).
        is_long: Long or short trade.

    Returns:
        Recommended trailing stop price.
    """
    mult = atr_mult if atr_mult is not None else TR.EXIT_TRAILING_ATR_MULT
    if is_long:
        raw_trail = current_extreme - mult * atr
        return round(max(entry, raw_trail), 2)
    raw_trail = current_extreme + mult * atr
    return round(min(entry, raw_trail), 2)


def calc_r_multiple(current_price: float, entry: float, stop: float, is_long: bool = True) -> float:
    """Current unrealised R-multiple for a long or short position."""
    risk = abs(entry - stop)
    if risk == 0:
        return 0.0
    pnl = (current_price - entry) if is_long else (entry - current_price)
    return round(pnl / risk, 3)


# ---------------------------------------------------------------------------
# Per-trade evaluation
# ---------------------------------------------------------------------------

def evaluate_open_trade(
    trade: dict,
    current_bar: dict,
    atr: float,
) -> dict:
    """
    Evaluate a single open trade and return an action recommendation.

    Args:
        trade: Row dict from the trades table (open_date, symbol, entry_price,
               stop_price, shares, partial_1_taken, partial_2_taken,
               current_stop, trailing_stop).
        current_bar: Today's price bar for the symbol
                     {"close": float, "high": float, "low": float}.
        atr: Current ATR for the symbol.

    Returns:
        Dict with keys:
            action: "hold" | "partial_exit_1" | "partial_exit_2" |
                    "trail_stop" | "full_exit_stop" | "full_exit_time" |
                    "full_exit_extended"
            recommended_price: Suggested exit price.
            new_stop: Updated stop level recommendation.
            current_r: Current unrealised R-multiple.
            days_held: Calendar days since entry.
            reason: Human-readable rationale.
            urgency: "immediate" | "today" | "monitor"
    """
    entry = float(trade.get("entry_price") or 0)
    original_stop = float(trade.get("stop_price") or 0)
    action = str(trade.get("action", "buy")).lower()
    partial_1_taken = bool(trade.get("partial_1_taken"))
    partial_2_taken = bool(trade.get("partial_2_taken"))

    # Use the most recently updated stop (could be a trailed stop)
    current_stop = float(trade.get("current_stop") or trade.get("stop_price") or 0)

    open_date_str = str(trade.get("open_date") or trade.get("date") or "")
    try:
        open_date = date.fromisoformat(open_date_str)
        days_held = (date.today() - open_date).days
    except (ValueError, TypeError):
        days_held = 0

    close = float(current_bar.get("close") or entry)
    high = float(current_bar.get("high") or close)
    low = float(current_bar.get("low") or close)
    highest_high_since_entry = float(current_bar.get("highest_high_since_entry") or high)
    lowest_low_since_entry = float(current_bar.get("lowest_low_since_entry") or low)

    is_long = action not in ("sell", "short")
    current_r = calc_r_multiple(close, entry, original_stop, is_long=is_long)

    # -----------------------------------------------------------------------
    # 1. Stop hit check (highest urgency — always check first)
    # -----------------------------------------------------------------------
    if is_long and low <= current_stop and current_stop > 0:
        return {
            "action": "full_exit_stop",
            "recommended_price": round(current_stop, 2),
            "new_stop": current_stop,
            "current_r": current_r,
            "days_held": days_held,
            "reason": f"Stop hit: low {low:.2f} crossed stop {current_stop:.2f}",
            "urgency": "immediate",
        }
    if not is_long and high >= current_stop and current_stop > 0:
        return {
            "action": "full_exit_stop",
            "recommended_price": round(current_stop, 2),
            "new_stop": current_stop,
            "current_r": current_r,
            "days_held": days_held,
            "reason": f"Stop hit: high {high:.2f} crossed stop {current_stop:.2f}",
            "urgency": "immediate",
        }

    # -----------------------------------------------------------------------
    # 2. Time stop — stale trade not working
    # -----------------------------------------------------------------------
    if days_held >= TR.EXIT_TIME_STOP_DAYS and current_r < (TR.EXIT_TIME_STOP_LOSS_PCT / 100.0):
        return {
            "action": "full_exit_time",
            "recommended_price": close,
            "new_stop": current_stop,
            "current_r": current_r,
            "days_held": days_held,
            "reason": (
                f"Time stop: {days_held}d held, {current_r:+.2f}R "
                f"(threshold: {TR.EXIT_TIME_STOP_LOSS_PCT}% loss after {TR.EXIT_TIME_STOP_DAYS}d)"
            ),
            "urgency": "today",
        }

    # -----------------------------------------------------------------------
    # 3. Maximum hold duration
    # -----------------------------------------------------------------------
    if days_held >= TR.EXIT_MAX_HOLD_DAYS and current_r < 0.5:
        return {
            "action": "full_exit_time",
            "recommended_price": close,
            "new_stop": current_stop,
            "current_r": current_r,
            "days_held": days_held,
            "reason": (
                f"Max hold ({TR.EXIT_MAX_HOLD_DAYS}d) reached with only {current_r:+.2f}R gain."
            ),
            "urgency": "today",
        }

    # -----------------------------------------------------------------------
    # 4. Partial exit 2 — price hit 2R
    # -----------------------------------------------------------------------
    if not partial_2_taken and current_r >= TR.EXIT_PARTIAL_2_AT_R:
        new_trail = calc_trailing_stop(
            entry,
            highest_high_since_entry if is_long else lowest_low_since_entry,
            atr,
            is_long=is_long,
        )
        return {
            "action": "partial_exit_2",
            "recommended_price": close,
            "new_stop": new_trail,
            "current_r": current_r,
            "days_held": days_held,
            "reason": (
                f"2R reached ({current_r:+.2f}R). Take second partial (1/3 size). "
                f"Trail remaining to {new_trail:.2f}."
            ),
            "urgency": "today",
        }

    # -----------------------------------------------------------------------
    # 5. Partial exit 1 — price hit 1R
    # -----------------------------------------------------------------------
    if not partial_1_taken and current_r >= TR.EXIT_PARTIAL_1_AT_R:
        new_trail = calc_trailing_stop(
            entry,
            highest_high_since_entry if is_long else lowest_low_since_entry,
            atr,
            is_long=is_long,
        )
        return {
            "action": "partial_exit_1",
            "recommended_price": close,
            "new_stop": new_trail,
            "current_r": current_r,
            "days_held": days_held,
            "reason": (
                f"1R reached ({current_r:+.2f}R). Take first partial (1/3 size). "
                f"Move stop to breakeven: {new_trail:.2f}."
            ),
            "urgency": "today",
        }

    # -----------------------------------------------------------------------
    # 6. Trail the stop once 1R has been achieved
    # -----------------------------------------------------------------------
    if current_r >= TR.EXIT_TRAIL_START_AT_R:
        new_trail = calc_trailing_stop(
            entry,
            highest_high_since_entry if is_long else lowest_low_since_entry,
            atr,
            is_long=is_long,
        )
        should_tighten = new_trail > current_stop if is_long else (current_stop == 0 or new_trail < current_stop)
        if should_tighten:
            return {
                "action": "trail_stop",
                "recommended_price": None,
                "new_stop": new_trail,
                "current_r": current_r,
                "days_held": days_held,
                "reason": f"Trail tightened: {current_stop:.2f} → {new_trail:.2f} (ATR-based)",
                "urgency": "monitor",
            }

    # -----------------------------------------------------------------------
    # 7. Hold — no action needed
    # -----------------------------------------------------------------------
    return {
        "action": "hold",
        "recommended_price": None,
        "new_stop": current_stop,
        "current_r": current_r,
        "days_held": days_held,
        "reason": f"No action: {current_r:+.2f}R, {days_held}d held",
        "urgency": "monitor",
    }


# ---------------------------------------------------------------------------
# Portfolio-wide exit scan
# ---------------------------------------------------------------------------

def run_exit_scan(
    data_store: Optional[dict] = None,
    symbol_filter: Optional[str] = None,
) -> list[dict]:
    """
    Evaluate all open positions and print/return exit recommendations.

    Args:
        data_store: Optional pre-loaded data store (avoids re-fetching).
        symbol_filter: If set, only evaluate this symbol.

    Returns:
        List of recommendation dicts (one per open trade).
    """
    db.initialize()
    open_trades = db.get_open_trades()

    if not open_trades:
        print("  EXITS: No open trades found")
        return []

    if symbol_filter:
        open_trades = [t for t in open_trades if t.get("symbol", "").upper() == symbol_filter.upper()]

    recommendations = []

    print(f"  EXITS: Evaluating {len(open_trades)} open position(s)...\n")
    print(f"  {'Symbol':<8} {'Days':>5} {'R':>7}  {'Action':<20}  Reason")
    print("  " + "-" * 75)

    for trade in open_trades:
        sym = trade.get("symbol", "?")

        # Load price data
        try:
            if data_store and sym in data_store:
                daily = data_store[sym].get("daily", pd.DataFrame())
            else:
                daily = mdata.load_daily(sym)
        except Exception:
            continue

        if daily.empty or len(daily) < 2:
            continue

        daily = daily.copy()
        if "date" in daily.columns:
            daily["date"] = pd.to_datetime(daily["date"])

        last = daily.iloc[-1]
        open_date_str = str(trade.get("open_date") or trade.get("date") or "")
        try:
            open_date = pd.Timestamp(open_date_str)
            since_open = daily[daily["date"] >= open_date]
            if since_open.empty:
                since_open = daily.tail(1)
        except Exception:
            since_open = daily.tail(1)

        current_bar = {
            "close": float(last.get("close", 0)),
            "high": float(last.get("high", 0)),
            "low": float(last.get("low", 0)),
            "highest_high_since_entry": float(since_open["high"].max()) if "high" in since_open.columns else float(last.get("high", 0)),
            "lowest_low_since_entry": float(since_open["low"].min()) if "low" in since_open.columns else float(last.get("low", 0)),
        }

        # ATR from the daily frame
        atr_col = daily.get("atr") if "atr" in daily.columns else None
        if atr_col is not None and not atr_col.empty:
            atr = float(atr_col.iloc[-1])
        else:
            # Rough ATR estimate: 1.5% of price
            atr = current_bar["close"] * 0.015

        rec = evaluate_open_trade(trade, current_bar, atr)
        rec["symbol"] = sym
        rec["trade_id"] = trade.get("id")
        rec["entry_price"] = trade.get("entry_price")
        rec["original_stop"] = trade.get("stop_price")
        rec["shares"] = trade.get("shares")

        recommendations.append(rec)

        urgency_icon = {"immediate": "!!!!", "today": "  ! ", "monitor": "    "}.get(rec["urgency"], "    ")
        print(
            f"  {sym:<8} {rec['days_held']:>5}d {rec['current_r']:>+6.2f}R  "
            f"{urgency_icon}{rec['action']:<16}  {rec['reason'][:50]}"
        )

        # Auto-update stop in DB for trail_stop recommendations
        if rec["action"] == "trail_stop" and rec.get("new_stop"):
            try:
                db.update_trade_stop(rec["trade_id"], rec["new_stop"])
            except Exception as e:
                print(f"    WARNING: Could not update stop for {sym}: {e}")

    print()

    # Summary
    actions = [r["action"] for r in recommendations]
    immediate = sum(1 for a in actions if "exit" in a)
    trailing = actions.count("trail_stop")
    partials = sum(1 for a in actions if "partial" in a)

    if immediate + trailing + partials > 0:
        print(
            f"  SUMMARY: {immediate} exit(s), {partials} partial(s), "
            f"{trailing} stop trail(s)\n"
        )

    return recommendations


def print_exit_report(recommendations: list[dict]) -> None:
    """Print a detailed exit report. Useful for end-of-day review."""
    if not recommendations:
        print("  No open positions.")
        return

    immediate = [r for r in recommendations if r["urgency"] == "immediate"]
    today = [r for r in recommendations if r["urgency"] == "today"]
    monitor = [r for r in recommendations if r["urgency"] == "monitor"]

    if immediate:
        print("\n  *** IMMEDIATE ACTION REQUIRED ***")
        for r in immediate:
            print(f"  {r['symbol']}: {r['action'].upper()} at {r.get('recommended_price', 'MKT')}"
                  f"  [{r['reason']}]")

    if today:
        print("\n  Today's actions:")
        for r in today:
            print(f"  {r['symbol']}: {r['action']} — {r['reason']}")

    if monitor:
        print("\n  Monitoring (no action):")
        for r in monitor:
            print(f"  {r['symbol']}: {r['current_r']:+.2f}R — {r['reason']}")
