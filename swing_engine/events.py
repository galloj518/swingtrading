"""
Event awareness: macro calendar and earnings proximity.
"""
from datetime import timedelta
import pandas as pd

from . import config as cfg


def get_event_context(as_of: str = None) -> dict:
    """
    Build event risk context for the current or given date.
    Events from TODAY are treated as 'happening/just passed' not 'imminent'.
    Only events TOMORROW or later are treated as upcoming risk.
    """
    ref = pd.Timestamp(as_of) if as_of else pd.Timestamp.now().normalize()
    tomorrow = ref + timedelta(days=1)
    lookahead = ref + timedelta(days=7)

    # Today's events (informational, not risk)
    today_events = []
    for dt_str, name, severity in cfg.MACRO_EVENTS:
        dt = pd.Timestamp(dt_str)
        if dt == ref:
            today_events.append({"date": dt_str, "event": name, "severity": severity, "status": "today"})

    # Future events (actual risk)
    upcoming = []
    for dt_str, name, severity in cfg.MACRO_EVENTS:
        dt = pd.Timestamp(dt_str)
        if tomorrow <= dt <= lookahead:
            days_away = (dt - ref).days
            upcoming.append({
                "date": dt_str,
                "event": name,
                "severity": severity,
                "days_away": days_away,
            })

    high_risk = any(e["severity"] == "high" and e["days_away"] <= 2
                    for e in upcoming)
    medium_risk = any(e["severity"] in ("high", "medium") and e["days_away"] <= 5
                      for e in upcoming)

    today_note = ""
    if today_events:
        names = ", ".join(e["event"] for e in today_events)
        today_note = f"TODAY: {names} (already happening/passed)"

    return {
        "reference_date": ref.strftime("%Y-%m-%d"),
        "today_events": today_events,
        "today_note": today_note,
        "upcoming_events": upcoming,
        "high_risk_imminent": high_risk,
        "elevated_risk": medium_risk,
        "event_count_7d": len(upcoming),
        "recommendation": (
            "REDUCE / TIGHTEN STOPS" if high_risk
            else "CAUTION - event risk ahead" if medium_risk
            else f"CLEAR - {today_note}" if today_note
            else "No imminent macro events"
        ),
    }


def get_earnings_flag(symbol: str, auto_fetch: bool = True) -> dict:
    """
    Check earnings proximity for non-benchmark symbols.

    Lookup order:
      1. Benchmarks → always has_earnings=False
      2. Manual cfg.EARNINGS_CALENDAR override (highest trust)
      3. Auto-fetched from yfinance (if AUTO_FETCH_EARNINGS=True)

    Args:
        symbol: Ticker symbol.
        auto_fetch: Whether to attempt live yfinance lookup on cache miss.

    Returns:
        Dict with keys: has_earnings, earnings_date, days_to_earnings,
        warning, note, source.
    """
    if symbol.upper() in cfg.BENCHMARK_SET:
        return {"has_earnings": False, "note": "Benchmark - skip", "source": "benchmark"}

    # Manual override
    earn_date = cfg.EARNINGS_CALENDAR.get(symbol.upper())
    source = "manual_config"

    # Auto-fetch if no manual override
    if not earn_date and auto_fetch:
        try:
            from . import data as _mdata
            earn_date = _mdata.fetch_earnings_date(symbol)
            if earn_date:
                source = "yfinance_auto"
        except Exception:
            pass

    if not earn_date:
        return {
            "has_earnings": False,
            "note": "No earnings date available",
            "source": "none",
        }

    days_to = (pd.Timestamp(earn_date) - pd.Timestamp.now().normalize()).days
    if days_to < 0:
        return {
            "has_earnings": False,
            "note": f"Passed ({earn_date})",
            "source": source,
        }

    return {
        "has_earnings": True,
        "earnings_date": earn_date,
        "days_to_earnings": days_to,
        "warning": days_to <= 5,
        "note": f"Earnings in {days_to}d ({earn_date})" + (" — HIGH RISK" if days_to <= 5 else ""),
        "source": source,
    }
