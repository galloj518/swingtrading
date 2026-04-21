from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def make_bundle(daily: pd.DataFrame, intraday: pd.DataFrame | None = None) -> dict:
    intraday = intraday if intraday is not None else pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    weekly = (
        daily.set_index("date")
        .resample("W-FRI")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    daily_meta = {"source": "fixture", "fetched_at": datetime.now(timezone.utc).isoformat()}
    intraday_meta = {
        "source": "fixture" if not intraday.empty else "unavailable",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "freshness_label": "fresh" if not intraday.empty else "missing",
        "freshness_age_minutes": 1.0 if not intraday.empty else None,
    }
    return {
        "daily": daily.copy(),
        "weekly": weekly,
        "intraday": intraday.copy(),
        "meta": {"daily": daily_meta, "weekly": daily_meta, "intraday": intraday_meta},
    }
