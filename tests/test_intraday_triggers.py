from __future__ import annotations

import pandas as pd

from swing_engine import intraday_triggers


def test_intraday_triggers_handle_missing_close_vwap():
    bars = 10
    start = pd.Timestamp.today().normalize() + pd.Timedelta(hours=9, minutes=30)
    df = pd.DataFrame(
        {
            "date": pd.date_range(start=start, periods=bars, freq="5min"),
            "open": [1.0] * bars,
            "high": [1.1] * bars,
            "low": [0.9] * bars,
            "close": [pd.NA] * bars,
            "volume": [1000] * bars,
        }
    )
    result = intraday_triggers.evaluate_intraday_triggers(df, {}, None, {"intraday_freshness_label": "fresh"})
    assert result["trigger_state"] == "data_unavailable"
    assert result["primary"]["trigger_type"] is None
    assert result["primary"]["triggered_now"] is False


def test_intraday_triggers_return_structured_block_for_valid_data(synthetic_intraday_frame):
    result = intraday_triggers.evaluate_intraday_triggers(
        synthetic_intraday_frame,
        {"prior_day_high": float(synthetic_intraday_frame["high"].iloc[5])},
        float(synthetic_intraday_frame["high"].iloc[10]),
        {"intraday_freshness_label": "fresh"},
    )
    assert "primary" in result
    assert "trigger_type" in result["primary"]
    assert result["trigger_state"] in {"triggered", "watch", "not_ready", "failed"}
