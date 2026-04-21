from __future__ import annotations

import pandas as pd

from swing_engine import data


def test_normalize_none_returns_empty_frame():
    df = data._normalize_df(None)
    assert df.empty
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_normalize_missing_date_returns_empty_frame():
    raw = pd.DataFrame({"open": [1], "high": [2], "low": [0.5], "close": [1.5], "volume": [100]})
    df = data._normalize_df(raw)
    assert df.empty


def test_normalize_malformed_columns_is_defensive():
    raw = pd.DataFrame({"Date": ["2026-01-01"], "Close": ["10.5"], "Volume": ["bad"]})
    df = data._normalize_df(raw)
    assert len(df) == 1
    assert df.loc[0, "close"] == 10.5
    assert pd.isna(df.loc[0, "volume"])
