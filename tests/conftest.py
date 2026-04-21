from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def synthetic_daily_frame() -> pd.DataFrame:
    periods = 320
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=periods)
    base = np.linspace(100.0, 132.0, periods) + np.sin(np.linspace(0, 10, periods)) * 1.4
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.4,
            "high": base + 0.9,
            "low": base - 0.8,
            "close": base,
            "volume": np.full(periods, 1_250_000, dtype=float),
        }
    )


@pytest.fixture
def synthetic_intraday_frame() -> pd.DataFrame:
    bars = 30
    start = pd.Timestamp.today().normalize() + pd.Timedelta(hours=9, minutes=30)
    dates = pd.date_range(start=start, periods=bars, freq="5min")
    base = np.linspace(131.0, 133.0, bars) + np.sin(np.linspace(0, 5, bars)) * 0.15
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.1,
            "high": base + 0.2,
            "low": base - 0.2,
            "close": base,
            "volume": np.full(bars, 210_000, dtype=float),
        }
    )


@pytest.fixture
def workspace_tmp_path() -> Path:
    base = Path.cwd() / ".pytest_tmp"
    base.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(dir=base))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
