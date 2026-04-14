"""
Market data loading with CSV caching.
Primary source: yfinance. No authentication required.
"""
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from . import config as cfg


def _provider_symbol(symbol: str) -> str:
    """Map display symbols to provider-specific tickers when needed."""
    return cfg.DOWNLOAD_SYMBOL_OVERRIDES.get(symbol, symbol)


def _cache_path(symbol: str, timeframe: str) -> Path:
    """Generate dated cache path for a symbol/timeframe."""
    today = date.today().isoformat()
    return cfg.CACHE_DIR / f"{symbol}_{timeframe}_{today}.csv"


def _normalize_df(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a yfinance DataFrame to always have:
    date, open, high, low, close, volume
    with date as a timezone-naive datetime column.

    Handles both Ticker.history() and yf.download() output formats,
    including multi-level column headers from yf.download().
    """
    if raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    # Flatten multi-level columns (yf.download returns these)
    if isinstance(df.columns, pd.MultiIndex):
        # Take the first level (Price names: Open, High, Low, Close, Volume)
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    # reset_index brings the Date/Datetime index into a column
    df = df.reset_index()

    # Lowercase all column names
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Find the datetime column
    date_col = None
    for candidate in ["date", "datetime", "index"]:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col is None:
        for col in df.columns:
            if hasattr(df[col], "dt") or "date" in col:
                date_col = col
                break

    if date_col is None:
        date_col = df.columns[0]

    if date_col != "date":
        df = df.rename(columns={date_col: "date"})

    # Convert to datetime and strip timezone
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df["date"] = df["date"].dt.tz_localize(None)

    # Keep only the columns we need
    keep = ["date", "open", "high", "low", "close", "volume"]
    available = [c for c in keep if c in df.columns]
    df = df[available]

    # Drop rows where date or close is NaN
    df = df.dropna(subset=["date", "close"])
    df = df.sort_values("date").reset_index(drop=True)

    return df


def load_daily(symbol: str, force: bool = False) -> pd.DataFrame:
    """
    Load daily OHLCV candles.
    Uses yf.download() which returns today's partial bar during market hours.
    Returns DataFrame with columns: date, open, high, low, close, volume.
    """
    cp = _cache_path(symbol, "daily")
    if cp.exists() and not force:
        df = pd.read_csv(cp, parse_dates=["date"])
        return df

    end = date.today() + timedelta(days=1)  # include today
    start = end - timedelta(days=cfg.DAILY_LOOKBACK_DAYS)

    try:
        raw = yf.download(_provider_symbol(symbol), start=start.isoformat(), end=end.isoformat(),
                          interval="1d", auto_adjust=True, progress=False)
    except Exception as e:
        print(f"  WARNING: Failed to fetch daily data for {symbol}: {e}")
        return pd.DataFrame()

    if raw.empty:
        print(f"  WARNING: No daily data for {symbol}")
        return pd.DataFrame()

    df = _normalize_df(raw)
    if not df.empty:
        df.to_csv(cp, index=False)

    return df


def load_intraday(symbol: str, force: bool = False) -> pd.DataFrame:
    """
    Load 5-minute intraday candles (last 5 trading days).
    """
    cp = _cache_path(symbol, "intra5m")
    if cp.exists() and not force:
        df = pd.read_csv(cp, parse_dates=["date"])
        return df

    try:
        raw = yf.download(_provider_symbol(symbol), period=f"{cfg.INTRADAY_LOOKBACK_DAYS}d",
                          interval="5m", auto_adjust=True, progress=False)
    except Exception as e:
        print(f"  WARNING: Failed to fetch intraday data for {symbol}: {e}")
        return pd.DataFrame()

    if raw.empty:
        print(f"  WARNING: No intraday data for {symbol}")
        return pd.DataFrame()

    df = _normalize_df(raw)
    if not df.empty:
        df.to_csv(cp, index=False)

    return df


def build_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Resample daily OHLCV into weekly bars (Friday close)."""
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    df = df.set_index("date")
    weekly = df.resample("W-FRI").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["open"])
    return weekly.reset_index()


def load_all(symbol: str, force: bool = False) -> dict:
    """Load all timeframes for a symbol. Returns dict of DataFrames."""
    daily = load_daily(symbol, force)
    intra = load_intraday(symbol, force)
    weekly = build_weekly(daily)
    return {"daily": daily, "weekly": weekly, "intraday": intra}


def load_vix(force: bool = False) -> pd.DataFrame:
    """Load VIX daily data."""
    return load_daily(cfg.VIX_SYMBOL, force)


def clean_old_cache(days_old: int = 3):
    """Remove cache files older than N days."""
    cutoff = date.today() - timedelta(days=days_old)
    removed = 0
    for f in cfg.CACHE_DIR.glob("*.csv"):
        parts = f.stem.rsplit("_", 1)
        if len(parts) == 2:
            try:
                file_date = date.fromisoformat(parts[1])
                if file_date < cutoff:
                    f.unlink()
                    removed += 1
            except ValueError:
                pass
    if removed:
        print(f"  Cleaned {removed} old cache files")
