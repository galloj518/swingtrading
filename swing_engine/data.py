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


YF_TZ_CACHE_DIR = cfg.CACHE_DIR / "yfinance_tz"
YF_TZ_CACHE_DIR.mkdir(parents=True, exist_ok=True)
try:
    yf.set_tz_cache_location(str(YF_TZ_CACHE_DIR))
except Exception:
    pass


def _provider_symbol(symbol: str) -> str:
    """Map display symbols to provider-specific tickers when needed."""
    return cfg.DOWNLOAD_SYMBOL_OVERRIDES.get(symbol, symbol)


def _cache_path(symbol: str, timeframe: str) -> Path:
    """Generate dated cache path for a symbol/timeframe."""
    today = date.today().isoformat()
    return cfg.CACHE_DIR / f"{symbol}_{timeframe}_{today}.csv"


def _latest_cached_df(symbol: str, timeframe: str, max_age_days: int) -> pd.DataFrame:
    """Use the most recent cache file when a live fetch fails."""
    pattern = f"{symbol}_{timeframe}_*.csv"
    candidates = sorted(cfg.CACHE_DIR.glob(pattern), reverse=True)
    today = date.today()
    for path in candidates:
        stem = path.stem
        try:
            file_date = date.fromisoformat(stem.rsplit("_", 1)[-1])
        except ValueError:
            continue
        if (today - file_date).days > max_age_days:
            continue
        try:
            return pd.read_csv(path, parse_dates=["date"])
        except Exception:
            continue
    return pd.DataFrame()


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
        fallback = _latest_cached_df(symbol, "daily", max_age_days=7)
        if not fallback.empty:
            print(f"  WARNING: Using recent cached daily data for {symbol}")
            return fallback
        return pd.DataFrame()

    if raw.empty:
        print(f"  WARNING: No daily data for {symbol}")
        fallback = _latest_cached_df(symbol, "daily", max_age_days=7)
        if not fallback.empty:
            print(f"  WARNING: Using recent cached daily data for {symbol}")
            return fallback
        return pd.DataFrame()

    df = _normalize_df(raw)
    if not df.empty:
        try:
            df.to_csv(cp, index=False)
        except Exception:
            pass

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
        fallback = _latest_cached_df(symbol, "intra5m", max_age_days=3)
        if not fallback.empty:
            print(f"  WARNING: Using recent cached intraday data for {symbol}")
            return fallback
        return pd.DataFrame()

    if raw.empty:
        print(f"  WARNING: No intraday data for {symbol}")
        fallback = _latest_cached_df(symbol, "intra5m", max_age_days=3)
        if not fallback.empty:
            print(f"  WARNING: Using recent cached intraday data for {symbol}")
            return fallback
        return pd.DataFrame()

    df = _normalize_df(raw)
    if not df.empty:
        try:
            df.to_csv(cp, index=False)
        except Exception:
            pass

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


def fetch_earnings_date(symbol: str, force: bool = False) -> Optional[str]:
    """
    Auto-fetch the next earnings date for a symbol from yfinance.

    Results are cached for the day (12-hour TTL via date-based filename).
    Manual overrides in cfg.EARNINGS_CALENDAR always take precedence.

    Args:
        symbol: Ticker symbol.
        force: Bypass the daily cache and re-fetch.

    Returns:
        ISO date string of next earnings, or None if unavailable.
    """
    if not getattr(cfg, "AUTO_FETCH_EARNINGS", True):
        return None

    # Manual override always wins
    manual = cfg.EARNINGS_CALENDAR.get(symbol.upper())
    if manual:
        return manual

    import json as _json

    cp = cfg.CACHE_DIR / f"{symbol}_earnings_{date.today().isoformat()}.json"
    if cp.exists() and not force:
        try:
            cached = _json.loads(cp.read_text())
            return cached.get("earnings_date")
        except Exception:
            pass

    try:
        ticker = yf.Ticker(_provider_symbol(symbol))
        cal = ticker.calendar
        # yfinance returns either a DataFrame or a dict depending on version
        earnings_dates = None
        if cal is not None:
            if hasattr(cal, "get"):
                raw = cal.get("Earnings Date")
                if raw is not None:
                    earnings_dates = raw if hasattr(raw, "__iter__") else [raw]
            elif hasattr(cal, "columns") and "Earnings Date" in cal.columns:
                earnings_dates = cal["Earnings Date"].dropna().tolist()

        if earnings_dates:
            today_ts = pd.Timestamp.now().normalize()
            future = [
                pd.Timestamp(d) for d in earnings_dates
                if pd.Timestamp(d) >= today_ts
            ]
            if future:
                dt_str = min(future).strftime("%Y-%m-%d")
                try:
                    cp.write_text(_json.dumps({"earnings_date": dt_str}))
                except Exception:
                    pass
                return dt_str
    except Exception:
        pass

    # Cache a None result to avoid hammering the API on re-runs
    try:
        cp.write_text(_json.dumps({"earnings_date": None}))
    except Exception:
        pass
    return None


def load_macro_signals(force: bool = False) -> dict:
    """
    Fetch macro regime-overlay signals: VIX term structure, credit spreads,
    yield curve, and options skew proxy.

    Tickers used:
        ^VIX   — CBOE VIX (front-month implied vol)
        ^VIX3M — CBOE 3-Month VIX (term structure)
        HYG    — iShares High Yield Corp Bond ETF
        LQD    — iShares Investment Grade Corp Bond ETF
        ^IRX   — 13-Week T-Bill yield (3-month rate)
        ^TNX   — 10-Year Treasury yield
        ^SKEW  — CBOE SKEW Index (tail-risk demand)

    All data is cached daily alongside regular market data.
    Returns a best-effort dict — any missing component is skipped gracefully.
    """
    macro_tickers = getattr(cfg, "MACRO_SIGNAL_TICKERS", {
        "vix":  "^VIX",
        "vix3m": "^VIX3M",
        "hyg":  "HYG",
        "lqd":  "LQD",
        "irx":  "^IRX",
        "tnx":  "^TNX",
        "skew": "^SKEW",
    })

    fetched = {}
    for key, ticker in macro_tickers.items():
        try:
            df = load_daily(ticker, force=force)
            if not df.empty:
                fetched[key] = df
        except Exception:
            pass

    result: dict = {}

    # --- VIX term structure (VIX3M / VIX) ---
    if "vix" in fetched and "vix3m" in fetched:
        vix_close = float(fetched["vix"]["close"].iloc[-1])
        vix3m_close = float(fetched["vix3m"]["close"].iloc[-1])
        if vix_close > 0:
            ratio = round(vix3m_close / vix_close, 4)
            from .constants import ThresholdRegistry as TR
            result["vix_level"] = round(vix_close, 2)
            result["vix3m_level"] = round(vix3m_close, 2)
            result["vix_term_structure"] = ratio
            result["term_structure_contango"] = ratio >= 1.0
            result["term_structure_stress"] = ratio < TR.MACRO_STRESS_CONTANGO_INVERSION

    # --- Credit spread proxy (HYG vs LQD 20d returns) ---
    if "hyg" in fetched and "lqd" in fetched:
        try:
            from .constants import ThresholdRegistry as TR
            hyg_ret = float(fetched["hyg"]["close"].iloc[-1] / fetched["hyg"]["close"].iloc[-21] - 1) * 100
            lqd_ret = float(fetched["lqd"]["close"].iloc[-1] / fetched["lqd"]["close"].iloc[-21] - 1) * 100
            spread = round(hyg_ret - lqd_ret, 3)
            result["hyg_lqd_spread_20d"] = spread
            if spread < TR.MACRO_STRESS_CREDIT_WIDENING:
                result["credit_signal"] = "stressed"
            elif spread < -0.5:
                result["credit_signal"] = "widening"
            else:
                result["credit_signal"] = "normal"
        except Exception:
            result["credit_signal"] = "unknown"

    # --- Yield curve (10Y minus 3M) ---
    if "tnx" in fetched and "irx" in fetched:
        try:
            tnx = float(fetched["tnx"]["close"].iloc[-1])
            irx = float(fetched["irx"]["close"].iloc[-1])
            spread = round(tnx - irx, 3)
            result["yield_curve_spread"] = spread
            result["curve_inverted"] = spread < 0
        except Exception:
            pass

    # --- Skew ---
    if "skew" in fetched:
        try:
            from .constants import ThresholdRegistry as TR
            skew_val = float(fetched["skew"]["close"].iloc[-1])
            result["skew_level"] = round(skew_val, 1)
            result["skew_elevated"] = skew_val > TR.MACRO_STRESS_SKEW_ELEVATED
        except Exception:
            pass
    elif "vix" in fetched and "vix3m" in fetched:
        # Fallback skew proxy using term structure inversion
        result["skew_elevated"] = result.get("term_structure_stress", False)

    return result


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
