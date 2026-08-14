"""
Market data loading with freshness-aware caching.

Daily data remains cheap and date-oriented. Intraday data is cached with
machine-readable freshness metadata so repeated same-day scans can detect stale
snapshots instead of assuming "same date" means "fresh enough."
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from . import config as cfg
from . import market_hours
from .runtime_logging import get_logger, log_event


YF_TZ_CACHE_DIR = cfg.CACHE_DIR / "yfinance_tz"
YF_TZ_CACHE_DIR.mkdir(parents=True, exist_ok=True)
try:
    yf.set_tz_cache_location(str(YF_TZ_CACHE_DIR))
except Exception:
    pass

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")
EMPTY_MARKET_FRAME = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
_MEMORY_CACHE: Dict[Tuple[str, str], Tuple[pd.DataFrame, dict]] = {}
_CONSECUTIVE_FETCH_FAILURES = 0
_LIVE_FETCH_DISABLED_UNTIL:Optional[datetime] = None
LOGGER = get_logger()


def _provider_symbol(symbol: str) -> str:
    return cfg.DOWNLOAD_SYMBOL_OVERRIDES.get(symbol, symbol)


def _cache_base(symbol: str, timeframe: str) -> Path:
    safe_symbol = symbol.replace("^", "IDX_").replace(".", "_")
    return cfg.CACHE_DIR / f"{safe_symbol}_{timeframe}"


def _safe_symbol_name(symbol: str) -> str:
    return symbol.replace("^", "IDX_").replace(".", "_")


def _cache_paths(symbol: str, timeframe: str) -> Tuple[Path, Path]:
    base = _cache_base(symbol, timeframe)
    return base.with_suffix(".csv"), base.with_suffix(".json")


def _empty_market_frame() -> pd.DataFrame:
    return EMPTY_MARKET_FRAME.copy()


def _merge_history_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    usable = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return _empty_market_frame()
    merged = pd.concat(usable, ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = (
        merged.dropna(subset=["date", "close"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    if merged.empty:
        return _empty_market_frame()
    return merged


def _import_candidates(symbol: str) -> list[Path]:
    safe_symbol = _safe_symbol_name(symbol)
    return [
        cfg.DAILY_IMPORTS_DIR / f"{symbol}.csv",
        cfg.DAILY_IMPORTS_DIR / f"{safe_symbol}.csv",
        cfg.DAILY_IMPORTS_DIR / f"{symbol}.parquet",
        cfg.DAILY_IMPORTS_DIR / f"{safe_symbol}.parquet",
    ]


def _read_import_history(symbol: str) -> Tuple[pd.DataFrame, dict]:
    for path in _import_candidates(symbol):
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".parquet":
                raw = pd.read_parquet(path)
            else:
                raw = pd.read_csv(path)
            frame = _normalize_df(raw)
            if frame.empty:
                continue
            meta = {
                "symbol": symbol,
                "timeframe": "daily_import",
                "source": "local_import",
                "import_path": str(path),
                "bars": int(len(frame)),
                "request_start": pd.Timestamp(frame["date"].min()).strftime("%Y-%m-%d"),
                "request_end": pd.Timestamp(frame["date"].max()).strftime("%Y-%m-%d"),
            }
            return _clone_cached(frame, meta), meta
        except Exception as exc:
            log_event(LOGGER, 30, "market_data_import_failed", symbol=symbol, path=str(path), error=type(exc).__name__)
    return pd.DataFrame(), {}


def _write_import_history(symbol: str, df: pd.DataFrame) -> Path:
    path = cfg.DAILY_IMPORTS_DIR / f"{_safe_symbol_name(symbol)}.csv"
    df.to_csv(path, index=False)
    return path


def _serialize_timestamp(value:Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _parse_timestamp(value:Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _read_cache(symbol: str, timeframe: str) -> Tuple[pd.DataFrame, dict]:
    csv_path, meta_path = _cache_paths(symbol, timeframe)
    if not csv_path.exists():
        return pd.DataFrame(), {}
    try:
        df = pd.read_csv(csv_path, parse_dates=["date"])
    except Exception:
        return pd.DataFrame(), {}
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    return df, meta


def _write_cache(symbol: str, timeframe: str, df: pd.DataFrame, meta: dict) -> None:
    csv_path, meta_path = _cache_paths(symbol, timeframe)
    df.to_csv(csv_path, index=False)
    meta_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")


def _normalize_df(raw) -> pd.DataFrame:
    if raw is None or not isinstance(raw, pd.DataFrame) or raw.empty:
        return _empty_market_frame()
    df = raw.copy()
    has_explicit_date = any(str(column).lower().strip() in {"date", "datetime"} for column in df.columns)
    has_datetime_index = isinstance(df.index, pd.DatetimeIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    if not has_explicit_date and not has_datetime_index:
        return _empty_market_frame()
    df = df.reset_index()
    df.columns = [str(c).lower().strip() for c in df.columns]
    for candidate in ("date", "datetime", "index"):
        if candidate in df.columns:
            df = df.rename(columns={candidate: "date"})
            break
    if "date" not in df.columns:
        return _empty_market_frame()
    for column in ("open", "high", "low", "close", "volume"):
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None)
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df = df.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if df.empty:
        return _empty_market_frame()
    return df


def _attach_meta(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    df = df.copy()
    df.attrs["cache_meta"] = meta
    return df


def get_frame_meta(df: pd.DataFrame) -> dict:
    return dict(df.attrs.get("cache_meta", {})) if hasattr(df, "attrs") else {}


def _make_meta(
    symbol: str,
    timeframe: str,
    source: str,
    fetched_at: datetime,
    df: pd.DataFrame,
    live_fetch: bool,
    fallback_used: bool = False,
    reason: str = "",
) -> dict:
    last_bar_time = None
    if not df.empty:
        last_bar = pd.Timestamp(df["date"].iloc[-1]).to_pydatetime()
        last_bar_time = last_bar.replace(tzinfo=UTC)
    current_et = market_hours.now_eastern()
    age_minutes = None
    if last_bar_time is not None:
        age_minutes = market_hours.minutes_since(last_bar_time.astimezone(EASTERN), current_et)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "source": source,
        "live_fetch": live_fetch,
        "fallback_used": fallback_used,
        "reason": reason,
        "fetched_at": _serialize_timestamp(fetched_at),
        "last_bar_time": _serialize_timestamp(last_bar_time),
        "session": market_hours.classify_market_session(current_et),
        "freshness_age_minutes": round(age_minutes, 1) if age_minutes is not None else None,
        "freshness_label": market_hours.intraday_freshness_label(age_minutes, market_hours.classify_market_session(current_et))
        if timeframe.startswith("intra")
        else "fresh",
        "bars": int(len(df)),
    }


def _clone_cached(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    return _attach_meta(df.copy(), dict(meta))


def _get_memory_cache(symbol: str, timeframe: str) -> Optional[Tuple[pd.DataFrame, dict]]:
    cached = _MEMORY_CACHE.get((symbol, timeframe))
    if not cached:
        return None
    df, meta = cached
    return df.copy(), dict(meta)


def _set_memory_cache(symbol: str, timeframe: str, df: pd.DataFrame, meta: dict) -> None:
    _MEMORY_CACHE[(symbol, timeframe)] = (df.copy(), dict(meta))


def _live_fetch_allowed(force: bool = False) -> bool:
    if force:
        return True
    if _LIVE_FETCH_DISABLED_UNTIL is None:
        return True
    return datetime.now(tz=UTC) >= _LIVE_FETCH_DISABLED_UNTIL


def _record_fetch_result(success: bool) -> None:
    global _CONSECUTIVE_FETCH_FAILURES, _LIVE_FETCH_DISABLED_UNTIL
    if success:
        _CONSECUTIVE_FETCH_FAILURES = 0
        _LIVE_FETCH_DISABLED_UNTIL = None
        return
    _CONSECUTIVE_FETCH_FAILURES += 1
    threshold = getattr(cfg, "YFINANCE_FAILURE_THRESHOLD", 6)
    if _CONSECUTIVE_FETCH_FAILURES >= threshold:
        cooldown = getattr(cfg, "YFINANCE_FAILURE_COOLDOWN_MINUTES", 10)
        _LIVE_FETCH_DISABLED_UNTIL = datetime.now(tz=UTC) + timedelta(minutes=cooldown)
        log_event(
            LOGGER,
            30,
            "provider_cooldown_started",
            failures=_CONSECUTIVE_FETCH_FAILURES,
            disabled_until=_LIVE_FETCH_DISABLED_UNTIL.isoformat(),
        )


def _circuit_reason() -> str:
    if _LIVE_FETCH_DISABLED_UNTIL is None:
        return ""
    return f"live_fetch_temporarily_disabled_until:{_LIVE_FETCH_DISABLED_UNTIL.isoformat()}"


def _download_history(symbol: str, **kwargs) -> pd.DataFrame:
    raw = yf.download(
        _provider_symbol(symbol),
        auto_adjust=True,
        progress=False,
        threads=False,
        timeout=getattr(cfg, "YFINANCE_TIMEOUT_SECONDS", 4),
        **kwargs,
    )
    return _normalize_df(raw)


def _fetch_daily(symbol: str) -> pd.DataFrame:
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=cfg.DAILY_LOOKBACK_DAYS)
    return _download_history(symbol, start=start.isoformat(), end=end.isoformat(), interval="1d")


def _fetch_daily_range(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    return _download_history(symbol, start=start_date, end=end_date, interval="1d")


def _fetch_intraday(symbol: str) -> pd.DataFrame:
    return _download_history(symbol, period=f"{cfg.INTRADAY_LOOKBACK_DAYS}d", interval="5m")


def _unavailable_meta(symbol: str, timeframe: str, reason: str, freshness_label:Optional[str] = None) -> dict:
    meta = {
        "symbol": symbol,
        "timeframe": timeframe,
        "source": "unavailable",
        "fallback_used": False,
        "reason": reason,
    }
    if timeframe.startswith("intra"):
        meta["freshness_label"] = freshness_label or "missing"
    return meta


def _load_from_cache_or_unavailable(symbol: str, timeframe: str, cached_df: pd.DataFrame, cached_meta: dict, reason: str) -> pd.DataFrame:
    if not cached_df.empty:
        meta = dict(cached_meta)
        if timeframe.startswith("intra"):
            last_bar_time = _parse_timestamp(meta.get("last_bar_time"))
            age_minutes = market_hours.minutes_since(last_bar_time.astimezone(EASTERN), market_hours.now_eastern()) if last_bar_time else None
            meta["freshness_age_minutes"] = round(age_minutes, 1) if age_minutes is not None else None
            meta["freshness_label"] = market_hours.intraday_freshness_label(age_minutes)
        meta.update({"source": "cache_fallback", "fallback_used": True, "reason": reason})
        _set_memory_cache(symbol, timeframe, cached_df, meta)
        log_event(LOGGER, 30, "market_data_cache_fallback", symbol=symbol, timeframe=timeframe, reason=reason)
        return _clone_cached(cached_df, meta)
    empty = _empty_market_frame()
    meta = _unavailable_meta(symbol, timeframe, reason)
    _set_memory_cache(symbol, timeframe, empty, meta)
    log_event(LOGGER, 30, "market_data_unavailable", symbol=symbol, timeframe=timeframe, reason=reason)
    return _clone_cached(empty, meta)


def load_daily(symbol: str, force: bool = False) -> pd.DataFrame:
    memory = _get_memory_cache(symbol, "daily")
    if memory and not force:
        df, meta = memory
        return _clone_cached(df, meta)
    cached_df, cached_meta = _read_cache(symbol, "daily")
    fetched_at = _parse_timestamp(cached_meta.get("fetched_at"))
    if not force and not cached_df.empty and fetched_at is not None:
        age_hours = (datetime.now(tz=UTC) - fetched_at.astimezone(UTC)).total_seconds() / 3600.0
        if age_hours <= cfg.DAILY_CACHE_MAX_AGE_HOURS:
            _set_memory_cache(symbol, "daily", cached_df, cached_meta)
            return _clone_cached(cached_df, cached_meta)

    if not _live_fetch_allowed(force=force):
        return _load_from_cache_or_unavailable(symbol, "daily", cached_df, cached_meta, _circuit_reason())

    try:
        df = _fetch_daily(symbol)
        if df.empty:
            raise ValueError("empty daily frame")
        meta = _make_meta(symbol, "daily", "yfinance", datetime.now(tz=UTC), df, live_fetch=True)
        _write_cache(symbol, "daily", df, meta)
        _record_fetch_result(success=True)
        _set_memory_cache(symbol, "daily", df, meta)
        return _clone_cached(df, meta)
    except Exception as exc:
        _record_fetch_result(success=False)
        return _load_from_cache_or_unavailable(symbol, "daily", cached_df, cached_meta, f"daily_fetch_failed:{type(exc).__name__}")


def load_intraday(symbol: str, force: bool = False) -> pd.DataFrame:
    memory = _get_memory_cache(symbol, "intra5m")
    if memory and not force:
        df, meta = memory
        return _clone_cached(df, meta)
    cached_df, cached_meta = _read_cache(symbol, "intra5m")
    fetched_at = _parse_timestamp(cached_meta.get("fetched_at"))
    if not force and not cached_df.empty and fetched_at is not None:
        if not market_hours.should_refresh_intraday(fetched_at, force=False):
            refreshed = dict(cached_meta)
            last_bar_time = _parse_timestamp(refreshed.get("last_bar_time"))
            age_minutes = market_hours.minutes_since(last_bar_time.astimezone(EASTERN), market_hours.now_eastern()) if last_bar_time else None
            refreshed["freshness_age_minutes"] = round(age_minutes, 1) if age_minutes is not None else None
            refreshed["freshness_label"] = market_hours.intraday_freshness_label(age_minutes)
            _set_memory_cache(symbol, "intra5m", cached_df, refreshed)
            return _clone_cached(cached_df, refreshed)

    if not _live_fetch_allowed(force=force):
        return _load_from_cache_or_unavailable(symbol, "intra5m", cached_df, cached_meta, _circuit_reason())

    try:
        df = _fetch_intraday(symbol)
        if df.empty:
            raise ValueError("empty intraday frame")
        meta = _make_meta(symbol, "intra5m", "yfinance", datetime.now(tz=UTC), df, live_fetch=True)
        _write_cache(symbol, "intra5m", df, meta)
        _record_fetch_result(success=True)
        _set_memory_cache(symbol, "intra5m", df, meta)
        return _clone_cached(df, meta)
    except Exception as exc:
        _record_fetch_result(success=False)
        return _load_from_cache_or_unavailable(symbol, "intra5m", cached_df, cached_meta, f"intraday_fetch_failed:{type(exc).__name__}")


def load_intraday_cache_only(symbol: str) -> pd.DataFrame:
    memory = _get_memory_cache(symbol, "intra5m")
    if memory:
        df, meta = memory
        return _clone_cached(df, meta)
    cached_df, cached_meta = _read_cache(symbol, "intra5m")
    if not cached_df.empty:
        _set_memory_cache(symbol, "intra5m", cached_df, cached_meta)
        return _clone_cached(cached_df, cached_meta)
    empty = _empty_market_frame()
    meta = _unavailable_meta(symbol, "intra5m", "backtest_intraday_cache_missing", freshness_label="missing")
    _set_memory_cache(symbol, "intra5m", empty, meta)
    return _clone_cached(empty, meta)


def build_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    weekly = (
        daily_df.set_index("date")
        .resample("W-FRI")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
        .reset_index()
    )
    return weekly


def load_all(symbol: str, force: bool = False) -> dict:
    daily = load_daily(symbol, force=force)
    intraday = load_intraday(symbol, force=force)
    weekly = build_weekly(daily)
    weekly.attrs["cache_meta"] = get_frame_meta(daily)
    return {
        "daily": daily,
        "weekly": weekly,
        "intraday": intraday,
        "meta": {
            "daily": get_frame_meta(daily),
            "weekly": get_frame_meta(weekly),
            "intraday": get_frame_meta(intraday),
            "market_hours": market_hours.market_context(),
        },
    }


def load_backtest_daily(symbol: str, start_date: str, end_date: str, force: bool = False) -> pd.DataFrame:
    timeframe = "daily_backtest"
    request_start = (pd.Timestamp(start_date) - pd.Timedelta(days=cfg.BACKTEST_HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    request_end = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    imported_df, imported_meta = _read_import_history(symbol)
    cached_df, cached_meta = _read_cache(symbol, timeframe)
    regular_cached_df, regular_cached_meta = _read_cache(symbol, "daily")
    if not imported_df.empty:
        merged = _merge_history_frames(imported_df, regular_cached_df)
        merged_start = pd.Timestamp(merged["date"].min()).strftime("%Y-%m-%d")
        merged_end = pd.Timestamp(merged["date"].max()).strftime("%Y-%m-%d")
        imported_result = dict(imported_meta)
        imported_result.update({
            "timeframe": timeframe,
            "source": "local_import_plus_cache" if not regular_cached_df.empty else "local_import",
            "fallback_used": False,
            "request_start": request_start,
            "request_end": request_end,
            "coverage_start": merged_start,
            "coverage_end": merged_end,
        })
        if merged_start <= request_start and merged_end >= request_end:
            return _clone_cached(merged, imported_result)
    cached_start = str(cached_meta.get("request_start") or "")
    cached_end = str(cached_meta.get("request_end") or "")
    if (
        not force
        and not cached_df.empty
        and cached_start
        and cached_end
        and cached_start <= request_start
        and cached_end >= request_end
    ):
        return _clone_cached(cached_df, cached_meta)

    try:
        df = _fetch_daily_range(symbol, request_start, request_end)
        if df.empty:
            raise ValueError("empty backtest daily frame")
        if not imported_df.empty:
            df = _merge_history_frames(imported_df, df)
        meta = _make_meta(symbol, timeframe, "yfinance", datetime.now(tz=UTC), df, live_fetch=True)
        meta["request_start"] = request_start
        meta["request_end"] = request_end
        if not imported_df.empty:
            meta["source"] = "local_import_plus_yfinance"
            meta["import_path"] = imported_meta.get("import_path")
        _write_cache(symbol, timeframe, df, meta)
        return _clone_cached(df, meta)
    except Exception as exc:
        if not imported_df.empty:
            fallback = dict(imported_meta)
            fallback.update({
                "timeframe": timeframe,
                "source": "local_import_plus_cache" if not regular_cached_df.empty else "local_import",
                "fallback_used": bool(not regular_cached_df.empty),
                "reason": f"backtest_daily_fetch_failed:{type(exc).__name__}",
                "request_start": request_start,
                "request_end": request_end,
            })
            merged = _merge_history_frames(imported_df, regular_cached_df)
            if not merged.empty:
                log_event(LOGGER, 30, "market_data_cache_fallback", symbol=symbol, timeframe=timeframe, reason=fallback["reason"])
                return _clone_cached(merged, fallback)
        if not regular_cached_df.empty:
            fallback = dict(regular_cached_meta)
            fallback.update({
                "source": "cache_fallback",
                "fallback_used": True,
                "reason": f"backtest_daily_fetch_failed:{type(exc).__name__}",
                "request_start": request_start,
                "request_end": request_end,
                "timeframe": timeframe,
            })
            log_event(LOGGER, 30, "market_data_cache_fallback", symbol=symbol, timeframe=timeframe, reason=fallback["reason"])
            return _clone_cached(regular_cached_df, fallback)
        if not cached_df.empty:
            fallback = dict(cached_meta)
            fallback.update({
                "source": "cache_fallback",
                "fallback_used": True,
                "reason": f"backtest_daily_fetch_failed:{type(exc).__name__}",
                "request_start": cached_start or request_start,
                "request_end": cached_end or request_end,
            })
            log_event(LOGGER, 30, "market_data_cache_fallback", symbol=symbol, timeframe=timeframe, reason=fallback["reason"])
            return _clone_cached(cached_df, fallback)
        empty = _empty_market_frame()
        meta = _unavailable_meta(symbol, timeframe, f"backtest_daily_fetch_failed:{type(exc).__name__}")
        meta["request_start"] = request_start
        meta["request_end"] = request_end
        log_event(LOGGER, 30, "market_data_unavailable", symbol=symbol, timeframe=timeframe, reason=meta["reason"])
        return _clone_cached(empty, meta)


def load_backtest_bundle(symbol: str, start_date: str, end_date: str, force: bool = False) -> dict:
    daily = load_backtest_daily(symbol, start_date, end_date, force=force)
    intraday = load_intraday_cache_only(symbol)
    weekly = build_weekly(daily)
    weekly.attrs["cache_meta"] = get_frame_meta(daily)
    return {
        "daily": daily,
        "weekly": weekly,
        "intraday": intraday,
        "meta": {
            "daily": get_frame_meta(daily),
            "weekly": get_frame_meta(weekly),
            "intraday": get_frame_meta(intraday),
            "market_hours": market_hours.market_context(),
        },
    }


def backfill_daily_imports(
    symbols: list[str],
    start_date: str,
    end_date: str,
    *,
    force: bool = False,
) -> dict:
    results = {
        "start_date": start_date,
        "end_date": end_date,
        "downloaded": [],
        "cache_seeded": [],
        "failed": [],
    }
    for raw_symbol in symbols:
        symbol = raw_symbol.upper()
        imported_df, _ = _read_import_history(symbol)
        if not imported_df.empty and not force:
            imported_start = pd.Timestamp(imported_df["date"].min()).strftime("%Y-%m-%d")
            imported_end = pd.Timestamp(imported_df["date"].max()).strftime("%Y-%m-%d")
            if imported_start <= start_date and imported_end >= end_date:
                results["downloaded"].append({
                    "symbol": symbol,
                    "path": str(cfg.DAILY_IMPORTS_DIR / f"{_safe_symbol_name(symbol)}.csv"),
                    "bars": int(len(imported_df)),
                    "source": "existing_import",
                    "coverage_start": imported_start,
                    "coverage_end": imported_end,
                })
                continue
        try:
            df = _fetch_daily_range(symbol, start_date, (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))
            if df.empty:
                raise ValueError("empty import frame")
            if not imported_df.empty:
                df = _merge_history_frames(imported_df, df)
            path = _write_import_history(symbol, df)
            results["downloaded"].append({
                "symbol": symbol,
                "path": str(path),
                "bars": int(len(df)),
                "source": "yfinance",
                "coverage_start": pd.Timestamp(df["date"].min()).strftime("%Y-%m-%d"),
                "coverage_end": pd.Timestamp(df["date"].max()).strftime("%Y-%m-%d"),
            })
        except Exception as exc:
            cached_df, _ = _read_cache(symbol, "daily")
            if not cached_df.empty:
                seeded = _merge_history_frames(imported_df, cached_df)
                path = _write_import_history(symbol, seeded)
                results["cache_seeded"].append({
                    "symbol": symbol,
                    "path": str(path),
                    "bars": int(len(seeded)),
                    "reason": type(exc).__name__,
                    "coverage_start": pd.Timestamp(seeded["date"].min()).strftime("%Y-%m-%d"),
                    "coverage_end": pd.Timestamp(seeded["date"].max()).strftime("%Y-%m-%d"),
                })
                continue
            results["failed"].append({
                "symbol": symbol,
                "reason": type(exc).__name__,
            })
    return results


def load_vix(force: bool = False) -> pd.DataFrame:
    return load_daily(cfg.VIX_SYMBOL, force=force)


def _latest_cached_df(symbol: str, timeframe: str, max_age_days: int) -> pd.DataFrame:
    pattern = f"{symbol}_{timeframe}*.csv"
    for path in sorted(cfg.CACHE_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
        if (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).days > max_age_days:
            continue
        try:
            return pd.read_csv(path, parse_dates=["date"])
        except Exception:
            continue
    return pd.DataFrame()


def fetch_earnings_date(symbol: str, force: bool = False) -> Optional[str]:
    if not getattr(cfg, "AUTO_FETCH_EARNINGS", True):
        return None
    manual = cfg.EARNINGS_CALENDAR.get(symbol.upper())
    if manual:
        return manual

    cache_path = cfg.CACHE_DIR / f"{symbol}_earnings.json"
    if cache_path.exists() and not force:
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            fetched_at = _parse_timestamp(payload.get("fetched_at"))
            if fetched_at and (datetime.now(tz=UTC) - fetched_at.astimezone(UTC)).total_seconds() <= 12 * 3600:
                return payload.get("earnings_date")
        except Exception:
            pass

    try:
        ticker = yf.Ticker(_provider_symbol(symbol))
        calendar = ticker.calendar
        earnings_dates = None
        if hasattr(calendar, "get"):
            earnings_dates = calendar.get("Earnings Date")
        elif hasattr(calendar, "columns") and "Earnings Date" in calendar.columns:
            earnings_dates = calendar["Earnings Date"].dropna().tolist()
        if earnings_dates:
            normalized = [pd.Timestamp(value) for value in earnings_dates if pd.Timestamp(value) >= pd.Timestamp.now().normalize()]
            if normalized:
                earnings_date = min(normalized).strftime("%Y-%m-%d")
                cache_path.write_text(json.dumps({
                    "earnings_date": earnings_date,
                    "fetched_at": _serialize_timestamp(datetime.now(tz=UTC)),
                }), encoding="utf-8")
                return earnings_date
    except Exception:
        pass

    cache_path.write_text(json.dumps({
        "earnings_date": None,
        "fetched_at": _serialize_timestamp(datetime.now(tz=UTC)),
    }), encoding="utf-8")
    return None


def load_macro_signals(force: bool = False) -> dict:
    fetched = {}
    for key, ticker in cfg.MACRO_SIGNAL_TICKERS.items():
        try:
            df = load_daily(ticker, force=force)
            if not df.empty:
                fetched[key] = df
        except Exception:
            continue

    result: dict = {}
    if "vix" in fetched and "vix3m" in fetched:
        vix_close = float(fetched["vix"]["close"].iloc[-1])
        vix3m_close = float(fetched["vix3m"]["close"].iloc[-1])
        if vix_close > 0:
            ratio = round(vix3m_close / vix_close, 4)
            result["vix_level"] = round(vix_close, 2)
            result["vix3m_level"] = round(vix3m_close, 2)
            result["vix_term_structure"] = ratio
            result["term_structure_contango"] = ratio >= 1.0
            result["term_structure_stress"] = ratio < 0.95

    if "hyg" in fetched and "lqd" in fetched and len(fetched["hyg"]) >= 21 and len(fetched["lqd"]) >= 21:
        hyg_ret = float(fetched["hyg"]["close"].iloc[-1] / fetched["hyg"]["close"].iloc[-21] - 1) * 100
        lqd_ret = float(fetched["lqd"]["close"].iloc[-1] / fetched["lqd"]["close"].iloc[-21] - 1) * 100
        spread = round(hyg_ret - lqd_ret, 3)
        result["hyg_lqd_spread_20d"] = spread
        result["credit_signal"] = "stressed" if spread < -2.0 else "widening" if spread < -0.5 else "normal"

    if "tnx" in fetched and "irx" in fetched:
        spread = round(float(fetched["tnx"]["close"].iloc[-1]) - float(fetched["irx"]["close"].iloc[-1]), 3)
        result["yield_curve_spread"] = spread
        result["curve_inverted"] = spread < 0

    if "skew" in fetched:
        skew_val = float(fetched["skew"]["close"].iloc[-1])
        result["skew_level"] = round(skew_val, 1)
        result["skew_elevated"] = skew_val > 140

    return result


def clean_old_cache(days_old:Optional[int] = None) -> None:
    days_old = days_old or cfg.CACHE_RETENTION_DAYS
    cutoff = datetime.now() - timedelta(days=days_old)
    removed = 0
    for file_path in cfg.CACHE_DIR.glob("*"):
        try:
            if datetime.fromtimestamp(file_path.stat().st_mtime) < cutoff:
                file_path.unlink()
                removed += 1
        except Exception:
            continue
    if removed:
        print(f"  Cleaned {removed} stale cache files")
