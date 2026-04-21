"""
US equity market-hours helpers used for intraday freshness decisions.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from . import config as cfg


EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def now_eastern() -> datetime:
    return datetime.now(tz=EASTERN)


def _session_bounds(current: datetime) -> tuple[datetime, datetime, datetime, datetime]:
    market_open = current.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = current.replace(hour=16, minute=0, second=0, microsecond=0)
    premarket_open = current.replace(hour=4, minute=0, second=0, microsecond=0)
    postmarket_close = current.replace(hour=20, minute=0, second=0, microsecond=0)
    return premarket_open, market_open, market_close, postmarket_close


def classify_market_session(current: datetime | None = None) -> str:
    current = current.astimezone(EASTERN) if current else now_eastern()
    if current.weekday() >= 5:
        return "closed"

    premarket_open, market_open, market_close, postmarket_close = _session_bounds(current)
    if premarket_open <= current < market_open:
        return "premarket"
    if market_open <= current <= market_close:
        return "regular"
    if market_close < current <= postmarket_close:
        return "postmarket"
    return "closed"


def intraday_max_age_minutes(session: str | None = None) -> int:
    session = session or classify_market_session()
    return {
        "premarket": cfg.PREMARKET_INTRADAY_MAX_AGE_MINUTES,
        "regular": cfg.REGULAR_SESSION_INTRADAY_MAX_AGE_MINUTES,
        "postmarket": cfg.POSTMARKET_INTRADAY_MAX_AGE_MINUTES,
        "closed": cfg.OFFSESSION_INTRADAY_MAX_AGE_MINUTES,
    }.get(session, cfg.OFFSESSION_INTRADAY_MAX_AGE_MINUTES)


def intraday_freshness_label(age_minutes: float | None, session: str | None = None) -> str:
    if age_minutes is None:
        return "missing"
    session = session or classify_market_session()
    max_age = intraday_max_age_minutes(session)
    mild = max_age * cfg.INTRADAY_MILD_STALE_MULTIPLIER
    hard = max_age * cfg.INTRADAY_HARD_STALE_MULTIPLIER
    if age_minutes <= max_age:
        return "fresh"
    if age_minutes <= mild:
        return "mildly_stale"
    if age_minutes <= hard:
        return "stale"
    return "very_stale"


def minutes_since(ts: datetime | None, current: datetime | None = None) -> float | None:
    if ts is None:
        return None
    current = current.astimezone(ts.tzinfo) if current and ts.tzinfo else (current or datetime.now(tz=ts.tzinfo))
    return max(0.0, (current - ts).total_seconds() / 60.0)


def should_refresh_intraday(fetched_at: datetime | None, force: bool = False, current: datetime | None = None) -> bool:
    if force or fetched_at is None:
        return True
    current = current.astimezone(EASTERN) if current else now_eastern()
    age_minutes = minutes_since(fetched_at.astimezone(EASTERN), current)
    label = intraday_freshness_label(age_minutes, classify_market_session(current))
    return label not in {"fresh"}


def market_context(current: datetime | None = None) -> dict:
    current = current.astimezone(EASTERN) if current else now_eastern()
    _, market_open, market_close, _ = _session_bounds(current)
    return {
        "as_of": current.isoformat(),
        "session": classify_market_session(current),
        "market_open": market_open.isoformat(),
        "market_close": market_close.isoformat(),
        "intraday_max_age_minutes": intraday_max_age_minutes(classify_market_session(current)),
    }
