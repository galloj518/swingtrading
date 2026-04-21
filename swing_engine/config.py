"""
Central configuration for Swing Engine.

The repo remains deterministic by default. Narrative generation is optional and
never part of the frequent scan path unless explicitly requested.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path


# =============================================================================
# DIRECTORIES
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
REPORTS_DIR = BASE_DIR / "reports"
TEMPLATES_DIR = BASE_DIR / "templates"
DB_PATH = DATA_DIR / "swing_engine.sqlite3"
DASHBOARD_OUTPUT_PATH = BASE_DIR / "dashboard.html"
RUN_HEALTH_OUTPUT_DIR = REPORTS_DIR
OFFLINE_SMOKE_OUTPUT_DIR = REPORTS_DIR
CHARTS_OUTPUT_DIR = REPORTS_DIR / "charts"

for _path in (DATA_DIR, CACHE_DIR, REPORTS_DIR, TEMPLATES_DIR, CHARTS_OUTPUT_DIR):
    _path.mkdir(parents=True, exist_ok=True)


# =============================================================================
# ACCOUNT / RISK
# =============================================================================
ACCOUNT_SIZE = 100_000
MAX_RISK_PCT = 1.0
MAX_GROUP_RISK_PCT = 2.0
DEFAULT_ATR_STOP_MULT = 1.5
MAX_DAILY_VOLUME_PARTICIPATION_PCT = 0.01


# =============================================================================
# SYMBOLS
# =============================================================================
BENCHMARKS = ["SPY", "QQQ", "SOXX", "DIA"]
VIX_SYMBOL = "^VIX"
BENCHMARK_SET = frozenset({"SPY", "QQQ", "SOXX", "DIA", "VIX", "^VIX", "IWM"})

WATCHLIST = [
    "AAOI", "ALAB", "AVGO", "ABBV", "BE", "BW", "CIEN", "CLS", "COHR", "CRDO",
    "DOCN", "EOSE", "ETN", "EYPT", "EZPW", "GEV", "GNRC", "HROW", "KOD", "LITE",
    "NVDA", "NVT", "OCUL", "PWR", "REGN", "VRT", "WMB",
    "AAPL", "ABT", "ACN", "ADP", "AMZN", "APD", "AXP", "BDX", "BLK", "BP",
    "BRK.B", "CBZ", "CMCSA", "CNXC", "CTSH", "EME", "ERIE", "EXAS", "FDS", "GOOG",
    "GOOGL", "HD", "IBM", "IBP", "ICLR", "IFNNY", "IQV", "IVZ", "JNJ", "KR",
    "KVUE", "LHX", "LZB", "MDT", "MLM", "MMM", "MSFT", "MU", "MUA", "NDAQ",
    "NFLX", "ORLY", "PEP", "PHR", "PKG", "PLD", "PNC", "PPG", "PRGS", "PYPL",
    "ROL", "SE", "SGI", "SMA", "T", "TSCO", "TROW", "TTD", "TXN", "VIAV",
    "VICI", "VRSN", "WAT", "WDAY", "WFC", "XNGSY", "ZTS",
]

STRUCTURAL_LEADER_COUNT = 12
BREAKOUT_WATCH_COUNT = 18
TRIGGERED_NOW_COUNT = 8
TOP_NARRATIVE_COUNT = 8
TOP_CHART_COUNT = 12
TOP_EXECUTION_INTRADAY_COUNT = 6


# =============================================================================
# CORRELATION GROUPS
# =============================================================================
CORRELATION_GROUPS = {
    "mega_tech": ["AAPL", "AMZN", "GOOG", "GOOGL", "MSFT", "NFLX"],
    "semis": ["NVDA", "AVGO", "MU", "TXN", "COHR", "LITE", "CLS", "CRDO", "SOXX", "SOXL"],
    "network_ai": ["AAOI", "ALAB", "CIEN", "DOCN", "VRT", "VIAV"],
    "software": ["ACN", "ADP", "CTSH", "PRGS", "SE", "WDAY", "TTD", "VRSN", "PHR"],
    "healthcare": ["ABBV", "ABT", "BDX", "EYPT", "EXAS", "HROW", "ICLR", "IQV", "JNJ", "KVUE", "MDT", "OCUL", "REGN", "WAT", "ZTS"],
    "power_infra": ["BE", "ETN", "GEV", "GNRC", "NVT", "PWR", "WMB"],
    "industrial": ["BW", "EME", "HD", "IBP", "MLM", "MMM", "ORLY", "PKG", "PPG", "ROL", "TSCO"],
    "financials": ["AXP", "BLK", "BRK.B", "ERIE", "EZPW", "FDS", "IVZ", "NDAQ", "PNC", "TROW", "WFC"],
    "defensive_value": ["APD", "BP", "CMCSA", "KR", "MUA", "PEP", "PLD", "T", "VICI", "XNGSY"],
    "special_situations": ["CBZ", "CNXC", "IFNNY", "KOD", "LHX", "LZB", "PYPL", "SGI", "SMA"],
    "broad_market": ["SPY", "QQQ", "DIA", "IWM"],
}

DOWNLOAD_SYMBOL_OVERRIDES = {"BRK.B": "BRK-B"}


# =============================================================================
# INDICATOR PARAMETERS
# =============================================================================
DAILY_SMA_PERIODS = [5, 10, 20, 50, 150, 200]
WEEKLY_SMA_PERIODS = [5, 10, 20, 30]
INTRA_SMA_PERIODS = [10, 20, 50]
ATR_PERIOD = 14
RVOL_PERIOD = 20
AVG_DOLLAR_VOL_PERIOD = 20

MIN_AVG_DAILY_VOLUME = 750_000
MIN_AVG_DOLLAR_VOLUME = 25_000_000
PREFERRED_AVG_DOLLAR_VOLUME = 100_000_000

DAILY_LOOKBACK_DAYS = 400
INTRADAY_LOOKBACK_DAYS = 5


# =============================================================================
# FRESHNESS / CACHE
# =============================================================================
DAILY_CACHE_MAX_AGE_HOURS = 18
PREMARKET_INTRADAY_MAX_AGE_MINUTES = 75
REGULAR_SESSION_INTRADAY_MAX_AGE_MINUTES = 12
POSTMARKET_INTRADAY_MAX_AGE_MINUTES = 20
OFFSESSION_INTRADAY_MAX_AGE_MINUTES = 240
INTRADAY_MILD_STALE_MULTIPLIER = 2.0
INTRADAY_HARD_STALE_MULTIPLIER = 4.0
INTRADAY_TRIGGER_MIN_BARS = 24
CACHE_RETENTION_DAYS = 6
YFINANCE_TIMEOUT_SECONDS = 4
YFINANCE_FAILURE_THRESHOLD = 6
YFINANCE_FAILURE_COOLDOWN_MINUTES = 10
RUN_DEGRADED_UNAVAILABLE_RATIO = 0.15
RUN_FAILED_UNAVAILABLE_RATIO = 0.5
RUN_DEGRADED_PACKET_FAILURE_COUNT = 1
RUN_DEGRADED_TRIGGER_RATIO = 0.25


# =============================================================================
# BREAKOUT / TRIGGER MODEL
# =============================================================================
SETUP_FAMILY_TOGGLES = {
    "near_high_breakout": True,
    "volatility_contraction": True,
    "flat_base": True,
    "shelf_breakout": True,
    "flag_pennant": True,
    "breakout_retest": True,
    "reclaim_and_go": True,
}

SETUP_STATES = (
    "FORMING",
    "POTENTIAL_BREAKOUT",
    "TRIGGER_WATCH",
    "ACTIONABLE_BREAKOUT",
    "ACTIONABLE_RETEST",
    "ACTIONABLE_RECLAIM",
    "EXTENDED",
    "FAILED",
    "BLOCKED",
    "DATA_UNAVAILABLE",
)

ACTIONABILITY_LABELS = (
    "BUY NOW",
    "BUY BREAKOUT",
    "BUY RETEST",
    "WATCH TRIGGER",
    "WATCH CONTINUATION",
    "WAIT PULLBACK",
    "WAIT FOR TIGHTENING",
    "BLOCK",
    "DATA UNAVAILABLE",
)

STRUCTURAL_MIN_SCORE = 55.0
BREAKOUT_WATCH_MIN_SCORE = 58.0
TRIGGER_WATCH_MIN_SCORE = 62.0
ACTIONABLE_TRIGGER_MIN_SCORE = 70.0
ACTIONABLE_TOTAL_MIN_SCORE = 68.0
PATTERN_MIN_CLARITY = 52.0
MAX_BREAKOUT_EXTENSION_ATR = 1.35
MAX_RETEST_EXTENSION_ATR = 0.9
PIVOT_PROXIMITY_PCT = 3.0
NEAR_HIGH_PCT_20D = 3.0
NEAR_HIGH_PCT_60D = 4.0
NEAR_HIGH_PCT_252D = 6.0
MAX_OVERHEAD_SUPPLY_PCT = 6.0


# =============================================================================
# NARRATIVE / RUN MODE
# =============================================================================
DEFAULT_RUN_MODE = "run"
ALLOW_NARRATIVES = True
NARRATIVES_REQUIRE_EXPLICIT_MODE = True
NARRATIVE_DEFAULT_INCLUDE = False
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
LOG_LEVEL = os.environ.get("SWING_ENGINE_LOG_LEVEL", "INFO").upper()


# =============================================================================
# ANCHORS
# =============================================================================
MACRO_ANCHORS = {
    "SPY": {
        "covid_low": "2020-03-23",
        "2022_bear_low": "2022-10-13",
        "2024_aug_low": "2024-08-05",
        "liberation_day": "2025-04-07",
        "ytd": f"{date.today().year}-01-02",
    },
    "QQQ": {
        "covid_low": "2020-03-23",
        "2022_bear_low": "2022-10-13",
        "liberation_day": "2025-04-07",
        "ytd": f"{date.today().year}-01-02",
    },
    "SOXX": {
        "recent_high": "2026-02-25",
        "recent_low": "2026-03-09",
        "covid_low": "2020-03-23",
        "liberation_day_low": "2025-04-07",
        "iran_war_low": "2026-03-10",
        "ytd": f"{date.today().year}-01-02",
    },
    "DIA": {
        "covid_low": "2020-03-23",
        "liberation_day": "2025-04-07",
        "ytd": f"{date.today().year}-01-02",
    },
}

DEFAULT_ANCHORS = {
    "ytd": f"{date.today().year}-01-02",
    "covid_low": "2020-03-23",
    "liberation_day": "2025-04-07",
    "iran_war_start": "2026-03-10",
}

COMPANY_ANCHORS = {
    "NVDA": {"earnings_gap_may25": "2025-05-29"},
}


# =============================================================================
# EVENTS / EARNINGS
# =============================================================================
MACRO_EVENTS = [
    ("2026-03-18", "FOMC Decision + Press Conf", "high"),
    ("2026-04-02", "Jobs Report", "medium"),
    ("2026-04-10", "CPI Release", "high"),
    ("2026-04-29", "FOMC Decision", "high"),
    ("2026-05-08", "Jobs Report", "medium"),
    ("2026-05-14", "CPI Release", "high"),
    ("2026-06-10", "FOMC Decision", "high"),
    ("2026-06-12", "CPI Release", "high"),
]

EARNINGS_CALENDAR = {}
AUTO_FETCH_EARNINGS = True


# =============================================================================
# BENCHMARK TACTICAL
# =============================================================================
SOXX_TACTICAL_ANCHORS = {
    "recent_high": "2026-02-25",
    "recent_low": "2026-03-09",
    "covid_low": "2020-03-23",
    "liberation_day_low": "2025-04-07",
    "iran_war_low": "2026-03-10",
    "ytd": f"{date.today().year}-01-02",
}

LEVERAGED_PAIRS = {
    "SPY": {"long": "SPXL", "short": "SPXS", "leverage": 3.0, "name": "S&P 500"},
    "QQQ": {"long": "TQQQ", "short": "SQQQ", "leverage": 3.0, "name": "Nasdaq 100"},
    "SOXX": {"long": "SOXL", "short": "SOXS", "leverage": 3.0, "name": "Semiconductors"},
}


# =============================================================================
# LEGACY GATES / BACKTESTING / CORRELATION
# =============================================================================
GATE_WEEKLY_REQUIRES = "constructive_weekly_structure"
GATE_DAILY_REQUIRES = "constructive_daily_backbone"
SCORE_CAP_WEEKLY_FAIL = 30
SCORE_CAP_DAILY_FAIL = 50
SIGNAL_EXPIRY_DAYS = 3

USE_DYNAMIC_CORRELATION = True
DYNAMIC_CORR_LOOKBACK = 60
DYNAMIC_CORR_THRESHOLD = 0.65

BACKTEST_START_DATE = "2023-01-01"
WALK_FORWARD_IN_SAMPLE_MONTHS = 12
WALK_FORWARD_OUT_OF_SAMPLE_MONTHS = 3
WALK_FORWARD_STEP_MONTHS = 3
CALIBRATION_MIN_SAMPLES_WEIGHT = 6


# =============================================================================
# MACRO SIGNAL TICKERS
# =============================================================================
MACRO_SIGNAL_TICKERS = {
    "vix": "^VIX",
    "vix3m": "^VIX3M",
    "hyg": "HYG",
    "lqd": "LQD",
    "irx": "^IRX",
    "tnx": "^TNX",
    "skew": "^SKEW",
}


# =============================================================================
# ALERTS / OPTIONAL ACCOUNT KEYS
# =============================================================================
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "")
ALERT_EMAIL_PASSWORD = os.environ.get("ALERT_EMAIL_PASSWORD", "")
ALERT_EMAIL_SMTP = os.environ.get("ALERT_EMAIL_SMTP", "smtp.gmail.com")
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")
ALERT_MIN_SCORE = 70
ALERT_ACTION_BIASES = ("buy", "lean_buy")

SCHWAB_APP_KEY = os.environ.get("SCHWAB_APP_KEY", "")
SCHWAB_APP_SECRET = os.environ.get("SCHWAB_APP_SECRET", "")
SCHWAB_REFRESH_TOKEN = os.environ.get("SCHWAB_REFRESH_TOKEN", "")


# =============================================================================
# EXECUTION COST MODEL
# =============================================================================
COST_MODEL = {
    "commission_per_share": 0.00,
    "commission_per_trade": 0.00,
    "slippage_bps_liquid": 3,
    "slippage_bps_semiliquid": 8,
    "slippage_bps_illiquid": 20,
    "spread_bps_liquid": 2,
    "spread_bps_semiliquid": 5,
    "spread_bps_illiquid": 15,
}


# =============================================================================
# EXIT POLICY
# =============================================================================
EXIT_POLICY = {
    "trailing_stop_atr_mult": 1.5,
    "partial_1_at_rr": 1.0,
    "partial_2_at_rr": 2.0,
    "trail_remaining_from_rr": 1.0,
    "max_hold_days": 15,
    "time_stop_below_entry_pct": -3.0,
    "time_stop_trigger_days": 5,
}
