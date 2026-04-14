"""
Central configuration for Swing Engine.
Edit this file to customize symbols, anchors, events, and parameters.
"""
import os
from pathlib import Path
from datetime import date

# =============================================================================
# DIRECTORIES
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = DATA_DIR / "swing_engine.sqlite3"

for _d in [DATA_DIR, CACHE_DIR, REPORTS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# =============================================================================
# ACCOUNT
# =============================================================================
ACCOUNT_SIZE = 100_000
MAX_RISK_PCT = 1.0          # % of account risked per trade
DEFAULT_ATR_STOP_MULT = 1.5
MAX_GROUP_RISK_PCT = 2.0    # max total risk in any correlation group

# =============================================================================
# SYMBOLS
# =============================================================================
BENCHMARKS = ["SPY", "QQQ", "SOXX", "DIA"]
VIX_SYMBOL = "^VIX"
BENCHMARK_SET = frozenset({"SPY", "QQQ", "SOXX", "DIA", "VIX", "^VIX", "IWM"})

WATCHLIST = [
    "NVDA", "AVGO", "TSM", "AAPL", "MSFT", "GOOGL", "META", "AMZN",
    "ANET", "ASML", "PLTR", "VRT", "CEG", "ETN", "PWR", "CRS",
]

# =============================================================================
# CORRELATION GROUPS (for position sizing)
# =============================================================================
CORRELATION_GROUPS = {
    "mega_tech":    ["AAPL", "MSFT", "GOOGL", "META", "AMZN"],
    "semis":        ["NVDA", "AVGO", "TSM", "ASML", "SOXX", "SOXL"],
    "ai_infra":     ["ANET", "VRT", "PLTR"],
    "power_infra":  ["CEG", "ETN", "PWR", "GEV"],
    "industrial":   ["CRS", "RTX", "AVAV"],
    "broad_market": ["SPY", "QQQ", "DIA", "IWM"],
}

# =============================================================================
# INDICATOR PARAMETERS
# =============================================================================
DAILY_SMA_PERIODS = [5, 10, 20, 50, 200]
WEEKLY_SMA_PERIODS = [5, 10, 20]
INTRA_SMA_PERIODS = [10, 20, 50]
ATR_PERIOD = 14

DAILY_LOOKBACK_DAYS = 400   # ~1.5 years for 200 SMA stability
INTRADAY_LOOKBACK_DAYS = 5

# =============================================================================
# MACRO ANCHORS (for anchored VWAP)
# =============================================================================
MACRO_ANCHORS = {
    "SPY": {
        "covid_low":      "2020-03-23",
        "2022_bear_low":  "2022-10-13",
        "2024_aug_low":   "2024-08-05",
        "liberation_day": "2025-04-07",
        "ytd":            f"{date.today().year}-01-02",
    },
    "QQQ": {
        "covid_low":      "2020-03-23",
        "2022_bear_low":  "2022-10-13",
        "liberation_day": "2025-04-07",
        "ytd":            f"{date.today().year}-01-02",
    },
    "SOXX": {
        "recent_high":         "2026-02-25",
        "recent_low":          "2026-03-09",
        "covid_low":           "2020-03-23",
        "liberation_day_low":  "2025-04-07",
        "iran_war_low":        "2026-03-10",
        "ytd":                 f"{date.today().year}-01-02",
    },
    "DIA": {
        "covid_low":      "2020-03-23",
        "liberation_day": "2025-04-07",
        "ytd":            f"{date.today().year}-01-02",
    },
}

DEFAULT_ANCHORS = {
    "ytd":                 f"{date.today().year}-01-02",
    "covid_low":           "2020-03-23",
    "liberation_day":      "2025-04-07",
    "iran_war_start":      "2026-03-10",
}

# =============================================================================
# COMPANY-SPECIFIC ANCHORS
# =============================================================================
COMPANY_ANCHORS = {
    "NVDA": {"earnings_gap_may25": "2025-05-29"},
    # Add per-symbol anchors as needed:
    # "AAPL": {"earnings_oct25": "2025-10-30"},
}

# =============================================================================
# MACRO EVENT CALENDAR (update Sunday evenings)
# =============================================================================
# (date_str, event_name, severity: "high" | "medium" | "low")
MACRO_EVENTS = [
    ("2026-03-18", "FOMC Decision + Press Conf", "high"),
    ("2026-04-02", "Jobs Report",                "medium"),
    ("2026-04-10", "CPI Release",                "high"),
    ("2026-04-29", "FOMC Decision",              "high"),
    ("2026-05-08", "Jobs Report",                "medium"),
    ("2026-05-14", "CPI Release",                "high"),
    ("2026-06-10", "FOMC Decision",              "high"),
    ("2026-06-12", "CPI Release",                "high"),
]

# Known upcoming earnings (non-benchmark symbols only)
EARNINGS_CALENDAR = {
    # "NVDA": "2026-05-28",
    # "AAPL": "2026-05-01",
}

# =============================================================================
# SOXX TACTICAL ANCHORS
# =============================================================================
SOXX_TACTICAL_ANCHORS = {
    "recent_high":         "2026-02-25",
    "recent_low":          "2026-03-09",
    "covid_low":           "2020-03-23",
    "liberation_day_low":  "2025-04-07",
    "iran_war_low":        "2026-03-10",
    "ytd":                 f"{date.today().year}-01-02",
}

# =============================================================================
# LEVERAGED BENCHMARK TACTICAL MAPPINGS
# Chart symbol -> (3x long vehicle, 3x short vehicle, leverage)
# =============================================================================
LEVERAGED_PAIRS = {
    "SPY":  {"long": "SPXL", "short": "SPXS", "leverage": 3.0, "name": "S&P 500"},
    "QQQ":  {"long": "TQQQ", "short": "SQQQ", "leverage": 3.0, "name": "Nasdaq 100"},
    "SOXX": {"long": "SOXL", "short": "SOXS", "leverage": 3.0, "name": "Semiconductors"},
}

# =============================================================================
# GATED SCORING PARAMETERS
# =============================================================================
# Gate thresholds — these are the values you calibrate over time
GATE_WEEKLY_REQUIRES = "close_above_sma_20"   # minimum weekly condition
GATE_DAILY_REQUIRES = "close_above_sma_50"    # minimum daily condition

# Score caps when gates fail
SCORE_CAP_WEEKLY_FAIL = 30
SCORE_CAP_DAILY_FAIL = 50

# Signal expiration
SIGNAL_EXPIRY_DAYS = 3

# =============================================================================
# SCHWAB (optional — for account data, not market data)
# =============================================================================
SCHWAB_APP_KEY = os.environ.get("SCHWAB_APP_KEY", "")
SCHWAB_APP_SECRET = os.environ.get("SCHWAB_APP_SECRET", "")
SCHWAB_REFRESH_TOKEN = os.environ.get("SCHWAB_REFRESH_TOKEN", "")
