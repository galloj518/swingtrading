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
    # User priority swing names
    "AAOI", "ALAB", "AVGO", "ABBV", "BE", "BW", "CIEN", "CLS", "COHR", "CRDO",
    "DOCN", "EOSE", "ETN", "EYPT", "EZPW", "GEV", "GNRC", "HROW", "KOD", "LITE",
    "NVDA", "NVT", "OCUL", "PWR", "REGN", "VRT", "WMB",
    # Broader scan universe
    "AAPL", "ABT", "ACN", "ADP", "AMZN", "APD", "AXP", "BDX", "BLK", "BP",
    "BRK.B", "CBZ", "CMCSA", "CNXC", "CTSH", "EME", "ERIE", "EXAS", "FDS", "GOOG",
    "GOOGL", "HD", "IBM", "IBP", "ICLR", "IFNNY", "IQV", "IVZ", "JNJ", "KR",
    "KVUE", "LHX", "LZB", "MDT", "MLM", "MMM", "MSFT", "MU", "MUA", "NDAQ",
    "NFLX", "ORLY", "PEP", "PHR", "PKG", "PLD", "PNC", "PPG", "PRGS", "PYPL",
    "ROL", "SE", "SGI", "SMA", "T", "TSCO", "TROW", "TTD", "TXN", "VIAV",
    "VICI", "VRSN", "WAT", "WDAY", "WFC", "XNGSY", "ZTS",
]

TOP_EXECUTION_COUNT = 5
TOP_NARRATIVE_COUNT = 7
TOP_CHART_COUNT = 15
TOP_EXECUTION_INTRADAY_COUNT = 5

# =============================================================================
# CORRELATION GROUPS (for position sizing)
# =============================================================================
CORRELATION_GROUPS = {
    "mega_tech":    ["AAPL", "AMZN", "GOOG", "GOOGL", "MSFT", "NFLX"],
    "semis":        ["NVDA", "AVGO", "MU", "TXN", "COHR", "LITE", "CLS", "CRDO", "SOXX", "SOXL"],
    "network_ai":   ["AAOI", "ALAB", "CIEN", "DOCN", "VRT", "VIAV"],
    "software":     ["ACN", "ADP", "CTSH", "PRGS", "SE", "WDAY", "TTD", "VRSN", "PHR"],
    "healthcare":   ["ABBV", "ABT", "BDX", "EYPT", "EXAS", "HROW", "ICLR", "IQV", "JNJ", "KVUE", "MDT", "OCUL", "REGN", "WAT", "ZTS"],
    "power_infra":  ["BE", "ETN", "GEV", "GNRC", "NVT", "PWR", "WMB"],
    "industrial":   ["BW", "EME", "HD", "IBP", "MLM", "MMM", "ORLY", "PKG", "PPG", "ROL", "TSCO"],
    "financials":   ["AXP", "BLK", "BRK.B", "ERIE", "EZPW", "FDS", "IVZ", "NDAQ", "PNC", "TROW", "WFC"],
    "defensive_value": ["APD", "BP", "CMCSA", "KR", "MUA", "PEP", "PLD", "T", "VICI", "XNGSY"],
    "special_situations": ["CBZ", "CNXC", "IFNNY", "KOD", "LHX", "LZB", "PYPL", "SGI", "SMA"],
    "broad_market": ["SPY", "QQQ", "DIA", "IWM"],
}

# Some symbols need provider-specific formatting for market data downloads.
DOWNLOAD_SYMBOL_OVERRIDES = {
    "BRK.B": "BRK-B",
}

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
MAX_DAILY_VOLUME_PARTICIPATION_PCT = 0.01

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

# =============================================================================
# EXECUTION COST MODEL
# =============================================================================
COST_MODEL = {
    "commission_per_share":   0.00,   # Schwab: zero stock commissions
    "commission_per_trade":   0.00,   # Default equity workflow: no per-trade commission
    "slippage_bps_liquid":    3,      # >$100M ADV names: ~3bps round-trip entry
    "slippage_bps_semiliquid": 8,     # $25M-$100M ADV names
    "slippage_bps_illiquid":  20,     # <$25M ADV (should be blocked anyway)
    "spread_bps_liquid":      2,      # Bid-ask half-spread for liquid names
    "spread_bps_semiliquid":  5,
    "spread_bps_illiquid":    15,
}

# =============================================================================
# ALERTS
# =============================================================================
ALERT_EMAIL_TO       = os.environ.get("ALERT_EMAIL_TO", "")
ALERT_EMAIL_FROM     = os.environ.get("ALERT_EMAIL_FROM", "")
ALERT_EMAIL_PASSWORD = os.environ.get("ALERT_EMAIL_PASSWORD", "")
ALERT_EMAIL_SMTP     = os.environ.get("ALERT_EMAIL_SMTP", "smtp.gmail.com")
ALERT_WEBHOOK_URL    = os.environ.get("ALERT_WEBHOOK_URL", "")  # Slack/Discord/Teams
ALERT_MIN_SCORE      = 70        # Only alert on setups scoring >= this
ALERT_ACTION_BIASES  = ("buy", "lean_buy")

# =============================================================================
# EARNINGS AUTO-FETCH
# =============================================================================
AUTO_FETCH_EARNINGS = True   # Set False to rely on manual EARNINGS_CALENDAR only

# =============================================================================
# MACRO SIGNAL TICKERS (for regime overlay)
# =============================================================================
MACRO_SIGNAL_TICKERS = {
    "vix":   "^VIX",
    "vix3m": "^VIX3M",
    "hyg":   "HYG",
    "lqd":   "LQD",
    "irx":   "^IRX",
    "tnx":   "^TNX",
    "skew":  "^SKEW",
}

# =============================================================================
# DYNAMIC CORRELATION
# =============================================================================
USE_DYNAMIC_CORRELATION   = True
DYNAMIC_CORR_LOOKBACK     = 60    # trading days
DYNAMIC_CORR_THRESHOLD    = 0.65  # correlation above which names share a risk bucket

# =============================================================================
# EXIT POLICY (matched to ThresholdRegistry — change there to override)
# =============================================================================
EXIT_POLICY = {
    "trailing_stop_atr_mult":     1.5,
    "partial_1_at_rr":            1.0,   # take 1/3 at 1R
    "partial_2_at_rr":            2.0,   # take 1/3 at 2R
    "trail_remaining_from_rr":    1.0,   # start trailing after 1R
    "max_hold_days":              15,
    "time_stop_below_entry_pct": -3.0,   # % loss threshold for time stop
    "time_stop_trigger_days":     5,
}

# =============================================================================
# BACKTESTING
# =============================================================================
BACKTEST_START_DATE                = "2023-01-01"
WALK_FORWARD_IN_SAMPLE_MONTHS      = 12
WALK_FORWARD_OUT_OF_SAMPLE_MONTHS  = 3
WALK_FORWARD_STEP_MONTHS           = 3

# =============================================================================
# CALIBRATION
# =============================================================================
CALIBRATION_MIN_SAMPLES_WEIGHT = 6   # Minimum signals before evidence gets weight
