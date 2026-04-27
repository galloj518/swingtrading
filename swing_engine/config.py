"""
Central configuration for Swing Engine.

The repo remains deterministic by default. Narrative generation is optional and
never part of the frequent scan path unless explicitly requested.
"""
from __future__ import annotations

import os
import copy
from datetime import date
from pathlib import Path


# =============================================================================
# DIRECTORIES
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
REPORTS_DIR = BASE_DIR / "reports"
RESEARCH_REPORTS_DIR = REPORTS_DIR / "research"
TEMPLATES_DIR = BASE_DIR / "templates"
DOCS_DIR = BASE_DIR / "docs"
DB_PATH = DATA_DIR / "swing_engine.sqlite3"
DASHBOARD_OUTPUT_PATH = DOCS_DIR / "dashboard.html"
DECISION_REPORT_OUTPUT_PATH = DOCS_DIR / "decision_report.txt"
RUN_HEALTH_OUTPUT_DIR = REPORTS_DIR
OFFLINE_SMOKE_OUTPUT_DIR = REPORTS_DIR
CHARTS_OUTPUT_DIR = REPORTS_DIR / "charts"

for _path in (DATA_DIR, CACHE_DIR, REPORTS_DIR, RESEARCH_REPORTS_DIR, TEMPLATES_DIR, CHARTS_OUTPUT_DIR, DOCS_DIR):
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
    "STALKING",
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
    "CONTINUATION ONLY",
    "RESEARCH ONLY",
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
    "russia_ukraine_war_start": "2022-02-24",
    "trump_inauguration_2025": "2025-01-20",
    "liberation_day": "2025-04-07",
    "iran_war_start": "2026-03-10",
}

GEOPOLITICAL_EVENT_ANCHORS = {
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
BACKTEST_END_DATE = date.today().isoformat()
BACKTEST_SYMBOLS = WATCHLIST[:8]
BACKTEST_CALIBRATION_ENABLED = True
BACKTEST_REPLAY_LOOKBACK_BARS = max(DAILY_SMA_PERIODS) + 20
WALK_FORWARD_IN_SAMPLE_MONTHS = 12
WALK_FORWARD_OUT_OF_SAMPLE_MONTHS = 3
WALK_FORWARD_STEP_MONTHS = 3
CALIBRATION_MIN_SAMPLES_WEIGHT = 6
OUTCOME_FORWARD_WINDOWS = (1, 3, 5, 10, 20)
OUTCOME_ANALYSIS_HORIZON_DAYS = 20
RESEARCH_MIN_GROUP_SIZE = CALIBRATION_MIN_SAMPLES_WEIGHT
RESEARCH_MIN_MODEL_ROWS = CALIBRATION_MIN_SAMPLES_WEIGHT * 4
RESEARCH_BOOTSTRAP_SYMBOLS = ("SMOKEA", "SMOKEB", "SMOKEC", "SMOKED", "SMOKEE", "SMOKEF")

# Calibration fallback thresholds preserved from the repo's pre-existing live
# scoring defaults. Provenance: legacy_preserved.
RR_MIN_ACTIONABLE = 1.15
RR_MIN_POTENTIAL = 1.35
OVERHEAD_MIN_SCORE = 55.0
ORDERLINESS_MIN_SCORE = 58.0
RS20_SUPPORTIVE_MIN = 0.0
RVOL_SUPPORTIVE_MIN = 1.0


# =============================================================================
# PRODUCTION PROMOTION RULES
# =============================================================================
# These gates are derived from the repo's own replay research artifacts dated
# 2026-04-22. They are intentionally narrow production-promotion gates rather
# than broad research-state filters.
#
# Provenance:
# - outcome_backed_band: grouped expectancy bins from research outputs dated
#   2026-04-22
# - legacy_preserved: existing repo constants intentionally retained where the
#   research output did not provide a stronger replacement
# - provisional_if_needed: only used where replay evidence is still sparse
PRODUCTION_PRIMARY_STATES = (
    "ACTIONABLE_BREAKOUT",
    "TRIGGER_WATCH",
    "ACTIONABLE_RECLAIM",
    "ACTIONABLE_RETEST",
)
RESEARCH_ONLY_STATES = ("STALKING",)
EXTENDED_SUBTYPES = ("EXTENDED_CONTINUATION", "EXTENDED_LATE")

# Outcome-backed primary-edge bands used in the live promotion path.
PRODUCTION_BAND_PROVENANCE = {
    "pivot_distance_pct": "outcome_backed_band",
    "pivot_position": "outcome_backed_band",
    "trigger_readiness_score": "outcome_backed_band",
    "breakout_readiness_score": "outcome_backed_band",
    "structural_score": "outcome_backed_band",
    "extension_atr": "outcome_backed_band",
    "overhead_supply_score": "outcome_backed_band",
    "rvol": "outcome_backed_band",
    "larger_ma_supportive": "outcome_backed_band",
    "tightening_to_short_ma": "outcome_backed_band",
    "short_ma_rising": "provisional_if_needed",
    "avwap_supportive": "outcome_backed_band",
    "avwap_resistance": "outcome_backed_band",
    "trigger_type": "outcome_backed_band",
    "extended_subtype": "provisional_if_needed",
}

# Outcome-backed band specs intentionally use quantile/distribution methodology
# instead of narrow literal bin edges. Runtime cut points are derived from the
# repo's replay outcomes and current packet distributions.
PRODUCTION_BAND_SPECS = {
    "pivot_distance_pct": {
        "mode": "target_abs",
        "target": 0.0,
        "favorable_quantile": 0.35,
        "acceptable_quantile": 0.7,
        "fallback_scale": PIVOT_PROXIMITY_PCT,
        "provenance": "outcome_backed_band",
    },
    "trigger_readiness_score": {
        "mode": "high",
        "unfavorable_quantile": 0.25,
        "favorable_quantile": 0.75,
        "fallback_min": 0.0,
        "fallback_max": 100.0,
        "provenance": "outcome_backed_band",
    },
    "breakout_readiness_score": {
        "mode": "high",
        "unfavorable_quantile": 0.25,
        "favorable_quantile": 0.75,
        "fallback_min": 0.0,
        "fallback_max": 100.0,
        "provenance": "outcome_backed_band",
    },
    "structural_score": {
        "mode": "high",
        "unfavorable_quantile": 0.25,
        "favorable_quantile": 0.75,
        "fallback_min": 0.0,
        "fallback_max": 100.0,
        "provenance": "outcome_backed_band",
    },
    "extension_atr": {
        "mode": "target_abs",
        "target": 0.0,
        "favorable_quantile": 0.35,
        "acceptable_quantile": 0.7,
        "fallback_scale": MAX_BREAKOUT_EXTENSION_ATR,
        "provenance": "outcome_backed_band",
    },
    "overhead_supply_score": {
        "mode": "low",
        "favorable_quantile": 0.25,
        "unfavorable_quantile": 0.75,
        "fallback_min": 0.0,
        "fallback_max": 100.0,
        "provenance": "outcome_backed_band",
    },
    "rvol": {
        "mode": "target_abs",
        "target": 1.0,
        "favorable_quantile": 0.35,
        "acceptable_quantile": 0.75,
        "fallback_scale": 1.0,
        "provenance": "outcome_backed_band",
    },
}

PRODUCTION_PIVOT_POSITION_BANDS = {
    "favorable": ("at_pivot",),
    "acceptable": ("below_pivot_but_near",),
    "unfavorable": ("far_below_pivot", "too_far_through_pivot"),
}

PRODUCTION_PIVOT_ZONES = {
    "prime": {"min_exclusive": -0.05, "max_inclusive": 0.0},
    "near": {"min_exclusive": -0.07, "max_inclusive": -0.05},
}

PRODUCTION_MA_CONFIRMATION = {
    "larger_ma_supportive_required": True,
    "tightening_to_short_ma_bonus": 10.0,
    "short_ma_rising_bonus": 4.0,
}

PRODUCTION_AVWAP_CONFLUENCE = {
    "resistance_penalty": 12.0,
}

PRODUCTION_AVWAP_LOCATION = {
    "blocked_distance_pct": 2.0,
    "caution_distance_pct": 4.0,
    "cluster_band_pct": 1.0,
    "cluster_min_count": 2,
    "blocked_score_penalty": 18.0,
    "caution_score_penalty": 8.0,
}

PRODUCTION_AVWAP_HIGH_CONCERN_ANCHORS = {
    "recent_downtrend_high",
    "major_pivot_high",
    "all_time_high",
    "high_52w",
    "recent_earnings_gap",
}

PRODUCTION_AVWAP_LOW_CONCERN_ANCHORS = {
    "covid_low",
    "russia_ukraine_war_start",
    "trump_inauguration_2025",
}

PRODUCTION_EXTENSION_CONTINUATION_MAX = 1.8

PRODUCTION_INTERACTION_WEIGHTS = {
    "pivot_zone_prime_bonus": 5.0,
    "pivot_zone_near_bonus": 1.5,
    "pivot_zone_far_penalty": 18.0,
    "approaching_pivot_cluster_bonus": 7.0,
    "pivot_trigger_alignment_bonus": 8.0,
    "pivot_breakout_alignment_bonus": 7.0,
    "trigger_breakout_structure_bonus": 12.0,
    "ma_confirmation_cluster_bonus": 9.0,
    "elite_cluster_bonus": 14.0,
    "extended_continuation_bonus": 8.0,
    "approaching_pivot_conflict_penalty": 12.0,
    "negative_conflict_penalty": 16.0,
    "overhead_conflict_penalty": 18.0,
    "late_extension_conflict_penalty": 18.0,
    "far_below_pivot_conflict_penalty": 18.0,
}

PRODUCTION_SIZING_LADDER = {
    "full": {"min_score": 92.0, "risk_multiplier": 1.0},
    "medium": {"min_score": 80.0, "risk_multiplier": 0.7},
    "small": {"min_score": 68.0, "risk_multiplier": 0.45},
    "starter": {"min_score": 55.0, "risk_multiplier": 0.25},
    "none": {"min_score": 0.0, "risk_multiplier": 0.0},
}

PRODUCTION_TRIGGER_WEIGHTS = {
    "opening_range_breakout": 1.00,
    "prior_day_high_break": 0.84,
    "vwap_reclaim_hold": 0.74,
    "intraday_consolidation_breakout": 0.38,
}
PRODUCTION_DEMOTED_TRIGGER_TYPES = ("intraday_consolidation_breakout",)
PRODUCTION_PRIMARY_SECTION_ORDER = (
    "actionable",
    "near_trigger",
    "stalking",
    "continuation",
    "avoid",
)

# Stable production runtime profile used by lightweight GitHub/daily runs.
# This is intentionally config-backed so production does not need to derive
# threshold or band distributions from research artifacts at runtime.
PRODUCTION_THRESHOLD_PROFILE = {
    "confidence": {
        "label": "provisional_insufficient_history",
        "sample_size": 3,
        "setup_state_diversity": 1,
        "setup_family_diversity": 1,
    },
    "pivot_distance": {
        "just_through_max_atr": 0.9,
        "too_far_through_atr": 1.35,
        "provenance": {
            "just_through_max_atr": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.MAX_RETEST_EXTENSION_ATR",
                "provisional": False,
            },
            "too_far_through_atr": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.MAX_BREAKOUT_EXTENSION_ATR",
                "provisional": False,
            },
        },
    },
    "actionability": {
        "rr_min_actionable": 1.15,
        "rr_min_potential": 1.35,
        "overhead_min": 55.0,
        "orderliness_min": 58.0,
        "provenance": {
            "rr_min_actionable": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.RR_MIN_ACTIONABLE",
                "provisional": False,
            },
            "rr_min_potential": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.RR_MIN_POTENTIAL",
                "provisional": False,
            },
            "overhead_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.OVERHEAD_MIN_SCORE",
                "provisional": False,
            },
            "orderliness_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.ORDERLINESS_MIN_SCORE",
                "provisional": False,
            },
        },
    },
    "participation": {
        "rs20_supportive_min": 0.0,
        "rvol_supportive_min": 1.0,
        "provenance": {
            "rs20_supportive_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.RS20_SUPPORTIVE_MIN",
                "provisional": False,
            },
            "rvol_supportive_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.RVOL_SUPPORTIVE_MIN",
                "provisional": False,
            },
        },
    },
    "score_gates": {
        "structural_min": 55.0,
        "breakout_watch_min": 58.0,
        "trigger_watch_min": 62.0,
        "provenance": {
            "structural_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.STRUCTURAL_MIN_SCORE",
                "provisional": False,
            },
            "breakout_watch_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.BREAKOUT_WATCH_MIN_SCORE",
                "provisional": False,
            },
            "trigger_watch_min": {
                "method_used": "legacy_preserved",
                "variable_source": "cfg.TRIGGER_WATCH_MIN_SCORE",
                "provisional": False,
            },
        },
    },
    "distribution_diagnostics": {
        "current_packet_extension_atr_median": -0.92,
        "current_packet_reward_risk_median": 0.07,
    },
    "band_distributions": {
        "pivot_distance_pct": {
            "mode": "target_abs",
            "feature": "pivot_distance_pct",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": -21.17,
            "median": -2.735,
            "max": -0.07,
            "target": 0.0,
            "favorable_cutoff": 2.1525,
            "acceptable_cutoff": 4.235,
        },
        "trigger_readiness_score": {
            "mode": "high",
            "feature": "trigger_readiness_score",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": 38.8,
            "median": 56.9,
            "max": 65.9,
            "unfavorable_cutoff": 56.9,
            "favorable_cutoff": 63.4,
        },
        "breakout_readiness_score": {
            "mode": "high",
            "feature": "breakout_readiness_score",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": 28.0,
            "median": 50.8,
            "max": 74.6,
            "unfavorable_cutoff": 44.025,
            "favorable_cutoff": 59.25,
        },
        "structural_score": {
            "mode": "high",
            "feature": "structural_score",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": 28.5,
            "median": 50.0,
            "max": 71.2,
            "unfavorable_cutoff": 39.825,
            "favorable_cutoff": 57.95,
        },
        "extension_atr": {
            "mode": "target_abs",
            "feature": "extension_atr",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": -4.51,
            "median": -0.92,
            "max": -0.05,
            "target": 0.0,
            "favorable_cutoff": 0.681,
            "acceptable_cutoff": 1.253,
        },
        "overhead_supply_score": {
            "mode": "low",
            "feature": "overhead_supply_score",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": 7.6,
            "median": 41.25,
            "max": 66.4,
            "favorable_cutoff": 30.275,
            "unfavorable_cutoff": 52.1,
        },
        "rvol": {
            "mode": "target_abs",
            "feature": "rvol",
            "sample_size": 94,
            "method_used": "provisional_if_needed",
            "provisional": True,
            "min": 0.0,
            "median": 0.49,
            "max": 2.4,
            "target": 1.0,
            "favorable_cutoff": 0.46,
            "acceptable_cutoff": 0.65,
        },
    },
}


def get_production_threshold_profile() -> dict:
    return copy.deepcopy(PRODUCTION_THRESHOLD_PROFILE)


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
