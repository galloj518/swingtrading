"""
Documented threshold registry for all scoring constants.

Every magic number in scoring.py and features.py should have an entry here.
This is a read-only reference — do not use it for mutable state.

Cross-reference with reports/threshold_recommendations_*.json for
empirically validated alternatives produced by walk-forward analysis.
"""


class ThresholdRegistry:
    """
    Single source of truth for all scoring thresholds.

    Attribute naming convention:
        - GATE_*       : hard pass/fail gate thresholds
        - CAP_*        : score ceiling values when conditions fail
        - BUY_*        : thresholds that trigger a buy action bias
        - LEAN_BUY_*   : thresholds for lean-buy (reduced conviction)
        - WATCH_*      : thresholds for watch list inclusion
        - ZONE_*       : price-zone distance thresholds (% from MA)
        - BLEND_*      : composite weighting coefficients
        - CALIB_*      : calibration evidence scaling
        - EXIT_*        : exit logic thresholds
        - RISK_*       : risk management thresholds
        - MACRO_*      : macro overlay stress thresholds
    """

    # =========================================================================
    # Gate score caps
    # =========================================================================

    GATE_WEEKLY_CAP: float = 30.0
    """
    Max composite score when weekly gate fails.
    Why: Broken weekly structure means no institutional sponsorship.
         Score of 30 = "research only, do not trade."
    Validated range (walk-forward): 25–35
    """

    GATE_DAILY_CAP: float = 50.0
    """
    Max composite score when daily gate fails.
    Why: Broken daily trend = unclear setup; wait for repair.
         Score of 50 = "possible watch, don't act yet."
    Validated range: 45–55
    """

    # =========================================================================
    # Action bias thresholds
    # =========================================================================

    BUY_IDEA_SCORE_MIN: float = 74.0
    """
    Minimum idea quality score to trigger "buy" action bias.
    Why: 74+ = solid weekly/daily structure, clean air, good RS.
    Validated range: 68–77
    """

    BUY_TIMING_SCORE_MIN: float = 78.0
    """
    Minimum entry timing score for "buy" bias.
    Why: 78+ = short-term MA stack constructive + price in ideal zone.
    Validated range: 72–81
    """

    BUY_COMPOSITE_SCORE_MIN: float = 74.0
    """
    Minimum blended composite score for "buy" bias.
    Blend: BLEND_IDEA_WEIGHT * idea + BLEND_TIMING_WEIGHT * timing.
    Validated range: 70–77
    """

    LEAN_BUY_IDEA_SCORE_MIN: float = 68.0
    LEAN_BUY_TIMING_SCORE_MIN: float = 60.0
    LEAN_BUY_COMPOSITE_SCORE_MIN: float = 65.0

    WATCH_IDEA_SCORE_MIN: float = 55.0

    # =========================================================================
    # Composite blend weights
    # =========================================================================

    BLEND_IDEA_WEIGHT: float = 0.68
    """
    Weight of idea quality in composite blended score.
    Why: Trend/RS/structure is the primary edge; timing is secondary.
         A great idea with OK timing beats a weak idea with perfect timing.
    """

    BLEND_TIMING_WEIGHT: float = 0.32
    """Complement of BLEND_IDEA_WEIGHT (they must sum to 1.0)."""

    # =========================================================================
    # Chart quality hard caps on composite
    # =========================================================================

    CHART_HARD_CAP_THRESHOLD: float = 35.0
    """Chart quality score below which composite is hard-capped."""

    CHART_HARD_CAP_VALUE: float = 52.0
    """Composite ceiling when chart quality is critically poor."""

    CHART_SOFT_CAP_THRESHOLD: float = 50.0
    """Chart quality score below which composite is soft-capped."""

    CHART_SOFT_CAP_VALUE: float = 70.0
    """Composite ceiling when chart quality is below par."""

    # =========================================================================
    # Zone distance thresholds (% distance from key MAs)
    # =========================================================================

    # 20 SMA zone — classic Shannon pullback setup
    ZONE_20_IDEAL_LOW_PCT: float = -2.0
    """
    Lower bound of ideal 20-SMA zone (% below 20 SMA).
    Why: Strong support typically halts pullbacks within 2% of the 20.
    """

    ZONE_20_IDEAL_HIGH_PCT: float = 2.5
    """Upper bound of ideal zone (% above 20 SMA — holding near support)."""

    ZONE_20_OUTER_LOW_PCT: float = -8.0
    """Beyond this below 20 SMA = too extended, trend integrity questionable."""

    ZONE_20_OUTER_HIGH_PCT: float = 8.0
    """Beyond this above 20 SMA = extended, not a pullback setup."""

    # 50 SMA zone — deeper pullback / trend repair setup
    ZONE_50_IDEAL_LOW_PCT: float = -3.0
    ZONE_50_IDEAL_HIGH_PCT: float = 6.0
    ZONE_50_OUTER_LOW_PCT: float = -12.0
    ZONE_50_OUTER_HIGH_PCT: float = 16.0

    # =========================================================================
    # Calibration evidence dynamic weight tiers
    # =========================================================================

    CALIB_MIN_SAMPLES: int = 6
    """Minimum matured-outcome signals before evidence gets meaningful weight."""

    CALIB_WEIGHT_TIER_1: float = 0.02
    """Evidence weight when sample_size < 6 (almost no history)."""

    CALIB_WEIGHT_TIER_2: float = 0.06
    """Evidence weight when 6 <= sample_size < 15."""

    CALIB_WEIGHT_TIER_3: float = 0.10
    """Evidence weight when 15 <= sample_size < 30."""

    CALIB_WEIGHT_TIER_4: float = 0.12
    """Evidence weight when sample_size >= 30 (well-calibrated)."""

    # =========================================================================
    # Exit policy
    # =========================================================================

    EXIT_PARTIAL_1_AT_R: float = 1.0
    """Take first partial (1/3 size) when trade reaches 1R profit."""

    EXIT_PARTIAL_2_AT_R: float = 2.0
    """Take second partial (1/3 size) when trade reaches 2R profit."""

    EXIT_TRAIL_START_AT_R: float = 1.0
    """Begin trailing the stop once the trade has reached this R multiple."""

    EXIT_TRAILING_ATR_MULT: float = 1.5
    """Trail stop = max(entry, recent_high - ATR * this multiplier)."""

    EXIT_TIME_STOP_DAYS: int = 5
    """Evaluate time-based exit after this many calendar days."""

    EXIT_TIME_STOP_LOSS_PCT: float = -3.0
    """Exit on time stop if return is below this % after EXIT_TIME_STOP_DAYS."""

    EXIT_MAX_HOLD_DAYS: int = 15
    """Hard maximum hold duration; evaluate for exit regardless of R."""

    # =========================================================================
    # Macro overlay stress thresholds
    # =========================================================================

    MACRO_STRESS_CONTANGO_INVERSION: float = 0.95
    """
    VIX term structure ratio (VIX3M / VIX) below which we flag stress.
    Contango = VIX3M/VIX > 1.0 (healthy).
    Backwardation = ratio < 0.95 (near-term fear > long-term fear).
    """

    MACRO_STRESS_CREDIT_WIDENING: float = -2.0
    """
    HYG-minus-LQD 20d return below this level flags credit stress.
    HYG underperforming LQD by >2% over 20 days = risk-off.
    """

    MACRO_STRESS_SKEW_ELEVATED: float = 140.0
    """CBOE SKEW index above this = elevated tail-risk hedging demand."""

    MACRO_DOWNGRADE_THRESHOLD: float = 50.0
    """Macro stress score above which risk appetite is downgraded one step."""

    MACRO_UPGRADE_THRESHOLD: float = 20.0
    """Macro stress score below which risk appetite may be upgraded one step."""

    # =========================================================================
    # Dynamic correlation
    # =========================================================================

    CORR_LOOKBACK_DAYS: int = 60
    """Rolling window for computing pairwise return correlations."""

    CORR_HIGH_THRESHOLD: float = 0.65
    """Correlation above which two names are treated as the same risk bucket."""

    # =========================================================================
    # Risk / sizing
    # =========================================================================

    RISK_DEFAULT_ATR_STOP_MULT: float = 1.5
    """ATR multiplier for initial stop placement below entry zone."""

    RISK_MAX_PARTICIPATION_PCT: float = 0.01
    """Maximum % of a symbol's average daily volume for one position."""
