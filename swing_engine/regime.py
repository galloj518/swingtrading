"""
Deterministic Market Regime Model.

Classifies market regime from benchmark data using price structure only.
No LLM involved — same inputs always produce same output.
"""
from typing import Optional


def calc_regime(spy: dict, qqq: dict, soxx: dict, dia: dict,
                vix_close: Optional[float] = None,
                event_risk: dict = None) -> dict:
    """
    Calculate market regime from benchmark MA states.

    Args:
        spy, qqq, soxx, dia: MA state dicts from features.extract_ma_state()
        vix_close: Current VIX level
        event_risk: Event context dict

    Returns:
        Regime classification with actionable fields.
    """
    # Build signal checklist
    signals = {
        "spy_above_200":   spy.get("close_above_sma_200", False),
        "spy_above_50":    spy.get("close_above_sma_50", False),
        "spy_20_above_50": spy.get("sma20_above_sma50", False),
        "spy_stack":       spy.get("ma_stack") == "bullish",
        "qqq_above_50":    qqq.get("close_above_sma_50", False),
        "qqq_leading":     True,  # placeholder — needs RS data
        "soxx_above_50":   soxx.get("close_above_sma_50", False),
        "soxx_stack":      soxx.get("ma_stack") == "bullish",
        "dia_above_50":    dia.get("close_above_sma_50", False),
        "vix_below_20":    (vix_close or 25) < 20,
        "vix_below_30":    (vix_close or 25) < 30,
    }

    bull_count = sum(bool(v) for v in signals.values())
    total = len(signals)

    # Regime classification
    if bull_count >= 9:
        regime = "bullish"
    elif bull_count >= 7:
        regime = "lean_bullish"
    elif bull_count >= 5:
        regime = "neutral"
    elif bull_count >= 3:
        regime = "lean_bearish"
    else:
        regime = "bearish"

    # VIX context
    vix = vix_close or 25
    if vix < 15:
        vix_ctx = "low_complacent"
    elif vix < 20:
        vix_ctx = "normal"
    elif vix < 30:
        vix_ctx = "elevated"
    elif vix < 40:
        vix_ctx = "fear"
    else:
        vix_ctx = "extreme_fear"

    # Leadership signals
    tech_leading = qqq.get("close_above_sma_50", False) and qqq.get("ma_stack") == "bullish"
    semi_leading = soxx.get("close_above_sma_50", False) and soxx.get("ma_stack") == "bullish"
    industrial_confirm = dia.get("close_above_sma_50", False)

    # Risk appetite
    er = event_risk or {}
    if er.get("high_risk_imminent"):
        risk_appetite = "defensive"
    elif regime in ("bearish", "lean_bearish"):
        risk_appetite = "minimal"
    elif regime == "neutral" or er.get("elevated_risk"):
        risk_appetite = "reduced"
    elif regime == "lean_bullish":
        risk_appetite = "moderate"
    else:
        risk_appetite = "full"

    # Swing bias
    if regime in ("bullish", "lean_bullish") and risk_appetite in ("full", "moderate"):
        swing_bias = "long"
    elif regime in ("bearish", "lean_bearish"):
        swing_bias = "short"
    else:
        swing_bias = "neutral"

    # Caution flags
    flags = []
    if vix_ctx in ("fear", "extreme_fear"):
        flags.append(f"VIX elevated at {vix:.0f}")
    if not spy.get("close_above_sma_200"):
        flags.append("SPY below 200 SMA")
    if not tech_leading:
        flags.append("Tech not leading")
    if not industrial_confirm:
        flags.append("Industrials not confirming")
    if er.get("high_risk_imminent"):
        flags.append("Macro event imminent")
    if spy.get("ma_stack") == "bearish":
        flags.append("SPY MA stack bearish")

    return {
        "regime": regime,
        "bull_signals": bull_count,
        "total_signals": total,
        "vix_context": vix_ctx,
        "vix_level": round(vix, 1) if vix_close else None,
        "tech_leading": tech_leading,
        "semi_leading": semi_leading,
        "industrial_confirm": industrial_confirm,
        "risk_appetite": risk_appetite,
        "swing_bias": swing_bias,
        "caution_flags": flags,
        "signals_detail": signals,
    }


def regime_summary_text(regime: dict) -> str:
    """Generate a one-line regime summary for the dashboard."""
    r = regime["regime"].upper().replace("_", " ")
    bias = regime["swing_bias"]
    appetite = regime["risk_appetite"]
    bull = regime["bull_signals"]
    total = regime["total_signals"]
    vix = regime.get("vix_context", "?")

    line = f"{r} ({bull}/{total}) | Bias: {bias} | Risk: {appetite} | VIX: {vix}"

    flags = regime.get("caution_flags", [])
    if flags:
        line += f" | CAUTION: {'; '.join(flags)}"
    return line
