"""
Deterministic Market Regime Model.

Classifies market regime from benchmark data using price structure only.
No LLM involved — same inputs always produce same output.
"""
from typing import Optional


def _leadership_score(state: dict) -> float:
    """Approximate benchmark leadership using structure and distance above key MAs."""
    score = 0.0
    if state.get("close_above_sma_50"):
        score += 30.0
    if state.get("close_above_sma_200"):
        score += 25.0
    if state.get("sma20_above_sma50"):
        score += 15.0
    if state.get("ma_stack") == "bullish":
        score += 15.0
    score += max(-10.0, min(15.0, float(state.get("dist_from_sma_50_pct", 0.0) or 0.0)))
    score += max(-8.0, min(10.0, float(state.get("dist_from_sma_200_pct", 0.0) or 0.0) / 2.0))
    return score


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
    spy_leadership = _leadership_score(spy)
    qqq_leadership = _leadership_score(qqq)
    soxx_leadership = _leadership_score(soxx)

    # Build signal checklist
    signals = {
        "spy_above_200":   spy.get("close_above_sma_200", False),
        "spy_above_50":    spy.get("close_above_sma_50", False),
        "spy_20_above_50": spy.get("sma20_above_sma50", False),
        "spy_stack":       spy.get("ma_stack") == "bullish",
        "qqq_above_50":    qqq.get("close_above_sma_50", False),
        "qqq_leading":     qqq_leadership >= spy_leadership + 3.0,
        "soxx_above_50":   soxx.get("close_above_sma_50", False),
        "soxx_stack":      soxx.get("ma_stack") == "bullish" and soxx_leadership >= qqq_leadership - 4.0,
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
    tech_leading = signals["qqq_leading"]
    semi_leading = soxx_leadership >= qqq_leadership - 2.0 and soxx.get("close_above_sma_50", False)
    industrial_confirm = dia.get("close_above_sma_50", False) and _leadership_score(dia) >= 35.0

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
        "leadership": {
            "spy": round(spy_leadership, 1),
            "qqq": round(qqq_leadership, 1),
            "soxx": round(soxx_leadership, 1),
            "dia": round(_leadership_score(dia), 1),
        },
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
