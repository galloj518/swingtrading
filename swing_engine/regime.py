"""
Deterministic Market Regime Model.

Two-layer design:
  Layer 1: Price-structure regime (original) — SPY/QQQ/SOXX/DIA MA signals.
  Layer 2: Macro overlay — VIX term structure, credit spreads, yield curve,
           options skew. Modulates risk_appetite without overriding the
           price-structure label.

Same inputs always produce same output (no LLM, no randomness).
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


def _calc_macro_overlay(macro_signals: dict) -> dict:
    """
    Convert macro indicator signals into a stress score and an appetite modifier.

    Stress score (0-100):
        0-20  → appetite_modifier = "upgrade"   (unusually benign conditions)
        20-50 → appetite_modifier = "neutral"   (normal background risk)
        50+   → appetite_modifier = "downgrade" (macro headwinds present)

    Components and their max stress contributions:
        Term structure inversion (VIX3M/VIX < 0.95):  +25
        Credit spread stress (HYG-LQD 20d < -2%):     +20
        Yield curve inversion:                         +15
        Elevated skew (SKEW > 140):                    +10
        Missing / unknown macro data:                   +5  (uncertainty premium)
    """
    from .constants import ThresholdRegistry as TR

    stress = 0.0
    detail = []

    term_ratio = macro_signals.get("vix_term_structure")
    critical_data_missing = False
    if term_ratio is not None:
        if term_ratio < TR.MACRO_STRESS_CONTANGO_INVERSION:
            stress += 25.0
            detail.append(f"VIX term structure inverted (ratio={term_ratio:.3f})")
        elif term_ratio < 1.0:
            stress += 10.0
            detail.append(f"VIX term structure flat (ratio={term_ratio:.3f})")
    else:
        stress += 5.0
        critical_data_missing = True
        detail.append("VIX term structure unavailable")

    credit = macro_signals.get("credit_signal")
    spread = macro_signals.get("hyg_lqd_spread_20d")
    if credit == "stressed":
        stress += 20.0
        detail.append(f"Credit spreads stressed (HYG-LQD={spread:.1f}%)")
    elif credit == "widening":
        stress += 10.0
        detail.append(f"Credit spreads widening (HYG-LQD={spread:.1f}%)")
    elif credit == "unknown" or credit is None:
        stress += 5.0
        critical_data_missing = True
        detail.append("Credit spread data unavailable")

    if macro_signals.get("curve_inverted"):
        stress += 15.0
        spread_val = macro_signals.get("yield_curve_spread", "?")
        detail.append(f"Yield curve inverted ({spread_val}%)")

    if macro_signals.get("skew_elevated"):
        stress += 10.0
        skew = macro_signals.get("skew_level", "?")
        detail.append(f"Options skew elevated (SKEW={skew})")

    stress = min(100.0, stress)

    if critical_data_missing:
        stress = max(stress, TR.MACRO_UPGRADE_THRESHOLD)
        detail.append("Macro overlay held neutral due to incomplete data")

    if stress < TR.MACRO_UPGRADE_THRESHOLD:
        modifier = "upgrade"
    elif stress < TR.MACRO_DOWNGRADE_THRESHOLD:
        modifier = "neutral"
    else:
        modifier = "downgrade"

    return {
        "macro_stress_score": round(stress, 1),
        "appetite_modifier": modifier,
        "macro_detail": detail,
        "vix_term_structure": term_ratio,
        "credit_signal": credit,
        "curve_inverted": macro_signals.get("curve_inverted"),
        "skew_elevated": macro_signals.get("skew_elevated"),
    }


def calc_regime(spy: dict, qqq: dict, soxx: dict, dia: dict,
                vix_close: Optional[float] = None,
                event_risk: dict = None,
                macro_signals: Optional[dict] = None) -> dict:
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

    # Layer 1: price-structure risk appetite
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

    # Layer 2: macro overlay — modulate risk_appetite without changing regime label
    macro_overlay = _calc_macro_overlay(macro_signals or {})
    modifier = macro_overlay["appetite_modifier"]

    _appetite_order = ["defensive", "minimal", "reduced", "moderate", "full"]
    if risk_appetite != "defensive":  # never upgrade past defensive
        idx = _appetite_order.index(risk_appetite)
        if modifier == "downgrade" and idx > 0:
            risk_appetite = _appetite_order[idx - 1]
        elif modifier == "upgrade" and idx < len(_appetite_order) - 1:
            risk_appetite = _appetite_order[idx + 1]

    # Swing bias
    if regime in ("bullish", "lean_bullish") and risk_appetite in ("full", "moderate"):
        swing_bias = "long"
    elif regime in ("bearish", "lean_bearish"):
        swing_bias = "short"
    else:
        swing_bias = "neutral"

    # Caution flags (price-structure)
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

    # Macro overlay flags
    macro_flags = macro_overlay.get("macro_detail", [])
    if macro_overlay["macro_stress_score"] >= 50:
        flags.extend(macro_flags)

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
        "macro_overlay": macro_overlay,
    }


def regime_summary_text(regime: dict) -> str:
    """Generate a one-line regime summary for the dashboard."""
    r = regime["regime"].upper().replace("_", " ")
    bias = regime["swing_bias"]
    appetite = regime["risk_appetite"]
    bull = regime["bull_signals"]
    total = regime["total_signals"]
    vix = regime.get("vix_context", "?")

    macro = regime.get("macro_overlay", {})
    stress = macro.get("macro_stress_score")
    stress_str = f" | Macro stress: {stress:.0f}/100" if stress is not None else ""

    line = f"{r} ({bull}/{total}) | Bias: {bias} | Risk: {appetite} | VIX: {vix}{stress_str}"

    flags = regime.get("caution_flags", [])
    if flags:
        line += f" | CAUTION: {'; '.join(flags[:2])}"  # cap at 2 for readability
    return line
