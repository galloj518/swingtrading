"""
Deterministic market regime model with breakout participation overlay.
"""
from __future__ import annotations

from typing import Optional


def _leadership_score(state: dict) -> float:
    score = 0.0
    if state.get("close_above_sma_50"):
        score += 28.0
    if state.get("close_above_sma_200"):
        score += 24.0
    if state.get("sma20_above_sma50"):
        score += 14.0
    if state.get("ma_stack") == "bullish":
        score += 18.0
    score += max(-10.0, min(16.0, float(state.get("dist_from_sma_50_pct", 0.0) or 0.0)))
    return score


def _calc_macro_overlay(macro_signals: dict) -> dict:
    stress = 0.0
    detail = []
    term_ratio = macro_signals.get("vix_term_structure")
    if term_ratio is not None:
        if term_ratio < 0.95:
            stress += 25.0
            detail.append(f"VIX term structure inverted ({term_ratio:.3f})")
        elif term_ratio < 1.0:
            stress += 10.0
            detail.append(f"VIX term structure flat ({term_ratio:.3f})")
    else:
        stress += 5.0
        detail.append("VIX term structure unavailable")

    credit = macro_signals.get("credit_signal")
    if credit == "stressed":
        stress += 20.0
        detail.append("Credit stressed")
    elif credit == "widening":
        stress += 10.0
        detail.append("Credit widening")

    if macro_signals.get("curve_inverted"):
        stress += 15.0
        detail.append("Yield curve inverted")
    if macro_signals.get("skew_elevated"):
        stress += 10.0
        detail.append("Options skew elevated")

    stress = min(100.0, stress)
    modifier = "upgrade" if stress < 20 else "neutral" if stress < 50 else "downgrade"
    return {"macro_stress_score": round(stress, 1), "appetite_modifier": modifier, "macro_detail": detail}


def calc_regime(spy: dict, qqq: dict, soxx: dict, dia: dict, vix_close: Optional[float] = None, event_risk: dict = None, macro_signals: Optional[dict] = None) -> dict:
    spy_leadership = _leadership_score(spy)
    qqq_leadership = _leadership_score(qqq)
    soxx_leadership = _leadership_score(soxx)
    signals = {
        "spy_above_200": spy.get("close_above_sma_200", False),
        "spy_above_50": spy.get("close_above_sma_50", False),
        "qqq_above_50": qqq.get("close_above_sma_50", False),
        "qqq_leading": qqq_leadership >= spy_leadership + 3.0,
        "soxx_above_50": soxx.get("close_above_sma_50", False),
        "soxx_stack": soxx.get("ma_stack") == "bullish",
        "dia_above_50": dia.get("close_above_sma_50", False),
        "dia_stack": dia.get("ma_stack") == "bullish",
        "vix_below_20": (vix_close or 25) < 20,
        "vix_below_30": (vix_close or 25) < 30,
    }
    bull_count = sum(bool(value) for value in signals.values())
    total = len(signals)
    regime = "bullish" if bull_count >= 8 else "lean_bullish" if bull_count >= 6 else "neutral" if bull_count >= 4 else "lean_bearish" if bull_count >= 2 else "bearish"
    vix = vix_close or 25
    vix_context = "low_complacent" if vix < 15 else "normal" if vix < 20 else "elevated" if vix < 30 else "fear" if vix < 40 else "extreme_fear"
    er = event_risk or {}
    risk_appetite = "defensive" if er.get("high_risk_imminent") else "minimal" if regime in {"bearish", "lean_bearish"} else "reduced" if regime == "neutral" or er.get("elevated_risk") else "moderate" if regime == "lean_bullish" else "full"
    macro_overlay = _calc_macro_overlay(macro_signals or {})
    appetite_order = ["defensive", "minimal", "reduced", "moderate", "full"]
    if risk_appetite != "defensive":
        idx = appetite_order.index(risk_appetite)
        if macro_overlay["appetite_modifier"] == "downgrade" and idx > 0:
            risk_appetite = appetite_order[idx - 1]
        elif macro_overlay["appetite_modifier"] == "upgrade" and idx < len(appetite_order) - 1:
            risk_appetite = appetite_order[idx + 1]
    swing_bias = "long" if regime in {"bullish", "lean_bullish"} and risk_appetite in {"full", "moderate"} else "short" if regime in {"bearish", "lean_bearish"} else "neutral"
    flags = []
    if vix_context in {"fear", "extreme_fear"}:
        flags.append(f"VIX elevated at {vix:.0f}")
    if not spy.get("close_above_sma_200"):
        flags.append("SPY below 200 SMA")
    if not signals["qqq_leading"]:
        flags.append("Tech not leading")
    if er.get("high_risk_imminent"):
        flags.append("Macro event imminent")
    if macro_overlay["macro_stress_score"] >= 50:
        flags.extend(macro_overlay.get("macro_detail", []))
    return {
        "regime": regime,
        "bull_signals": bull_count,
        "total_signals": total,
        "vix_context": vix_context,
        "vix_level": round(vix, 1) if vix_close else None,
        "tech_leading": signals["qqq_leading"],
        "semi_leading": soxx_leadership >= qqq_leadership - 2.0 and soxx.get("close_above_sma_50", False),
        "industrial_confirm": signals["dia_above_50"],
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
        "breakout_overlay": {},
    }


def calc_breakout_regime_overlay(packets: dict) -> dict:
    watchlist_packets = [packet for symbol, packet in packets.items() if symbol not in {"SPY", "QQQ", "SOXX", "DIA"}]
    if not watchlist_packets:
        return {"breakout_environment": "unknown", "detail": "No watchlist packets"}
    total = len(watchlist_packets)
    near_high = sum(1 for pkt in watchlist_packets if float(pkt.get("breakout_features", {}).get("near_high", {}).get("score", 0) or 0) >= 60)
    ready = sum(1 for pkt in watchlist_packets if float(pkt.get("score", {}).get("breakout_readiness_score", 0) or 0) >= 65)
    triggered = sum(1 for pkt in watchlist_packets if pkt.get("intraday_trigger", {}).get("primary", {}).get("triggered_now"))
    rs_strong = sum(1 for pkt in watchlist_packets if float(pkt.get("relative_strength", {}).get("rs_20d", 0) or 0) >= 3)
    score = (near_high / total) * 28 + (ready / total) * 34 + (triggered / total) * 18 + (rs_strong / total) * 20
    environment = "hot" if score >= 58 else "supportive" if score >= 42 else "mixed" if score >= 28 else "thin"
    return {
        "breakout_environment": environment,
        "near_high_ratio": round(near_high / total, 2),
        "ready_ratio": round(ready / total, 2),
        "triggered_ratio": round(triggered / total, 2),
        "rs_strong_ratio": round(rs_strong / total, 2),
        "score": round(score, 1),
        "detail": f"Near highs {near_high}/{total}, ready {ready}/{total}, triggered {triggered}/{total}",
    }


def regime_summary_text(regime: dict) -> str:
    headline = regime["regime"].upper().replace("_", " ")
    line = f"{headline} ({regime['bull_signals']}/{regime['total_signals']}) | Bias: {regime['swing_bias']} | Risk: {regime['risk_appetite']} | VIX: {regime.get('vix_context', '?')}"
    overlay = regime.get("breakout_overlay", {})
    if overlay:
        line += f" | Breakouts: {overlay.get('breakout_environment', '?')}"
    flags = regime.get("caution_flags", [])
    if flags:
        line += f" | CAUTION: {'; '.join(flags[:2])}"
    return line
