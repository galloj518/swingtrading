"""
Gated Scoring Engine.

Hierarchical, not additive:
  Gate 1: Weekly regime (pass/fail) — caps score at 30 if fail
  Gate 2: Daily trend (pass/fail)  — caps score at 50 if fail
  Gate 3: Entry quality scoring    — only matters if gates 1+2 pass

This prevents the system from producing misleading scores on names
with broken weekly structure.
"""
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from . import config as cfg


def _check_weekly_gate(weekly_state: dict) -> dict:
    """
    Gate 1: Weekly trend must show minimum constructive structure.
    Requires: close above weekly 20 SMA.
    """
    key = cfg.GATE_WEEKLY_REQUIRES  # "close_above_sma_20"
    passed = weekly_state.get(key, False)
    return {
        "passed": bool(passed),
        "check": key,
        "detail": "Weekly close above 20 SMA" if passed else "FAILED: below weekly 20 SMA",
    }


def _check_daily_gate(daily_state: dict) -> dict:
    """
    Gate 2: Daily trend must show minimum structure.
    Requires: close above daily 50 SMA.
    """
    key = cfg.GATE_DAILY_REQUIRES  # "close_above_sma_50"
    passed = daily_state.get(key, False)
    return {
        "passed": bool(passed),
        "check": key,
        "detail": "Daily close above 50 SMA" if passed else "FAILED: below daily 50 SMA",
    }


def _score_entry_quality(daily_state: dict, weekly_state: dict,
                          intra_state: dict, avwap_map: dict,
                          rs: dict, confluence: dict,
                          event_risk: dict, earnings: dict) -> dict:
    """
    Gate 3: Score entry quality (0-100) for names that passed both gates.
    Now includes MA direction awareness — a stock with falling short-term
    MAs is losing momentum even if structure is intact.
    """
    score = 50
    reasons = []
    price = daily_state.get("last_close", 0)

    # --- Weekly alignment (max +15) ---
    if weekly_state.get("sma5_above_sma10"):
        score += 8
        reasons.append("+8 weekly 5>10")
    if weekly_state.get("sma10_above_sma20"):
        score += 7
        reasons.append("+7 weekly 10>20")

    # --- Daily MA alignment (max +20) ---
    if daily_state.get("close_above_sma_10"):
        score += 5
        reasons.append("+5 above daily 10")
    if daily_state.get("close_above_sma_20"):
        score += 5
        reasons.append("+5 above daily 20")

    # Stack bonus — but only if short-term MAs are not falling
    # Shannon: ordered stack with falling 5/10 means momentum rolling over
    daily_stack_bullish = (
        daily_state.get("sma10_above_sma20") and
        daily_state.get("sma20_above_sma50")
    )
    sma5_falling = daily_state.get("sma_5_direction") == "falling"
    sma10_falling = daily_state.get("sma_10_direction") == "falling"

    if daily_stack_bullish and not sma5_falling and not sma10_falling:
        score += 10
        reasons.append("+10 daily MA stack bullish with rising short MAs")
    elif daily_stack_bullish and (sma5_falling or sma10_falling):
        score += 3
        reasons.append("+3 daily MA stack ordered BUT short MAs rolling (momentum fading)")
    elif not daily_stack_bullish:
        pass  # no bonus

    # --- Daily 200 SMA (max +5) ---
    if daily_state.get("close_above_sma_200"):
        score += 5
        reasons.append("+5 above 200 SMA")
    else:
        score -= 5
        reasons.append("-5 below 200 SMA")

    # =================================================================
    # MA DIRECTION — this is the timing layer
    # Structure can be right but timing wrong if MAs are falling
    # =================================================================

    # --- DAILY MA DIRECTIONS ---

    # Daily 5 SMA direction (most sensitive to momentum)
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    if sma5_dir == "falling":
        score -= 8
        reasons.append("-8 daily 5 SMA FALLING (momentum fading)")
    elif sma5_dir == "rising":
        score += 3
        reasons.append("+3 daily 5 SMA rising")

    # Daily 10 SMA direction
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    if sma10_dir == "falling":
        score -= 6
        reasons.append("-6 daily 10 SMA FALLING")
    elif sma10_dir == "rising":
        score += 2
        reasons.append("+2 daily 10 SMA rising")

    # Daily 20 SMA direction (trend health)
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    if sma20_dir == "falling":
        score -= 10
        reasons.append("-10 daily 20 SMA FALLING (trend weakening)")
    elif sma20_dir == "rising":
        score += 5
        reasons.append("+5 daily 20 SMA rising (healthy trend)")

    # Daily 50 SMA direction (intermediate trend)
    sma50_dir = daily_state.get("sma_50_direction", "unknown")
    if sma50_dir == "falling":
        score -= 5
        reasons.append("-5 daily 50 SMA FALLING (intermediate trend weak)")
    elif sma50_dir == "rising":
        score += 3
        reasons.append("+3 daily 50 SMA rising")

    # Daily 200 SMA direction (long-term trend)
    sma200_dir = daily_state.get("sma_200_direction", "unknown")
    if sma200_dir == "falling":
        score -= 5
        reasons.append("-5 daily 200 SMA FALLING (long-term trend deteriorating)")
    elif sma200_dir == "rising":
        score += 3
        reasons.append("+3 daily 200 SMA rising")

    # --- WEEKLY MA DIRECTIONS ---

    # Weekly 5 SMA direction
    w_sma5_dir = weekly_state.get("sma_5_direction", "unknown")
    if w_sma5_dir == "falling":
        score -= 7
        reasons.append("-7 WEEKLY 5 SMA FALLING")
    elif w_sma5_dir == "rising":
        score += 3
        reasons.append("+3 weekly 5 SMA rising")

    # Weekly 10 SMA direction
    w_sma10_dir = weekly_state.get("sma_10_direction", "unknown")
    if w_sma10_dir == "falling":
        score -= 7
        reasons.append("-7 WEEKLY 10 SMA FALLING (weekly momentum weak)")
    elif w_sma10_dir == "rising":
        score += 3
        reasons.append("+3 weekly 10 SMA rising")

    # Weekly 20 SMA direction (the big one — weekly trend direction)
    w_sma20_dir = weekly_state.get("sma_20_direction", "unknown")
    if w_sma20_dir == "falling":
        score -= 10
        reasons.append("-10 WEEKLY 20 SMA FALLING (primary trend weakening)")
    elif w_sma20_dir == "rising":
        score += 5
        reasons.append("+5 weekly 20 SMA rising (primary trend healthy)")

    # --- TOMORROW BIAS ---
    sma5_tmw = daily_state.get("sma_5_tomorrow_bias", "unknown")
    sma10_tmw = daily_state.get("sma_10_tomorrow_bias", "unknown")
    if sma5_tmw == "will_fall" and sma10_tmw == "will_fall":
        score -= 5
        reasons.append("-5 both 5+10 SMA will fall tomorrow")
    elif sma5_tmw == "will_rise" and sma10_tmw == "will_rise":
        score += 3
        reasons.append("+3 both 5+10 SMA will rise tomorrow")

    # Extended from 10 SMA — don't chase
    dist_10 = daily_state.get("dist_from_sma_10_pct", 0)
    if dist_10 and dist_10 > 4:
        score -= 8
        reasons.append(f"-8 extended {dist_10:.1f}% above 10 SMA (don't chase)")
    elif dist_10 and dist_10 < -4:
        score -= 5
        reasons.append(f"-5 too far below 10 SMA ({dist_10:.1f}%)")

    # --- Intraday alignment (max +10) ---
    if intra_state.get("ma_stack") == "bullish":
        score += 10
        reasons.append("+10 intraday stack bullish")
    elif intra_state.get("close_above_sma_50"):
        score += 5
        reasons.append("+5 intraday above 50")

    # --- AVWAP location (enhanced) ---
    # Check multiple AVWAPs, not just YTD
    avwap_above = 0
    avwap_below = 0
    for label, data in avwap_map.items():
        av = data.get("avwap", 0)
        if av and price:
            if price > av:
                avwap_above += 1
            else:
                avwap_below += 1

    if avwap_above > 0 and avwap_below == 0:
        score += 8
        reasons.append(f"+8 above ALL {avwap_above} AVWAPs")
    elif avwap_above > avwap_below:
        score += 4
        reasons.append(f"+4 above {avwap_above}/{avwap_above+avwap_below} AVWAPs")
    elif avwap_below > avwap_above and avwap_below > 0:
        score -= 5
        reasons.append(f"-5 below majority of AVWAPs ({avwap_below}/{avwap_above+avwap_below})")

    # --- Relative strength (max +10) ---
    rs20 = rs.get("rs_20d")
    if rs20 is not None:
        if rs20 > 3:
            score += 10
            reasons.append(f"+10 strong RS20 ({rs20})")
        elif rs20 > 0:
            score += 5
            reasons.append(f"+5 positive RS20 ({rs20})")
        elif rs20 < -3:
            score -= 8
            reasons.append(f"-8 weak RS20 ({rs20})")

    # --- Relative volume context ---
    rvol = daily_state.get("rvol", 1.0)
    dist_20 = daily_state.get("dist_from_sma_20_pct", 0)
    dist_10_vol = daily_state.get("dist_from_sma_10_pct", 0)
    if rvol:
        # Low volume pullback to MAs = healthy (institutional holding, not selling)
        if rvol < 0.7 and dist_20 and -3 <= dist_20 <= 0:
            score += 8
            reasons.append(f"+8 LOW volume pullback ({rvol}x) = healthy consolidation")
        # High volume selloff = institutions distributing
        elif rvol > 1.8 and dist_20 and dist_20 < -2:
            score -= 10
            reasons.append(f"-10 HIGH volume selloff ({rvol}x) = distribution")
        # High volume near highs = demand
        elif rvol > 1.5 and dist_10_vol and dist_10_vol > 0:
            score += 5
            reasons.append(f"+5 elevated volume near highs ({rvol}x)")

    # --- Confluence bonus (max +5) ---
    conf_score = confluence.get("score", 0)
    if conf_score >= 3:
        score += 5
        reasons.append(f"+5 high confluence ({conf_score} levels)")

    # --- Event risk penalties ---
    if event_risk.get("high_risk_imminent"):
        score -= 15
        reasons.append("-15 HIGH event risk")
    elif event_risk.get("elevated_risk"):
        score -= 8
        reasons.append("-8 elevated event risk")

    if earnings.get("warning"):
        score -= 10
        reasons.append("-10 earnings imminent")

    score = max(0, min(100, score))
    return {"score": score, "reasons": reasons}


def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict,
                 avwap_map: dict, rs: dict, confluence: dict,
                 event_risk: dict, earnings: dict,
                 regime: dict = None) -> dict:
    """
    Full gated scoring pipeline for a symbol.
    Includes post-scoring adjustments for:
    - Price above entry zone (chasing penalty)
    - Regime context (hard cap in bearish regime)
    - Hard cap when falling 5 SMA
    """
    # Gate 0: Regime — bearish regime caps all long scores
    regime = regime or {}
    regime_label = regime.get("regime", "neutral")
    risk_appetite = regime.get("risk_appetite", "full")

    # Gate 1: Weekly
    wg = _check_weekly_gate(weekly_state)

    if not wg["passed"]:
        return {
            "score": 20,
            "quality": "F — weekly trend broken",
            "weekly_gate": wg,
            "daily_gate": {"passed": False, "detail": "Skipped — weekly failed"},
            "reasons": [wg["detail"], "Score capped at 30"],
            "action_bias": "avoid",
        }

    # Gate 2: Daily
    dg = _check_daily_gate(daily_state)

    if not dg["passed"]:
        return {
            "score": 40,
            "quality": "D — daily trend broken",
            "weekly_gate": wg,
            "daily_gate": dg,
            "reasons": [wg["detail"], dg["detail"], "Score capped at 50"],
            "action_bias": "wait",
        }

    # Gate 3: Entry quality
    eq = _score_entry_quality(
        daily_state, weekly_state, intra_state,
        avwap_map, rs, confluence, event_risk, earnings,
    )

    score = eq["score"]
    reasons = [wg["detail"], dg["detail"]] + eq["reasons"]

    # =================================================================
    # POST-SCORING ADJUSTMENTS — these are hard constraints
    # =================================================================

    # HARD CAP: If daily 5 SMA is falling, max score is 75
    # Shannon: you don't get an A+ setup with declining momentum
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    if sma5_dir == "falling" and score > 75:
        score = 75
        reasons.append("CAPPED at 75: daily 5 SMA falling (momentum not confirmed)")

    # HARD CAP: If daily 5 AND 10 SMA both falling, max score is 60
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    if sma5_dir == "falling" and sma10_dir == "falling" and score > 60:
        score = 60
        reasons.append("CAPPED at 60: daily 5+10 SMA both falling")

    # HARD CAP: If daily 20 SMA falling, max score is 65
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    if sma20_dir == "falling" and score > 65:
        score = 65
        reasons.append("CAPPED at 65: daily 20 SMA falling (trend weakening)")

    # ZONE PENALTY: Price above entry zone = chasing
    price = daily_state.get("last_close", 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    entry_high = max(sma10, sma20) if sma10 and sma20 else price
    if price and entry_high and price > entry_high:
        chase_pct = (price / entry_high - 1) * 100
        if chase_pct > 3:
            penalty = 20
            reasons.append(f"-{penalty} CHASING: price {chase_pct:.1f}% above entry zone")
        elif chase_pct > 1:
            penalty = 10
            reasons.append(f"-{penalty} above entry zone by {chase_pct:.1f}%")
        else:
            penalty = 5
            reasons.append(f"-{penalty} slightly above entry zone")
        score -= penalty

    # REGIME GATE: Bearish regime caps all long swing scores
    if regime_label in ("bearish", "lean_bearish") and risk_appetite in ("defensive", "minimal"):
        if score > 60:
            score = 60
            reasons.append(f"CAPPED at 60: regime {regime_label}, risk appetite {risk_appetite}")
    elif regime_label == "lean_bearish" and score > 75:
        score = 75
        reasons.append(f"CAPPED at 75: regime {regime_label}")

    score = max(0, min(100, score))

    quality = (
        "A — strong"   if score >= 80 else
        "B — good"     if score >= 65 else
        "C — marginal" if score >= 50 else
        "D — weak"     if score >= 35 else
        "F — avoid"
    )

    action_bias = (
        "buy"      if score >= 75 else
        "lean_buy" if score >= 65 else
        "wait"     if score >= 45 else
        "avoid"
    )

    return {
        "score": score,
        "quality": quality,
        "weekly_gate": wg,
        "daily_gate": dg,
        "reasons": reasons,
        "action_bias": action_bias,
    }


# =============================================================================
# FULL TRADE PLAN GENERATOR
# Merges the best of deterministic analysis with the notebook's rich fields:
# gap plans, partial takes, max chase, time horizon, upgrade conditions
# =============================================================================

def classify_setup(daily_state: dict, score: int, action_bias: str,
                   recent_high: dict, price: float,
                   entry_zone: dict = None, pivots: dict = None,
                   event_risk: dict = None, weekly_state: dict = None) -> dict:
    """
    Generate a complete trade plan with specific triggers, gap scenarios,
    partial take plan, max chase logic, and upgrade conditions.
    """
    entry_zone = entry_zone or {}
    pivots = pivots or {}
    event_risk = event_risk or {}
    weekly_state = weekly_state or {}

    sma5 = daily_state.get("sma_5", 0)
    sma10 = daily_state.get("sma_10", 0)
    sma20 = daily_state.get("sma_20", 0)
    sma50 = daily_state.get("sma_50", 0)
    atr = daily_state.get("atr", price * 0.02 if price else 1)

    dist_from_10 = daily_state.get("dist_from_sma_10_pct", 0)
    dist_from_20 = daily_state.get("dist_from_sma_20_pct", 0)
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    rvol = daily_state.get("rvol", 1.0)

    rh_price = recent_high.get("price", price * 1.2) if recent_high else price * 1.2
    rl_price = entry_zone.get("stop", price * 0.95)

    r1 = pivots.get("r1")
    r2 = pivots.get("r2")
    s1 = pivots.get("s1")
    stop = entry_zone.get("stop", 0)
    t1 = entry_zone.get("target_1", 0)
    t2 = entry_zone.get("target_2", 0)
    ez_low = entry_zone.get("entry_low", sma20)
    ez_high = entry_zone.get("entry_high", sma10)
    in_zone = entry_zone.get("in_zone", False)

    has_event_risk = event_risk.get("high_risk_imminent") or event_risk.get("elevated_risk")

    # --- BASE fields every plan gets ---
    base = {
        "max_chase_pct": 0,
        "time_horizon": "N/A",
        "gap_up_plan": "N/A",
        "gap_down_plan": "N/A",
        "partial_take_plan": "N/A",
        "position_size_guidance": "standard",
        "upgrade_conditions": [],
    }

    # === NO SETUP ===
    if action_bias == "avoid":
        return {**base,
            "type": "no_setup",
            "description": "Conditions not met — weekly or daily structure broken",
            "trigger": None,
            "watch_for": None,
            "invalidation": None,
            "upgrade_conditions": [
                f"Weekly close back above weekly 20 SMA",
                f"Daily close above {_fmt2(sma50)} (50 SMA)",
            ],
        }

    # === EXTENDED: too far from MAs ===
    if dist_from_10 and dist_from_10 > 5:
        pullback_target = round(sma10 + 0.5 * atr, 2) if sma10 else None
        return {**base,
            "type": "extended_wait",
            "description": f"Extended {dist_from_10:.1f}% above 10 SMA. Do NOT chase.",
            "trigger": f"Wait for pullback to {_fmt2(pullback_target)}",
            "watch_for": f"Price pulling back toward {_fmt2(sma10)} on declining volume (RVol < 0.8)",
            "invalidation": f"10 SMA starts falling",
            "gap_up_plan": "Absolutely do not chase a gap up from extended levels",
            "gap_down_plan": f"If gaps to {_fmt2(sma10)} area, watch for intraday hold — could become entry",
            "max_chase_pct": 0,
            "time_horizon": "Wait 2-5 days for pullback",
            "upgrade_conditions": [
                f"Price pulls back to {_fmt2(sma10)} - {_fmt2(sma20)} zone on light volume",
                f"5 SMA catches up to price (distance narrows to < 2%)",
            ],
        }

    # === BREAKOUT: near recent high ===
    if rh_price and price and abs(price / rh_price - 1) < 0.02 and score >= 55:
        breakout_trigger = round(rh_price + 0.1 * atr, 2)
        return {**base,
            "type": "breakout",
            "description": f"Testing recent high {_fmt2(rh_price)}",
            "trigger": f"BUY on close above {_fmt2(breakout_trigger)}",
            "watch_for": f"Volume expansion above {_fmt2(rh_price)} (need RVol > 1.3). Weak volume = false breakout.",
            "invalidation": f"Rejection and close back below {_fmt2(sma10)}",
            "gap_up_plan": f"If gaps above {_fmt2(breakout_trigger)}: buy half size on open, add on first pullback to hold breakout level",
            "gap_down_plan": f"If gaps down: no action, setup is not triggered",
            "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), 1/3 at R2 ({_fmt2(r2)}), trail last 1/3 with 10 SMA",
            "max_chase_pct": round(0.5 * atr / price * 100, 1) if price else 0.5,
            "time_horizon": "1-3 days for breakout confirmation, hold 5-10 days",
            "position_size_guidance": "half size until breakout confirms" if has_event_risk else "standard",
        }

    # === PULLBACK TO RISING 20 SMA (classic Shannon) ===
    if (daily_state.get("sma10_above_sma20") and
        daily_state.get("sma20_above_sma50") and
        sma20_dir == "rising" and
        -3 <= dist_from_20 <= 1.5):

        if sma5_dir == "falling":
            return {**base,
                "type": "pullback_developing",
                "description": f"Pulling back to rising 20 SMA ({dist_from_20:.1f}%). 5 SMA still falling — not ready yet.",
                "trigger": f"BUY when 5 SMA flattens/turns up AND price holds above {_fmt2(sma20)}",
                "watch_for": f"5 SMA ({_fmt2(sma5)}) to stop falling. Declining volume on pullback = healthy. RVol now: {rvol}x",
                "invalidation": f"Close below {_fmt2(sma20)} or {_fmt2(round(sma20 - atr, 2))}",
                "gap_up_plan": f"If gaps above {_fmt2(sma10)}: do not chase, let it pull back to test",
                "gap_down_plan": f"If gaps to {_fmt2(sma20)} area on light volume: WATCH for hold — this could trigger entry",
                "partial_take_plan": f"Take 1/3 at {_fmt2(t1)} ({entry_zone.get('target_1_ref', '2R')}), 1/3 at {_fmt2(t2)}, trail rest",
                "max_chase_pct": 0,
                "time_horizon": "Wait 1-3 days for 5 SMA to flatten",
                "position_size_guidance": "half size" if has_event_risk else "standard",
                "upgrade_conditions": [
                    "Daily 5 SMA turns flat or rising",
                    f"Price holds above {_fmt2(sma20)} on 2 consecutive closes",
                    f"Low volume pullback (RVol < 0.7) at {_fmt2(sma20)} = institutional holding",
                ],
            }
        else:
            return {**base,
                "type": "pullback_to_ma",
                "description": f"Pullback to rising 20 SMA with rising short MAs. Prime Shannon setup.",
                "trigger": f"BUY at {_fmt2(ez_low)} - {_fmt2(ez_high)} on intraday strength",
                "watch_for": f"Intraday bounce off {_fmt2(sma20)} with session VWAP reclaim. Volume expanding on bounce.",
                "invalidation": f"Close below {_fmt2(sma20)}",
                "gap_up_plan": f"If gaps above {_fmt2(ez_high)}: buy half on open if within {round(0.5 * atr, 2)} of zone top",
                "gap_down_plan": f"If gaps into zone ({_fmt2(ez_low)}-{_fmt2(ez_high)}): strong entry if holds intraday",
                "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), 1/3 at {_fmt2(t1)}, trail last 1/3 with 10 SMA",
                "max_chase_pct": round(0.3 * atr / price * 100, 1) if price else 0.3,
                "time_horizon": "Enter today/tomorrow, hold 3-7 days",
                "position_size_guidance": "half size" if has_event_risk else "full",
            }

    # === PULLBACK TO 10 SMA in strong trend ===
    if (daily_state.get("sma10_above_sma20") and
        sma10_dir == "rising" and
        -1.5 <= dist_from_10 <= 0.5):
        return {**base,
            "type": "pullback_to_10sma",
            "description": f"Shallow pullback to rising 10 SMA. Strong trend — tight entry.",
            "trigger": f"BUY on intraday hold of {_fmt2(sma10)}",
            "watch_for": f"Price finding support at {_fmt2(sma10)} with session VWAP bounce",
            "invalidation": f"Close below {_fmt2(sma20)}",
            "gap_up_plan": f"If gaps above {_fmt2(sma10)}: acceptable entry if within 1%",
            "gap_down_plan": f"If gaps below {_fmt2(sma10)} to {_fmt2(sma20)}: watch for hold — deeper pullback entry",
            "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), trail rest with 10 SMA",
            "max_chase_pct": round(0.3 * atr / price * 100, 1) if price else 0.3,
            "time_horizon": "Enter today, hold 3-5 days",
            "position_size_guidance": "half size" if has_event_risk else "full",
        }

    # === RECLAIM: recovering key MA ===
    if (daily_state.get("close_above_sma_20") and
        dist_from_20 > 0 and dist_from_20 < 2 and
        daily_state.get("ma_stack") == "mixed"):
        return {**base,
            "type": "reclaim",
            "description": f"Reclaiming 20 SMA from below. Higher risk — needs confirmation.",
            "trigger": f"BUY if holds above {_fmt2(sma20)} for 2 consecutive closes",
            "watch_for": f"Volume increasing on reclaim. 5 SMA turning up. RVol now: {rvol}x",
            "invalidation": f"Close back below {_fmt2(sma20)} = failed reclaim",
            "gap_up_plan": f"If gaps above {_fmt2(sma10)}: half size only, this is unconfirmed",
            "gap_down_plan": f"If gaps below {_fmt2(sma20)}: reclaim failed, no entry",
            "partial_take_plan": f"Take 1/2 at R1 ({_fmt2(r1)}) — this is a lower-conviction setup",
            "max_chase_pct": 0,
            "time_horizon": "Wait 1-2 days for confirmation, then hold 3-5 days",
            "position_size_guidance": "half size — unconfirmed reclaim",
            "upgrade_conditions": [
                f"2 consecutive closes above {_fmt2(sma20)}",
                "5 SMA turns up",
                "RVol increases on up day",
            ],
        }

    # === ABOVE ZONE: good trend but chasing ===
    if price and sma10 and price > sma10 and dist_from_10 > 2:
        return {**base,
            "type": "above_zone_wait",
            "description": f"Trend is right but price {dist_from_10:.1f}% above 10 SMA. Don't chase.",
            "trigger": f"BUY on pullback to {_fmt2(sma10)} - {_fmt2(sma20)} zone",
            "watch_for": f"Light-volume pullback toward {_fmt2(sma10)}",
            "invalidation": f"10 SMA rolls over and starts falling",
            "gap_up_plan": "Do NOT buy — further from entry zone",
            "gap_down_plan": f"If gaps to {_fmt2(sma10)}: potential entry, watch for intraday hold",
            "max_chase_pct": 0,
            "time_horizon": "Wait 2-5 days for pullback",
            "upgrade_conditions": [
                f"Price pulls back to {_fmt2(sma10)} on declining volume",
                f"Breakout above {_fmt2(rh_price)} on volume > 1.3x avg",
            ],
        }

    # === DEFAULT: positive but no clean pattern ===
    if action_bias in ("buy", "lean_buy"):
        return {**base,
            "type": "watch",
            "description": "Trend positive but no clean entry pattern. Be patient.",
            "trigger": f"BUY on pullback to {_fmt2(sma20)} or breakout above {_fmt2(rh_price)}",
            "watch_for": "Wait for price to come to you",
            "invalidation": f"20 SMA turns down or close below {_fmt2(sma50)}",
            "gap_up_plan": "No action — let it develop",
            "gap_down_plan": f"If gaps to {_fmt2(sma20)}: watch for hold and potential entry",
            "max_chase_pct": 0,
            "time_horizon": "Wait for setup to develop",
            "upgrade_conditions": [
                f"Pullback to {_fmt2(sma20)} on light volume",
                f"Breakout above {_fmt2(rh_price)} on strong volume",
            ],
        }

    return {**base,
        "type": "no_setup",
        "description": "No actionable pattern",
        "trigger": None, "watch_for": None, "invalidation": None,
    }


def _fmt2(val):
    """Format a price value."""
    if val is None:
        return "?"
    try:
        return f"{float(val):.2f}"
    except (ValueError, TypeError):
        return str(val)


# =============================================================================
# ENTRY ZONE
# =============================================================================

def calc_entry_zone(daily_state: dict, pivots: dict = None) -> dict:
    """Calculate numeric entry zone, stop, and targets.
    Uses pivot levels for targets when they're above the entry zone.
    Uses pivot support levels for stop references."""
    price = daily_state.get("last_close", 0)
    atr = daily_state.get("atr", price * 0.02 if price else 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    sma50 = daily_state.get("sma_50", price)
    pivots = pivots or {}

    if not price:
        return {}

    # Ideal entry zone: between 10 and 20 SMA
    entry_low = round(min(sma10, sma20), 2)
    entry_high = round(max(sma10, sma20), 2)

    # If zone is tiny (< 0.5 ATR), widen it
    if entry_high - entry_low < 0.5 * atr:
        mid = (entry_high + entry_low) / 2
        entry_low = round(mid - 0.5 * atr, 2)
        entry_high = round(mid + 0.5 * atr, 2)

    # If price is already below zone, adjust
    if price < entry_low:
        entry_low = round(price - 0.3 * atr, 2)
        entry_high = round(price + 0.5 * atr, 2)

    # Stop: use pivot support if available and reasonable, else ATR-based
    atr_stop = round(entry_low - cfg.DEFAULT_ATR_STOP_MULT * atr, 2)
    s1 = pivots.get("s1")
    s2 = pivots.get("s2")
    # Use S1 as stop if it's within 1-2 ATR below entry, else use ATR-based
    if s1 and entry_low - 2 * atr < s1 < entry_low:
        stop = round(s1 - 0.10 * atr, 2)  # just below S1
        stop_ref = f"Below S1 ({_pct_dist(entry_low, s1)} below zone)"
    else:
        stop = atr_stop
        stop_ref = f"1.5 ATR below entry zone"

    # Risk calculated from MIDPOINT of entry zone to stop
    entry_mid = round((entry_low + entry_high) / 2, 2)
    risk_per_share = round(abs(entry_mid - stop), 2)

    # Targets: use R1/R2 if above entry and reasonable, else risk-based
    r1 = pivots.get("r1")
    r2 = pivots.get("r2")
    r3 = pivots.get("r3")

    # Target 1: use R1 if it gives at least 1.5:1 R:R, else use 2x risk
    risk_t1_default = round(entry_mid + 2.0 * risk_per_share, 2)
    if r1 and r1 > entry_high and (r1 - entry_mid) / risk_per_share >= 1.5:
        target_1 = round(r1, 2)
        t1_ref = f"R1 pivot"
    else:
        target_1 = risk_t1_default
        t1_ref = "2x risk"

    # Target 2: use R2 if available, else 3.5x risk
    risk_t2_default = round(entry_mid + 3.5 * risk_per_share, 2)
    if r2 and r2 > target_1:
        target_2 = round(r2, 2)
        t2_ref = f"R2 pivot"
    else:
        target_2 = risk_t2_default
        t2_ref = "3.5x risk"

    in_zone = entry_low <= price <= entry_high

    rr_t1 = round((target_1 - entry_mid) / risk_per_share, 1) if risk_per_share > 0 else 0
    rr_t2 = round((target_2 - entry_mid) / risk_per_share, 1) if risk_per_share > 0 else 0

    return {
        "price": round(price, 2),
        "entry_low": entry_low,
        "entry_high": entry_high,
        "entry_mid": entry_mid,
        "in_zone": in_zone,
        "stop": stop,
        "stop_ref": stop_ref,
        "target_1": target_1,
        "target_1_ref": t1_ref,
        "target_2": target_2,
        "target_2_ref": t2_ref,
        "risk_per_share": risk_per_share,
        "atr": round(atr, 2),
        "rr_t1": rr_t1,
        "rr_t2": rr_t2,
        "price_vs_zone": (
            "IN ZONE" if in_zone else
            f"ABOVE by {_pct_dist(price, entry_high)}" if price > entry_high else
            f"BELOW by {_pct_dist(entry_low, price)}"
        ),
        "pivots_used": {
            "r1": r1, "r2": r2, "r3": r3,
            "s1": s1, "s2": s2,
        },
    }


def _pct_dist(a, b):
    """Format percentage distance."""
    if b == 0:
        return "?"
    return f"{abs(a/b - 1)*100:.1f}%"
