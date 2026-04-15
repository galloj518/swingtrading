"""
Pre-trade checklist generator.

This is the most important output of the system.
Forces structured decision-making before every trade.
"""
from . import config as cfg


def _as_bool(val) -> bool:
    """Normalize booleans that may arrive as strings from saved reports."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes", "y")
    return bool(val)


def evaluate_actionability(packet: dict, checks: list | None = None) -> dict:
    """
    Label whether a setup is actionable now, a watch item, or blocked.

    Lower rank means higher urgency/actionability and is used by the dashboard.
    """
    sc = packet.get("score", {})
    setup = packet.get("setup", {})
    ez = packet.get("entry_zone", {})
    action_bias = sc.get("action_bias", "")
    setup_type = setup.get("type", "no_setup")
    in_zone = _as_bool(ez.get("in_zone", False))
    data_quality = packet.get("data_quality", {})
    data_quality_score = float(data_quality.get("score", 0) or 0)
    idea_score = float(sc.get("idea_quality_score", sc.get("score", 0)) or 0)
    timing_score = float(sc.get("entry_timing_score", sc.get("score", 0)) or 0)
    confidence_score = float(sc.get("confidence_score", sc.get("confidence_adjusted_score", sc.get("score", 0))) or 0)
    tradeability = sc.get("tradeability", {})
    tradeability_score = float(tradeability.get("score", sc.get("score", 0)) or 0)
    idea_factors = sc.get("idea_factors", {}) or {}
    timing_factors = sc.get("timing_factors", {}) or {}
    group_score = float(idea_factors.get("group_strength", 55) or 55)
    rs_score = float(idea_factors.get("relative_strength", 50) or 50)
    continuation_score = float(idea_factors.get("continuation_pattern", 55) or 55)
    sponsorship_score = float(idea_factors.get("institutional_sponsorship", 55) or 55)
    short_term_posture = float(timing_factors.get("short_term_posture", timing_score) or timing_score)

    failed_items = {
        c["item"] for c in (checks or []) if not c.get("passed", False)
    }
    critical_failed = bool(
        {"Regime", "Weekly Gate", "Daily Gate", "Stop", "Event Risk"} & failed_items
    )
    timing_failed = "Entry Timing" in failed_items

    if action_bias == "unavailable" or setup_type == "data_unavailable" or data_quality_score <= 15:
        return {
            "label": "DATA UNAVAILABLE",
            "detail": data_quality.get("detail") or "Fresh market data is not available yet",
            "rank": 6,
            "actionable_now": False,
        }

    if critical_failed or action_bias == "avoid" or setup_type == "no_setup" or idea_score < 50:
        return {
            "label": "BLOCK",
            "detail": "Idea quality, trend, or risk gates failed",
            "rank": 5,
            "actionable_now": False,
        }

    institutional_ready = (
        confidence_score >= 55 and
        data_quality_score >= 60 and
        group_score >= 45 and
        rs_score >= 45
    )

    if setup_type == "tight_continuation":
        if (
            tradeability_score >= 82 and
            idea_score >= 60 and
            timing_score >= 72 and
            short_term_posture >= 72 and
            continuation_score >= 70 and
            sponsorship_score >= 62 and
            institutional_ready
        ):
            return {
                "label": "BUY NOW",
                "detail": setup.get("trigger") or "Tight continuation is actionable now",
                "rank": 0,
                "actionable_now": True,
            }
        if tradeability_score >= 58 and continuation_score >= 68 and short_term_posture >= 68:
            return {
                "label": "WATCH CONTINUATION",
                "detail": "Strong continuation candidate, but still needs better confirmation",
                "rank": 1,
                "actionable_now": False,
            }
        return {
            "label": "WAIT SETUP",
            "detail": setup.get("trigger") or "Continuation pattern is close but not ready",
            "rank": 3,
            "actionable_now": False,
        }

    if setup_type == "breakout":
        return {
            "label": "WATCH BREAKOUT",
            "detail": setup.get("trigger") or "Needs breakout confirmation",
            "rank": 1,
            "actionable_now": False,
        }

    if setup_type in ("reclaim", "pullback_developing", "watch", "below_10dma_wait"):
        return {
            "label": "WAIT SETUP",
            "detail": setup.get("trigger") or "Pattern is still developing",
            "rank": 3,
            "actionable_now": False,
        }

    if setup_type in ("extended_wait", "above_zone_wait", "below_5dma_wait"):
        if (
            setup_type in ("extended_wait", "above_zone_wait") and
            tradeability_score >= 52 and
            continuation_score >= 70 and
            sponsorship_score >= 60 and
            short_term_posture >= 72 and
            institutional_ready and
            rs_score >= 55
        ):
            return {
                "label": "WATCH CONTINUATION",
                "detail": "Continuation is strong, but price is still extended above value",
                "rank": 1,
                "actionable_now": False,
            }
        return {
            "label": "WAIT PULLBACK",
            "detail": setup.get("trigger") or "Price is extended above the zone",
            "rank": 2,
            "actionable_now": False,
        }

    if timing_failed or not institutional_ready:
        return {
            "label": "WAIT SETUP",
            "detail": "Entry timing or confirmation quality is not ready yet",
            "rank": 3,
            "actionable_now": False,
        }

    if not in_zone:
        return {
            "label": "WAIT ZONE",
            "detail": ez.get("price_vs_zone") or "Price is outside the entry zone",
            "rank": 2,
            "actionable_now": False,
        }

    return {
        "label": "BUY NOW",
        "detail": setup.get("trigger") or "Inside valid entry zone",
        "rank": 0,
        "actionable_now": True,
    }


def generate_checklist(packet: dict, regime: dict) -> dict:
    """
    Generate a pre-trade checklist from a symbol packet and regime.

    Returns structured checklist with pass/fail for each gate.
    """
    symbol = packet["symbol"]
    sc = packet["score"]
    ez = packet["entry_zone"]
    ev = packet["events"]
    earn = packet["earnings"]
    setup = packet["setup"]
    ps = packet["position_sizing"]
    rs = packet["relative_strength"]
    tradeability = sc.get("tradeability", {})
    tradeability_score = float(tradeability.get("score", sc.get("score", 0)) or 0)

    price = ez.get("price", 0)

    checks = []

    regime_ok = regime.get("swing_bias") in ("long",) or (
        regime.get("swing_bias") == "neutral" and
        regime.get("risk_appetite") in ("moderate", "full")
    )
    checks.append({
        "item": "Regime",
        "value": f"{regime.get('regime', '?')} / bias: {regime.get('swing_bias', '?')}",
        "passed": regime_ok,
    })

    checks.append({
        "item": "Weekly Gate",
        "value": sc["weekly_gate"]["detail"],
        "passed": sc["weekly_gate"]["passed"],
    })

    checks.append({
        "item": "Daily Gate",
        "value": sc["daily_gate"]["detail"],
        "passed": sc["daily_gate"]["passed"],
    })

    checks.append({
        "item": "Score",
        "value": (
            f"Tradeability {tradeability_score}/100 ({tradeability.get('label', '--')}) | "
            f"Composite {sc['score']}/100 ({sc['quality']}) | "
            f"Idea {sc.get('idea_quality_score', sc['score'])}/100 ({sc.get('idea_quality', sc['quality'])}) | "
            f"Timing {sc.get('entry_timing_score', sc['score'])}/100 ({sc.get('entry_timing', sc['quality'])})"
        ),
        "passed": tradeability_score >= 55,
    })

    checks.append({
        "item": "Idea Quality",
        "value": f"{sc.get('idea_quality_score', sc['score'])}/100 ({sc.get('idea_quality', sc['quality'])})",
        "passed": sc.get("idea_quality_score", sc["score"]) >= 55,
    })

    checks.append({
        "item": "Entry Timing",
        "value": f"{sc.get('entry_timing_score', sc['score'])}/100 ({sc.get('entry_timing', sc['quality'])})",
        "passed": sc.get("entry_timing_score", sc["score"]) >= 55,
    })

    checks.append({
        "item": "Setup",
        "value": f"{setup['type']} - {setup['description']}",
        "passed": setup["type"] not in (
            "no_setup", "extended", "extended_wait", "above_zone_wait", "watch",
            "below_5dma_wait", "below_10dma_wait"
        ),
    })

    in_zone = _as_bool(ez.get("in_zone", False))
    checks.append({
        "item": "Entry Zone",
        "value": f"{ez.get('entry_low')} - {ez.get('entry_high')} (price: {price})",
        "passed": in_zone,
    })

    checks.append({
        "item": "Stop",
        "value": f"{ez.get('stop')}",
        "passed": ez.get("stop") is not None and ez.get("stop", 0) > 0,
    })

    rr = ez.get("rr_t1", 0)
    checks.append({
        "item": "R:R to T1",
        "value": f"{rr}:1",
        "passed": rr >= 1.5,
    })

    checks.append({
        "item": "Risk",
        "value": f"${ps.get('risk_dollars', 0)} ({ps.get('risk_pct', 0)}% acct) - {ps.get('shares', 0)} shares",
        "passed": ps.get("risk_pct", 999) <= cfg.MAX_RISK_PCT,
    })

    group_remaining = ps.get("group_risk_remaining", 999)
    checks.append({
        "item": "Group Exposure",
        "value": f"{ps.get('group', '?')} - ${ps.get('group_risk_used', 0)} used, ${group_remaining} remaining",
        "passed": group_remaining >= 0,
    })

    liquidity_status = ps.get("liquidity_status", "ok")
    avg_vol = ps.get("avg_volume", 0)
    avg_dollar_vol = ps.get("avg_dollar_volume", 0)
    rvol = ps.get("rvol", 0)
    checks.append({
        "item": "Liquidity",
        "value": (
            f"ADV: {avg_vol:,.0f} | $ADV: ${avg_dollar_vol:,.0f} | "
            f"RVol: {rvol} | status: {liquidity_status}"
        ),
        "passed": liquidity_status != "blocked",
    })

    dq = packet.get("data_quality", {})
    checks.append({
        "item": "Data Quality",
        "value": f"{dq.get('score', '--')}/100 ({dq.get('label', '--')}) - {dq.get('detail', '--')}",
        "passed": float(dq.get("score", 0) or 0) >= 60,
    })

    checks.append({
        "item": "Event Risk",
        "value": ev.get("recommendation", "?"),
        "passed": not ev.get("high_risk_imminent", False),
    })

    checks.append({
        "item": "Earnings",
        "value": earn.get("note", "?"),
        "passed": not earn.get("warning", False),
    })

    rs20 = rs.get("rs_20d")
    checks.append({
        "item": "Relative Strength",
        "value": f"RS20: {rs20}" if rs20 is not None else "N/A",
        "passed": rs20 is not None and rs20 > -3,
    })

    critical_checks = [
        c for c in checks
        if c["item"] in ("Regime", "Weekly Gate", "Daily Gate", "Stop", "Event Risk", "Entry Zone", "Liquidity", "Data Quality")
    ]
    all_critical_pass = all(c["passed"] for c in critical_checks)
    total_pass = sum(1 for c in checks if c["passed"])
    total = len(checks)
    actionability = evaluate_actionability(packet, checks)

    if actionability["label"] == "BLOCK":
        verdict = "BLOCK - critical check failed"
    elif actionability["label"] == "DATA UNAVAILABLE":
        verdict = f"DATA UNAVAILABLE - {actionability['detail']}"
    elif actionability["actionable_now"] and total_pass == total:
        verdict = f"BUY NOW - {sc['quality']} setup in {regime.get('regime', '?')} regime"
    elif actionability["label"].startswith("WATCH"):
        verdict = f"{actionability['label']} - {actionability['detail']}"
    elif actionability["label"].startswith("WAIT"):
        verdict = f"{actionability['label']} - {actionability['detail']}"
    elif total_pass >= total - 3:
        verdict = f"CAUTION - {total - total_pass} checks failed, review before entry"
    else:
        verdict = "BLOCK - too many checks failed"

    return {
        "symbol": symbol,
        "checks": checks,
        "passed": total_pass,
        "total": total,
        "all_critical_pass": all_critical_pass,
        "verdict": verdict,
        "actionability": actionability,
    }


def print_checklist(cl: dict) -> None:
    """Pretty-print a checklist."""
    symbol = cl["symbol"]
    print(f"\n{'='*55}")
    print(f"  PRE-TRADE CHECKLIST - {symbol}")
    print(f"{'='*55}")

    for c in cl["checks"]:
        icon = "PASS" if c["passed"] else "FAIL"
        print(f"  [{icon}] {c['item']}: {c['value']}")

    print(f"\n  Result: {cl['passed']}/{cl['total']} checks passed")
    print(f"  >>> {cl['verdict']}")
    print()
