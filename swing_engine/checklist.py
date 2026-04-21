"""
Deterministic checklist and actionability logic.
"""
from __future__ import annotations

from . import config as cfg


def _as_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes", "y")
    return bool(val)


def evaluate_actionability(packet: dict, checks: list | None = None) -> dict:
    score = packet.get("score", {})
    setup = packet.get("setup", {})
    dq = packet.get("data_quality", {})
    trigger = packet.get("intraday_trigger", {})
    entry_zone = packet.get("entry_zone", {})
    setup_state = score.get("setup_state", "FORMING")
    setup_family = score.get("setup_family", "none")
    structural = float(score.get("structural_score", 0) or 0)
    breakout = float(score.get("breakout_readiness_score", 0) or 0)
    trigger_score = float(score.get("trigger_readiness_score", 0) or 0)
    total = float(score.get("score", 0) or 0)
    freshness = str(dq.get("intraday_freshness_label", "missing"))

    if setup_state == "DATA_UNAVAILABLE" or freshness == "missing":
        return {"label": "DATA UNAVAILABLE", "detail": dq.get("detail", "Fresh data unavailable"), "rank": 8, "actionable_now": False}
    if setup_state in {"FAILED", "BLOCKED"} or structural < 45:
        return {"label": "BLOCK", "detail": "Structure, failure state, or risk gates block action", "rank": 7, "actionable_now": False}
    if setup_state == "EXTENDED":
        return {"label": "WAIT PULLBACK", "detail": score.get("decision_summary") or "Setup is too extended through the pivot to buy now", "rank": 6, "actionable_now": False}

    if setup_state == "ACTIONABLE_BREAKOUT":
        return {"label": "BUY BREAKOUT", "detail": setup.get("trigger") or "Breakout trigger is live now", "rank": 0, "actionable_now": True}
    if setup_state == "ACTIONABLE_RETEST":
        return {"label": "BUY RETEST", "detail": setup.get("trigger") or "Retest is holding and actionable", "rank": 0, "actionable_now": True}
    if setup_state == "ACTIONABLE_RECLAIM":
        return {"label": "BUY NOW", "detail": setup.get("trigger") or "Reclaim-and-go trigger is live now", "rank": 0, "actionable_now": True}

    if setup_state == "TRIGGER_WATCH":
        return {"label": "WATCH TRIGGER", "detail": setup.get("trigger") or "Setup is close to a live trigger", "rank": 1, "actionable_now": False}
    if setup_state == "POTENTIAL_BREAKOUT":
        return {"label": "WATCH CONTINUATION", "detail": setup.get("description") or score.get("decision_summary") or "Constructive early breakout candidate", "rank": 2, "actionable_now": False}

    if entry_zone and not _as_bool(entry_zone.get("in_zone")) and setup_family in {"breakout_retest", "reclaim_and_go"}:
        return {"label": "WAIT PULLBACK", "detail": entry_zone.get("price_vs_zone") or "Wait for price to pull back into support", "rank": 4, "actionable_now": False}
    return {"label": "WAIT FOR TIGHTENING", "detail": setup.get("description") or "Needs more contraction or pivot pressure", "rank": 5, "actionable_now": False}


def generate_checklist(packet: dict, regime: dict) -> dict:
    score = packet.get("score", {})
    dq = packet.get("data_quality", {})
    setup = packet.get("setup", {})
    entry_zone = packet.get("entry_zone", {})
    position = packet.get("position_sizing", {})
    context_quality = packet.get("context_quality", {})
    actionability = evaluate_actionability(packet)
    checks = [
        {"item": "Regime", "value": f"{regime.get('regime', '?')} / bias {regime.get('swing_bias', '?')}", "passed": regime.get("swing_bias") in {"long", "neutral"}},
        {"item": "Benchmark Context", "value": f"{context_quality.get('benchmark_status', '--')} / regime {context_quality.get('regime_quality', '--')}", "passed": context_quality.get("benchmark_status") != "unavailable"},
        {"item": "Weekly Gate", "value": score.get("weekly_gate", {}).get("detail", "--"), "passed": bool(score.get("weekly_gate", {}).get("passed"))},
        {"item": "Daily Gate", "value": score.get("daily_gate", {}).get("detail", "--"), "passed": bool(score.get("daily_gate", {}).get("passed"))},
        {"item": "Structural", "value": f"{score.get('structural_score', '--')}/100", "passed": float(score.get("structural_score", 0) or 0) >= cfg.STRUCTURAL_MIN_SCORE},
        {"item": "Breakout Readiness", "value": f"{score.get('breakout_readiness_score', '--')}/100", "passed": float(score.get("breakout_readiness_score", 0) or 0) >= cfg.BREAKOUT_WATCH_MIN_SCORE},
        {"item": "Trigger Readiness", "value": f"{score.get('trigger_readiness_score', '--')}/100", "passed": float(score.get("trigger_readiness_score", 0) or 0) >= 55},
        {"item": "Setup", "value": f"{setup.get('setup_family', '--')} / {setup.get('state', '--')}", "passed": setup.get("state") not in {"FAILED", "BLOCKED", "DATA_UNAVAILABLE"}},
        {"item": "Pivot / Zone", "value": f"Pivot {setup.get('pivot_level', '--')} | {entry_zone.get('price_vs_zone', '--')}", "passed": setup.get("pivot_level") is not None},
        {"item": "Data Quality", "value": f"{dq.get('score', '--')}/100 ({dq.get('intraday_freshness_label', '--')})", "passed": float(dq.get("score", 0) or 0) >= 45},
        {"item": "Trigger Freshness", "value": f"{dq.get('intraday_freshness_minutes', '--')} min old", "passed": dq.get("intraday_freshness_label") in {"fresh", "mildly_stale"}},
        {"item": "Risk/Reward", "value": f"T1 {entry_zone.get('rr_t1', '--')}:1 | stop {entry_zone.get('stop', '--')}", "passed": float(entry_zone.get("rr_t1", 0) or 0) >= 1.4},
        {"item": "Liquidity", "value": f"{position.get('liquidity_status', '--')} | shares {position.get('shares', '--')}", "passed": position.get("liquidity_status") != "blocked"},
    ]
    passed = sum(1 for item in checks if item["passed"])
    verdict = f"{actionability['label']} - {actionability['detail']}"
    return {
        "symbol": packet.get("symbol", "?"),
        "checks": checks,
        "passed": passed,
        "total": len(checks),
        "all_critical_pass": all(item["passed"] for item in checks[:5] + checks[9:11]),
        "verdict": verdict,
        "actionability": actionability,
    }


def print_checklist(cl: dict) -> None:
    print(f"\n{'='*55}")
    print(f"  PRE-TRADE CHECKLIST - {cl['symbol']}")
    print(f"{'='*55}")
    for item in cl["checks"]:
        icon = "PASS" if item["passed"] else "FAIL"
        print(f"  [{icon}] {item['item']}: {item['value']}")
    print(f"\n  Result: {cl['passed']}/{cl['total']} checks passed")
    print(f"  >>> {cl['verdict']}\n")
