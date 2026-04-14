"""
Pre-Trade Checklist Generator.

This is the most important output of the system.
Forces structured decision-making before every trade.
"""
from . import config as cfg


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
    conf = packet["confluence"]

    price = ez.get("price", 0)

    checks = []

    # 1. Regime
    regime_ok = regime.get("swing_bias") in ("long",) or \
                (regime.get("swing_bias") == "neutral" and
                 regime.get("risk_appetite") in ("moderate", "full"))
    checks.append({
        "item": "Regime",
        "value": f"{regime.get('regime', '?')} / bias: {regime.get('swing_bias', '?')}",
        "passed": regime_ok,
    })

    # 2. Weekly gate
    checks.append({
        "item": "Weekly Gate",
        "value": sc["weekly_gate"]["detail"],
        "passed": sc["weekly_gate"]["passed"],
    })

    # 3. Daily gate
    checks.append({
        "item": "Daily Gate",
        "value": sc["daily_gate"]["detail"],
        "passed": sc["daily_gate"]["passed"],
    })

    # 4. Score
    checks.append({
        "item": "Score",
        "value": f"{sc['score']}/100 ({sc['quality']})",
        "passed": sc["score"] >= 60,
    })

    # 5. Setup type
    checks.append({
        "item": "Setup",
        "value": f"{setup['type']} — {setup['description']}",
        "passed": setup["type"] not in ("no_setup", "extended"),
    })

    # 6. Entry zone
    in_zone = ez.get("in_zone", False)
    checks.append({
        "item": "Entry Zone",
        "value": f"{ez.get('entry_low')} — {ez.get('entry_high')} (price: {price})",
        "passed": in_zone,
    })

    # 7. Stop defined
    checks.append({
        "item": "Stop",
        "value": f"{ez.get('stop')}",
        "passed": ez.get("stop") is not None and ez.get("stop", 0) > 0,
    })

    # 8. R:R
    rr = ez.get("rr_t1", 0)
    checks.append({
        "item": "R:R to T1",
        "value": f"{rr}:1",
        "passed": rr >= 1.5,
    })

    # 9. Risk dollars
    checks.append({
        "item": "Risk",
        "value": f"${ps.get('risk_dollars', 0)} ({ps.get('risk_pct', 0)}% acct) — {ps.get('shares', 0)} shares",
        "passed": ps.get("risk_pct", 999) <= cfg.MAX_RISK_PCT,
    })

    # 10. Group exposure
    group_remaining = ps.get("group_risk_remaining", 999)
    checks.append({
        "item": "Group Exposure",
        "value": f"{ps.get('group', '?')} — ${ps.get('group_risk_used', 0)} used, ${group_remaining} remaining",
        "passed": group_remaining >= 0,
    })

    # 11. Event risk
    checks.append({
        "item": "Event Risk",
        "value": ev.get("recommendation", "?"),
        "passed": not ev.get("high_risk_imminent", False),
    })

    # 12. Earnings
    checks.append({
        "item": "Earnings",
        "value": earn.get("note", "?"),
        "passed": not earn.get("warning", False),
    })

    # 13. Relative strength
    rs20 = rs.get("rs_20d")
    checks.append({
        "item": "Relative Strength",
        "value": f"RS20: {rs20}" if rs20 is not None else "N/A",
        "passed": rs20 is not None and rs20 > -3,
    })

    # Overall verdict
    critical_checks = [c for c in checks if c["item"] in
                       ("Regime", "Weekly Gate", "Daily Gate", "Stop", "Event Risk")]
    all_critical_pass = all(c["passed"] for c in critical_checks)
    total_pass = sum(1 for c in checks if c["passed"])
    total = len(checks)

    if not all_critical_pass:
        verdict = "BLOCK — critical check failed"
    elif total_pass >= total - 1:
        verdict = f"PROCEED — {sc['quality']} setup in {regime.get('regime', '?')} regime"
    elif total_pass >= total - 3:
        verdict = f"CAUTION — {total - total_pass} checks failed, review before entry"
    else:
        verdict = "PASS — too many checks failed"

    return {
        "symbol": symbol,
        "checks": checks,
        "passed": total_pass,
        "total": total,
        "all_critical_pass": all_critical_pass,
        "verdict": verdict,
    }


def print_checklist(cl: dict) -> None:
    """Pretty-print a checklist."""
    symbol = cl["symbol"]
    print(f"\n{'='*55}")
    print(f"  PRE-TRADE CHECKLIST — {symbol}")
    print(f"{'='*55}")

    for c in cl["checks"]:
        icon = "PASS" if c["passed"] else "FAIL"
        print(f"  [{icon}] {c['item']}: {c['value']}")

    print(f"\n  Result: {cl['passed']}/{cl['total']} checks passed")
    print(f"  >>> {cl['verdict']}")
    print()
