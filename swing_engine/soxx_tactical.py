"""
SOXX → SOXL Tactical Module.

Analyzes SOXX (unleveraged chart) to make execution decisions
on SOXL (3x daily leveraged ETF).

Rules:
- SOXX is the chart, SOXL is the vehicle
- Half normal position size due to 3x leverage
- Max hold: 5-10 trading days
- Never hold through FOMC, CPI, or major semi earnings
- Traffic light system: green / yellow / red
"""
from datetime import date
from typing import Optional

from . import config as cfg
from . import data as mdata
from . import features as feat
from . import scoring
from . import events
from . import sizing
from . import packets


def build_soxx_packet(force: bool = False,
                      spy_daily=None) -> dict:
    """Build SOXX packet with tactical anchors."""
    soxx_data = mdata.load_all("SOXX", force=force)

    if spy_daily is None:
        try:
            spy_data = mdata.load_all("SPY", force=force)
            spy_daily = spy_data.get("daily")
        except Exception:
            spy_daily = None

    # Temporarily override SOXX anchors with tactical set
    original = cfg.MACRO_ANCHORS.get("SOXX", {}).copy()
    cfg.MACRO_ANCHORS["SOXX"] = cfg.SOXX_TACTICAL_ANCHORS

    packet = packets.build_packet("SOXX", soxx_data, spy_daily)

    # Restore
    cfg.MACRO_ANCHORS["SOXX"] = original

    return packet


def build_ma_layer(packet: dict) -> dict:
    """
    Build detailed MA state layer for SOXX tactical analysis.
    All the MA relationships the LLM would have interpreted,
    computed deterministically.
    """
    d = packet.get("daily", {})
    w = packet.get("weekly", {})
    i = packet.get("intraday", {})

    layer = {
        "daily": {
            "above_sma5":  d.get("close_above_sma_5"),
            "above_sma10": d.get("close_above_sma_10"),
            "above_sma20": d.get("close_above_sma_20"),
            "above_sma50": d.get("close_above_sma_50"),
            "above_sma200": d.get("close_above_sma_200"),
            "sma10_above_20": d.get("sma10_above_sma20"),
            "sma20_above_50": d.get("sma20_above_sma50"),
            "dist_10_pct": d.get("dist_from_sma_10_pct"),
            "dist_20_pct": d.get("dist_from_sma_20_pct"),
            "dist_50_pct": d.get("dist_from_sma_50_pct"),
            "stack": d.get("ma_stack"),
        },
        "weekly": {
            "above_sma5":  w.get("close_above_sma_5"),
            "above_sma10": w.get("close_above_sma_10"),
            "above_sma20": w.get("close_above_sma_20"),
            "sma5_above_10": w.get("sma5_above_sma10"),
            "sma10_above_20": w.get("sma10_above_sma20"),
            "dist_10_pct": w.get("dist_from_sma_10_pct"),
            "dist_20_pct": w.get("dist_from_sma_20_pct"),
            "stack": w.get("ma_stack"),
        },
        "intraday": {
            "above_sma10": i.get("close_above_sma_10"),
            "above_sma20": i.get("close_above_sma_20"),
            "above_sma50": i.get("close_above_sma_50"),
            "stack": i.get("ma_stack"),
        },
    }

    # Alignment summary
    stacks = [d.get("ma_stack", "?"), w.get("ma_stack", "?"), i.get("ma_stack", "?")]
    bull = sum(1 for s in stacks if s == "bullish")
    bear = sum(1 for s in stacks if s == "bearish")

    if bull == 3:
        alignment = "FULL_BULLISH"
    elif bull >= 2:
        alignment = "MOSTLY_BULLISH"
    elif bear == 3:
        alignment = "FULL_BEARISH"
    elif bear >= 2:
        alignment = "MOSTLY_BEARISH"
    else:
        alignment = "MIXED"

    layer["alignment"] = alignment
    return layer


def traffic_light(packet: dict, ma_layer: dict) -> dict:
    """
    SOXX→SOXL traffic light decision.

    GREEN: Buy SOXL — conditions met
    YELLOW: Wait — close but not yet
    RED: No position

    Returns structured decision with execution details.
    """
    score = packet["score"]["score"]
    action_bias = packet["score"]["action_bias"]
    alignment = ma_layer["alignment"]
    event_ctx = packet["events"]
    entry_zone = packet["entry_zone"]
    price = packet["daily"].get("last_close", 0)

    # Hard blocks (RED regardless)
    if event_ctx.get("high_risk_imminent"):
        # But if the setup is otherwise good, show YELLOW with detail instead of flat RED
        if score >= 60 and alignment not in ("FULL_BEARISH", "MOSTLY_BEARISH"):
            upcoming = event_ctx.get("upcoming_events", [])
            event_names = [e["event"] + f" ({e['date']})" for e in upcoming if e["severity"] == "high"]
            return _yellow(
                f"Setup is good (score {score}, {alignment}) but blocked by: {', '.join(event_names)}. "
                f"ACTIONABLE after event passes if SOXX holds above {entry_zone.get('stop', '?')}.",
                packet
            )
        return _red("Macro event imminent — no leveraged positions", packet)

    if alignment in ("FULL_BEARISH", "MOSTLY_BEARISH"):
        return _red(f"MA alignment {alignment} — not suitable for leveraged long", packet)

    if score < 45:
        return _red(f"Score too low ({score}) for leveraged trade", packet)

    # GREEN conditions
    if (score >= 65 and
        alignment in ("FULL_BULLISH", "MOSTLY_BULLISH") and
        entry_zone.get("in_zone", False) and
        not event_ctx.get("elevated_risk")):

        return _green(packet, ma_layer)

    # Partial conditions — YELLOW
    if score >= 55 and alignment in ("FULL_BULLISH", "MOSTLY_BULLISH"):
        reasons = []
        if not entry_zone.get("in_zone"):
            reasons.append(f"Price {price} not in zone {entry_zone.get('entry_low')}-{entry_zone.get('entry_high')}")
        if event_ctx.get("elevated_risk"):
            reasons.append("Elevated event risk — reduce or wait")
        if score < 65:
            reasons.append(f"Score {score} — prefer 65+")
        return _yellow("; ".join(reasons) if reasons else "Close but conditions not fully met", packet)

    return _red(f"Insufficient conditions (score={score}, align={alignment})", packet)


def _green(packet: dict, ma_layer: dict) -> dict:
    """Build GREEN signal with execution protocol."""
    ez = packet["entry_zone"]
    price = packet["daily"]["last_close"]
    atr = packet["daily"].get("atr", price * 0.02)
    score = packet["score"]["score"]

    # SOXX stop: below entry zone - 1.5 ATR
    soxx_stop = ez["stop"]
    soxx_t1 = ez["target_1"]
    soxx_t2 = ez["target_2"]

    # SOXL approximate moves (3x for short holds)
    soxx_risk_pct = abs(price - soxx_stop) / price * 100
    soxl_risk_pct = round(soxx_risk_pct * 3, 1)
    soxx_t1_pct = (soxx_t1 / price - 1) * 100
    soxx_t2_pct = (soxx_t2 / price - 1) * 100

    # Position sizing at half normal (leveraged)
    pos = sizing.calc_position_size(
        price, soxx_stop, symbol="SOXL",
        leverage=3.0,
    )

    # Determine tactical size
    if score >= 80:
        tac_size = "half_50pct"
    elif score >= 70:
        tac_size = "starter_25pct"
    else:
        tac_size = "starter_25pct"

    return {
        "signal": "GREEN",
        "action": "BUY SOXL",
        "reason": f"SOXX score {score}, {ma_layer['alignment']}, price in zone",
        "soxx_price": price,
        "soxx_stop": soxx_stop,
        "soxx_target_1": soxx_t1,
        "soxx_target_2": soxx_t2,
        "soxl_approx_risk_pct": soxl_risk_pct,
        "soxl_approx_t1_pct": round(soxx_t1_pct * 3, 1),
        "soxl_approx_t2_pct": round(soxx_t2_pct * 3, 1),
        "execution": {
            "tactical_size": tac_size,
            "shares": pos["shares"],
            "risk_dollars": pos["risk_dollars"],
            "risk_pct_account": pos["risk_pct"],
            "max_hold_days": 7,
            "stop_protocol": f"Hard stop if SOXX closes below {soxx_stop}",
            "partial_take": f"Take half at SOXX {soxx_t1}",
            "do_not_hold_through": _get_upcoming_events(packet),
        },
    }


def _yellow(reason: str, packet: dict) -> dict:
    """Build YELLOW signal."""
    ez = packet["entry_zone"]
    return {
        "signal": "YELLOW",
        "action": "WAIT",
        "reason": reason,
        "soxx_price": packet["daily"].get("last_close"),
        "watch_for": {
            "entry_zone": f"{ez.get('entry_low')} — {ez.get('entry_high')}",
            "would_turn_green": "Price enters zone + score 65+ + no event risk",
        },
    }


def _red(reason: str, packet: dict) -> dict:
    """Build RED signal."""
    return {
        "signal": "RED",
        "action": "NO POSITION",
        "reason": reason,
        "soxx_price": packet["daily"].get("last_close"),
    }


def _get_upcoming_events(packet: dict) -> list:
    """Get events to avoid holding through."""
    evts = packet["events"].get("upcoming_events", [])
    return [f"{e['event']} ({e['date']})" for e in evts if e["severity"] == "high"]


def estimate_soxl_payoff(soxx_price: float, soxx_stop: float,
                          soxx_t1: float, soxx_t2: float) -> dict:
    """Approximate SOXL payoff from SOXX move scenarios."""
    if soxx_price <= 0:
        return {}

    def pct(target):
        return round((target / soxx_price - 1) * 100, 2)

    stop_pct = pct(soxx_stop)
    t1_pct = pct(soxx_t1)
    t2_pct = pct(soxx_t2)

    return {
        "stop":     {"soxx_pct": stop_pct, "soxl_approx_pct": round(stop_pct * 3, 1)},
        "target_1": {"soxx_pct": t1_pct,   "soxl_approx_pct": round(t1_pct * 3, 1)},
        "target_2": {"soxx_pct": t2_pct,   "soxl_approx_pct": round(t2_pct * 3, 1)},
        "rr_t1": round(abs(t1_pct / stop_pct), 1) if stop_pct else 0,
        "rr_t2": round(abs(t2_pct / stop_pct), 1) if stop_pct else 0,
    }


def run_tactical(force: bool = False) -> dict:
    """
    Full SOXX→SOXL tactical pipeline.
    Returns structured result dict.
    """
    print("=" * 60)
    print("SOXX → SOXL TACTICAL ANALYSIS")
    print("=" * 60)

    packet = build_soxx_packet(force=force)
    ma_layer = build_ma_layer(packet)
    decision = traffic_light(packet, ma_layer)

    # Payoff estimate if green
    if decision["signal"] == "GREEN":
        payoff = estimate_soxl_payoff(
            decision["soxx_price"], decision["soxx_stop"],
            decision["soxx_target_1"], decision["soxx_target_2"],
        )
        decision["payoff_estimate"] = payoff

    print(f"\n  SOXX Price:  {packet['daily'].get('last_close', '?')}")
    print(f"  Score:       {packet['score']['score']}/100 ({packet['score']['quality']})")
    print(f"  MA Align:    {ma_layer['alignment']}")
    print(f"  Signal:      {decision['signal']}")
    print(f"  Action:      {decision['action']}")
    print(f"  Reason:      {decision['reason']}")

    if decision["signal"] == "GREEN":
        ex = decision["execution"]
        print(f"\n  EXECUTION PROTOCOL:")
        print(f"    Size:       {ex['tactical_size']} ({ex['shares']} shares)")
        print(f"    Risk:       ${ex['risk_dollars']} ({ex['risk_pct_account']}% acct)")
        print(f"    Max Hold:   {ex['max_hold_days']} days")
        print(f"    Stop:       {ex['stop_protocol']}")
        print(f"    Partial:    {ex['partial_take']}")
        dnht = ex.get("do_not_hold_through", [])
        if dnht:
            print(f"    DO NOT HOLD: {', '.join(dnht)}")

        pf = decision.get("payoff_estimate", {})
        if pf:
            print(f"\n  PAYOFF ESTIMATE:")
            for k, v in pf.items():
                if isinstance(v, dict):
                    print(f"    {k}: SOXX {v['soxx_pct']}% → SOXL ~{v['soxl_approx_pct']}%")

    elif decision["signal"] == "YELLOW":
        wf = decision.get("watch_for", {})
        print(f"\n  WATCH FOR:")
        print(f"    Entry Zone: {wf.get('entry_zone')}")
        print(f"    Turn Green: {wf.get('would_turn_green')}")

    return {
        "packet": packet,
        "ma_layer": ma_layer,
        "decision": decision,
    }
