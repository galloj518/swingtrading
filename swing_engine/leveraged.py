"""
Leveraged Benchmark Tactical Module.

Analyzes SPY, QQQ, SOXX as the underlying charts and produces
trade plans for leveraged vehicles in both directions:
  Long:  SPXL, TQQQ, SOXL (3x bull)
  Short: SPXS, SQQQ, SOXS (3x bear)

Rules for 3x leveraged ETFs:
- Half normal position size (3x leverage = 3x risk)
- Max hold: 5-10 trading days (decay)
- Never hold through FOMC, CPI, or major events
- Strict stops — 3% underlying move = ~9% leveraged move
- Both directions analyzed: long setup AND short setup
"""
from . import config as cfg
from . import data as mdata
from . import features as feat
from . import scoring
from . import events
from . import sizing
from . import packets


def _traffic_light(score, action_bias, ma_alignment, event_risk, in_zone, direction="long"):
    """
    Determine GREEN/YELLOW/RED for a leveraged trade.
    direction: 'long' or 'short'
    """
    has_event = event_risk.get("high_risk_imminent")

    if has_event:
        # If setup is otherwise decent, show YELLOW with context instead of flat RED
        if direction == "long" and score >= 55 and ma_alignment in ("bullish", "mixed"):
            upcoming = event_risk.get("upcoming_events", [])
            events = [e["event"] for e in upcoming if e.get("severity") == "high"] if upcoming else ["macro event"]
            return "YELLOW", f"Score {score}, {ma_alignment} — BLOCKED by {', '.join(events)}. Actionable after event."
        elif direction == "short" and score <= 40 and ma_alignment in ("bearish", "mixed"):
            return "YELLOW", f"Score {score}, {ma_alignment} — short setup blocked by event risk"
        return "RED", "Macro event imminent — no leveraged positions"

    if direction == "long":
        if score >= 65 and ma_alignment in ("bullish",) and in_zone and not event_risk.get("elevated_risk"):
            return "GREEN", f"Score {score}, MAs aligned bullish, price in zone"
        if score >= 50 and ma_alignment in ("bullish", "mixed"):
            return "YELLOW", f"Score {score}, waiting for better entry or MA confirmation"
        return "RED", f"Score {score}, alignment {ma_alignment} — not suitable for leveraged long"

    else:  # short
        if score <= 35 and ma_alignment in ("bearish",):
            return "GREEN", f"Score {score}, MAs aligned bearish — short setup"
        if score <= 45 and ma_alignment in ("bearish", "mixed"):
            return "YELLOW", f"Score {score}, bearish but waiting for confirmation"
        return "RED", f"Score {score}, alignment {ma_alignment} — not suitable for leveraged short"


def _estimate_leveraged_payoff(price, stop, t1, t2, leverage=3.0):
    """Estimate leveraged ETF moves from underlying moves."""
    if not price or price <= 0:
        return {}

    def pct(target):
        return round((target / price - 1) * 100, 2)

    stop_pct = pct(stop)
    t1_pct = pct(t1)
    t2_pct = pct(t2)

    return {
        "stop": {"underlying_pct": stop_pct, "leveraged_pct": round(stop_pct * leverage, 1)},
        "target_1": {"underlying_pct": t1_pct, "leveraged_pct": round(t1_pct * leverage, 1)},
        "target_2": {"underlying_pct": t2_pct, "leveraged_pct": round(t2_pct * leverage, 1)},
    }


def analyze_benchmark_tactical(chart_symbol, packet, regime, force=False):
    """
    Analyze one benchmark for leveraged tactical trades in both directions.

    Args:
        chart_symbol: 'SPY', 'QQQ', or 'SOXX'
        packet: pre-built packet for the chart symbol
        regime: regime dict

    Returns:
        Dict with long and short analysis
    """
    pair = cfg.LEVERAGED_PAIRS.get(chart_symbol)
    if not pair:
        return {}

    d = packet.get("daily", {})
    w = packet.get("weekly", {})
    ez = packet.get("entry_zone", {})
    sc = packet.get("score", {})
    setup = packet.get("setup", {})
    pivots = packet.get("pivots", {})
    avwaps = packet.get("avwap_map", {})
    event_ctx = packet.get("events", {})

    score = sc.get("score", 50)
    price = d.get("last_close", 0)
    atr = d.get("atr", price * 0.02 if price else 1)
    ma_stack = d.get("ma_stack", "mixed")
    in_zone = ez.get("in_zone", False)

    # --- LONG analysis ---
    long_signal, long_reason = _traffic_light(
        score, sc.get("action_bias"), ma_stack, event_ctx, in_zone, "long"
    )
    long_stop = ez.get("stop", round(price - 2 * atr, 2))
    long_t1 = ez.get("target_1", round(price + 2 * atr, 2))
    long_t2 = ez.get("target_2", round(price + 3.5 * atr, 2))
    long_payoff = _estimate_leveraged_payoff(price, long_stop, long_t1, long_t2, pair["leverage"])

    long_size = sizing.calc_position_size(
        price, long_stop, symbol=pair["long"], leverage=pair["leverage"]
    )

    # --- SHORT analysis (inverted logic) ---
    # For shorts: resistance becomes entry, support becomes target
    short_entry = pivots.get("r1", round(price + atr, 2))
    short_stop = pivots.get("r2", round(price + 2 * atr, 2))
    short_t1 = pivots.get("s1", round(price - atr, 2))
    short_t2 = pivots.get("s2", round(price - 2 * atr, 2))

    short_signal, short_reason = _traffic_light(
        score, sc.get("action_bias"), ma_stack, event_ctx, False, "short"
    )
    short_payoff = _estimate_leveraged_payoff(price, short_stop, short_t1, short_t2, pair["leverage"])

    short_size = sizing.calc_position_size(
        price, short_stop, symbol=pair["short"], leverage=pair["leverage"]
    )

    # Key AVWAP levels
    avwap_levels = {}
    for label, data in avwaps.items():
        avwap_levels[label] = data.get("avwap")

    return {
        "chart_symbol": chart_symbol,
        "name": pair["name"],
        "price": price,
        "atr": round(atr, 2),
        "daily_stack": ma_stack,
        "weekly_stack": w.get("ma_stack", "?"),
        "score": score,
        "pivots": pivots,
        "avwap_levels": avwap_levels,
        "event_risk": event_ctx.get("recommendation", ""),
        "long": {
            "vehicle": pair["long"],
            "leverage": pair["leverage"],
            "signal": long_signal,
            "reason": long_reason,
            "stop": long_stop,
            "target_1": long_t1,
            "target_2": long_t2,
            "payoff": long_payoff,
            "sizing": long_size,
            "setup_type": setup.get("type", "?"),
            "setup_trigger": setup.get("trigger"),
            "max_hold_days": 7,
            "do_not_hold_through": [e["event"] for e in event_ctx.get("upcoming_events", []) if e["severity"] == "high"],
        },
        "short": {
            "vehicle": pair["short"],
            "leverage": pair["leverage"],
            "signal": short_signal,
            "reason": short_reason,
            "entry_level": short_entry,
            "stop": short_stop,
            "target_1": short_t1,
            "target_2": short_t2,
            "payoff": short_payoff,
            "sizing": short_size,
            "trigger": f"SHORT via {pair['short']} if {chart_symbol} breaks below {pivots.get('s1', '?')} on volume" if short_signal != "RED" else None,
            "max_hold_days": 5,
            "do_not_hold_through": [e["event"] for e in event_ctx.get("upcoming_events", []) if e["severity"] == "high"],
        },
    }


def run_all_leveraged_tactical(benchmark_packets, regime, force=False):
    """
    Run leveraged tactical analysis for all configured benchmark pairs.
    Returns dict of chart_symbol -> tactical analysis.
    """
    print("\n  LEVERAGED BENCHMARK TACTICAL:")
    results = {}
    for chart_sym in cfg.LEVERAGED_PAIRS:
        pkt = benchmark_packets.get(chart_sym)
        if not pkt:
            continue
        result = analyze_benchmark_tactical(chart_sym, pkt, regime, force)
        results[chart_sym] = result

        pair = cfg.LEVERAGED_PAIRS[chart_sym]
        l = result["long"]
        s = result["short"]
        print(f"    {chart_sym} ({result['price']}) score={result['score']} stack={result['daily_stack']}")
        print(f"      LONG  {pair['long']:5s}: {l['signal']} — {l['reason']}")
        print(f"      SHORT {pair['short']:5s}: {s['signal']} — {s['reason']}")

    return results
