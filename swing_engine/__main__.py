"""
CLI entry point for Swing Engine.

Usage:
    python -m swing_engine run          # Full daily analysis
    python -m swing_engine check NVDA   # Single symbol
    python -m swing_engine soxx         # SOXX→SOXL tactical
    python -m swing_engine review       # Weekly review
    python -m swing_engine dashboard    # Regenerate dashboard only
    python -m swing_engine backfill     # Backfill signal outcomes
    python -m swing_engine db-sync      # Sync CSV history into SQLite
"""
import sys
import json
import time
from datetime import date

from . import config as cfg
from . import data as mdata
from . import features as feat
from . import scoring
from . import regime as regime_mod
from . import events
from . import packets
from . import signals
from . import soxx_tactical
from . import checklist
from . import dashboard
from . import review
from . import narrative
from . import leveraged
from . import db


def _load_spy(force: bool = False):
    """Load SPY daily data (needed for RS calculations)."""
    return mdata.load_daily("SPY", force=force)


def _load_vix(force: bool = False):
    """Load VIX and return latest close."""
    try:
        vix = mdata.load_daily(cfg.VIX_SYMBOL, force=force)
        if not vix.empty:
            return vix["close"].iloc[-1]
    except Exception as e:
        print(f"  VIX load failed: {e}")
    return None


def run_daily(force: bool = False):
    """
    Full daily analysis pipeline.
    This is the main command — run before market open.
    """
    print("=" * 60)
    print(f"  SWING ENGINE — DAILY RUN — {date.today().isoformat()}")
    print("=" * 60)

    db.initialize()

    # Clean old cache
    mdata.clean_old_cache()

    # --- Load benchmark data ---
    print("\n[1/6] Loading benchmark data...")
    spy_daily = _load_spy(force)
    vix_close = _load_vix(force)

    bench_data = {}
    bench_states = {}
    for sym in cfg.BENCHMARKS:
        print(f"  {sym}...", end=" ")
        data = mdata.load_all(sym, force=force)
        bench_data[sym] = data
        daily = data["daily"].copy()
        if not daily.empty:
            daily = feat.add_smas(daily, cfg.DAILY_SMA_PERIODS)
            daily = feat.add_atr(daily)
        bench_states[sym] = feat.extract_ma_state(
            daily, cfg.DAILY_SMA_PERIODS, "daily"
        )
        print(f"({len(data['daily'])} bars)")

    # --- Regime ---
    print("\n[2/6] Calculating market regime...")
    regime_result = regime_mod.calc_regime(
        spy=bench_states.get("SPY", {}),
        qqq=bench_states.get("QQQ", {}),
        soxx=bench_states.get("SOXX", {}),
        dia=bench_states.get("DIA", {}),
        vix_close=vix_close,
        event_risk=events.get_event_context(),
    )
    regime_line = regime_mod.regime_summary_text(regime_result)
    print(f"  {regime_line}")

    # --- Load watchlist data ---
    print("\n[3/8] Loading watchlist data...")
    all_packets = {}
    data_store = dict(bench_data)  # start with benchmark data
    for sym in cfg.WATCHLIST:
        print(f"  {sym}...", end=" ")
        data = mdata.load_all(sym, force=force)
        data_store[sym] = data
        pkt = packets.build_packet(sym, data, spy_daily, regime=regime_result)
        all_packets[sym] = pkt
        sc = pkt["score"]
        print(f"score={sc['score']}/100 ({sc['quality']}), "
              f"bias={sc['action_bias']}, "
              f"setup={pkt['setup']['type']}")
        packets.save_packet(sym, pkt)

    # Also build benchmark packets for the dashboard
    for sym in cfg.BENCHMARKS:
        pkt = packets.build_packet(sym, bench_data[sym], spy_daily, regime=regime_result)
        all_packets[sym] = pkt

    # --- Log signals ---
    print("\n[4/8] Logging signals...")
    for sym in cfg.WATCHLIST:
        signals.log_signal(all_packets[sym], regime_result.get("regime", ""))
    print(f"  {len(cfg.WATCHLIST)} signals logged")

    # --- Generate checklists ---
    print("\n[5/8] Generating checklists...")
    all_checklists = {}
    for sym in cfg.WATCHLIST:
        cl = checklist.generate_checklist(all_packets[sym], regime_result)
        all_checklists[sym] = cl
        sc_val = all_packets[sym]["score"]["score"]
        if sc_val >= 60:
            checklist.print_checklist(cl)

    ranked_watchlist = sorted(
        cfg.WATCHLIST,
        key=lambda sym: (
            all_checklists[sym].get("actionability", {}).get("rank", 99),
            -all_packets[sym].get("score", {}).get("score", 0),
            sym,
        ),
    )

    top_execution = ranked_watchlist[:cfg.TOP_EXECUTION_COUNT]
    narrative_candidates = [
        sym for sym in ranked_watchlist
        if all_packets[sym].get("score", {}).get("score", 0) >= 60
        and all_checklists[sym].get("actionability", {}).get("label")
        in ("BUY NOW", "WATCH BREAKOUT", "WAIT PULLBACK", "WAIT ZONE")
    ][:cfg.TOP_NARRATIVE_COUNT]
    top_chart_symbols = ranked_watchlist[:cfg.TOP_CHART_COUNT]

    print("\n  PRIORITY LISTS:")
    print(f"    Top execution: {', '.join(top_execution) if top_execution else 'None'}")
    print(f"    Narrative set: {', '.join(narrative_candidates) if narrative_candidates else 'None'}")
    print(f"    Chart set: {', '.join(top_chart_symbols) if top_chart_symbols else 'None'}")

    # --- SOXX tactical ---
    soxx_dec = None
    try:
        soxx_result = soxx_tactical.run_tactical(force=force)
        soxx_dec = soxx_result.get("decision")
    except Exception as e:
        print(f"  SOXX tactical error: {e}")

    # --- Leveraged benchmark tactical (SPY/QQQ/SOXX -> long/short vehicles) ---
    leveraged_results = {}
    try:
        bench_pkts = {s: all_packets[s] for s in cfg.LEVERAGED_PAIRS if s in all_packets}
        leveraged_results = leveraged.run_all_leveraged_tactical(bench_pkts, regime_result)
    except Exception as e:
        print(f"  Leveraged tactical error: {e}")

    # --- LLM Narratives (optional, only if OPENAI_API_KEY set) ---
    narratives = narrative.generate_narratives(
        all_packets,
        regime_result,
        min_score=60,
        selected_symbols=narrative_candidates,
        max_count=cfg.TOP_NARRATIVE_COUNT,
    )

    # --- Charts ---
    print("\n[7/8] Generating charts...")
    from . import charts
    all_symbols = list(dict.fromkeys(cfg.BENCHMARKS + top_chart_symbols))
    chart_data = charts.generate_all_charts(all_symbols, data_store, all_packets)

    # --- Dashboard ---
    print("\n[8/8] Generating dashboard...")
    dashboard.generate_dashboard(
        regime=regime_result,
        packets=all_packets,
        checklists=all_checklists,
        soxx_decision=soxx_dec,
        narratives=narratives,
        leveraged=leveraged_results,
        chart_images=chart_data,
    )

    # --- Score deltas (compare to yesterday) ---
    from datetime import timedelta
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    yesterday_path = cfg.REPORTS_DIR / f"daily_{yesterday}.json"
    score_deltas = {}
    if yesterday_path.exists():
        try:
            with open(yesterday_path) as f:
                prev = json.load(f)
            prev_syms = prev.get("symbols", {})
            print("\n  SCORE CHANGES vs YESTERDAY:")
            for sym in cfg.WATCHLIST:
                today_sc = all_packets.get(sym, {}).get("score", {}).get("score", 0)
                prev_sc = prev_syms.get(sym, {}).get("score", {}).get("score", 0)
                delta = today_sc - prev_sc
                score_deltas[sym] = delta
                if abs(delta) >= 5:
                    arrow = "+" if delta > 0 else ""
                    print(f"    {sym}: {prev_sc} -> {today_sc} ({arrow}{delta})")
        except Exception:
            pass

    # --- Save daily report ---
    report = {
        "date": date.today().isoformat(),
        "regime": regime_result,
        "score_deltas": score_deltas,
        "narratives": narratives,
        "top_execution": top_execution,
        "top_narratives": narrative_candidates,
        "top_charts": top_chart_symbols,
        "leveraged_tactical": {s: {
            "long_signal": r["long"]["signal"],
            "long_vehicle": r["long"]["vehicle"],
            "short_signal": r["short"]["signal"],
            "short_vehicle": r["short"]["vehicle"],
            "price": r["price"],
            "score": r["score"],
        } for s, r in leveraged_results.items()} if leveraged_results else {},
        "soxx_tactical": {
            "decision": soxx_dec,
            "soxx_score": all_packets.get("SOXX", {}).get("score", {}),
            "soxx_daily": all_packets.get("SOXX", {}).get("daily", {}),
            "soxx_weekly": all_packets.get("SOXX", {}).get("weekly", {}),
            "soxx_avwaps": all_packets.get("SOXX", {}).get("avwap_map", {}),
            "soxx_pivots": all_packets.get("SOXX", {}).get("pivots", {}),
        } if soxx_dec else {},
        "symbols": {s: {
            "score": p["score"],
            "setup": p["setup"],
            "entry_zone": p["entry_zone"],
            "checklist": all_checklists.get(s, {}),
            "avwap_map": p.get("avwap_map", {}),
            "reference_levels": p.get("reference_levels", {}),
            "session_vwaps": p.get("session_vwaps", {}),
            "pivots": p.get("pivots", {}),
            "relative_strength": p.get("relative_strength", {}),
            "recent_high": p.get("recent_high", {}),
            "recent_low": p.get("recent_low", {}),
            "earnings": p.get("earnings", {}),
            "daily_ma_stack": p.get("daily", {}).get("ma_stack"),
            "weekly_ma_stack": p.get("weekly", {}).get("ma_stack"),
        } for s, p in all_packets.items()},
    }
    report_path = cfg.REPORTS_DIR / f"daily_{date.today().isoformat()}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"  DAILY RUN COMPLETE")
    print(f"  Dashboard: {cfg.BASE_DIR / 'dashboard.html'}")
    print(f"  Report:    {report_path}")
    print(f"{'='*60}")


def check_symbol(symbol: str, force: bool = False):
    """Quick check on a single symbol."""
    print(f"\n  Checking {symbol}...")

    spy_daily = _load_spy(force)
    data = mdata.load_all(symbol, force=force)
    pkt = packets.build_packet(symbol, data, spy_daily)

    # Quick regime
    vix_close = _load_vix(force)
    spy_data = mdata.load_all("SPY", force=force)
    spy_d = spy_data["daily"].copy()
    if not spy_d.empty:
        spy_d = feat.add_smas(spy_d, cfg.DAILY_SMA_PERIODS)
        spy_d = feat.add_atr(spy_d)
    spy_state = feat.extract_ma_state(spy_d, cfg.DAILY_SMA_PERIODS, "daily")

    # Minimal regime (SPY-only for quick check)
    regime_result = regime_mod.calc_regime(
        spy=spy_state, qqq=spy_state, soxx=spy_state, dia=spy_state,
        vix_close=vix_close,
    )

    cl = checklist.generate_checklist(pkt, regime_result)
    checklist.print_checklist(cl)

    print(f"  Price:     {pkt['daily'].get('last_close', '?')}")
    print(f"  Setup:     {pkt['setup']['type']} — {pkt['setup']['description']}")
    print(f"  Entry:     {pkt['entry_zone'].get('entry_low')} — {pkt['entry_zone'].get('entry_high')}")
    print(f"  In Zone:   {pkt['entry_zone'].get('in_zone')}")
    print(f"  Stop:      {pkt['entry_zone'].get('stop')}")
    print(f"  Target 1:  {pkt['entry_zone'].get('target_1')} (R:R {pkt['entry_zone'].get('rr_t1')}:1)")
    print(f"  RS 20d:    {pkt['relative_strength'].get('rs_20d')}")
    print(f"  AVWAP:     {list(pkt['avwap_map'].keys())}")


def run_backfill():
    """Backfill outcomes for old signals."""
    print("  Backfilling signal outcomes...")
    count = signals.backfill_outcomes()
    print(f"  Backfilled {count} signals")


def run_db_sync():
    """Sync legacy CSV rows into SQLite."""
    print("  Syncing CSV history into SQLite...")
    db.initialize()
    db.sync_csv_to_db()
    print(f"  Database ready: {cfg.DB_PATH}")


def main():
    """CLI dispatcher."""
    args = sys.argv[1:]

    if not args or args[0] == "run":
        force = "--force" in args
        run_daily(force=force)

    elif args[0] == "check" and len(args) >= 2:
        force = "--force" in args
        check_symbol(args[1].upper(), force=force)

    elif args[0] == "soxx":
        force = "--force" in args
        soxx_tactical.run_tactical(force=force)

    elif args[0] == "review":
        review.run_review()

    elif args[0] == "dashboard":
        print("  Use 'run' to generate a fresh dashboard with current data.")

    elif args[0] == "backfill":
        run_backfill()

    elif args[0] == "db-sync":
        run_db_sync()

    elif args[0] == "log-trade":
        # python -m swing_engine log-trade NVDA buy 134.50 128.00 100 checklist
        if len(args) >= 7:
            signals.log_trade(
                symbol=args[1].upper(),
                action=args[2],
                entry_price=float(args[3]),
                stop_price=float(args[4]),
                shares=int(args[5]),
                reason=args[6] if len(args) > 6 else "checklist",
            )
            print(f"  Trade logged: {args[1].upper()}")
        else:
            print("  Usage: log-trade SYMBOL action entry stop shares reason")

    elif args[0] == "close-trade":
        # python -m swing_engine close-trade NVDA 142.50 target
        if len(args) >= 4:
            signals.close_trade(args[1].upper(), float(args[2]), args[3])
        else:
            print("  Usage: close-trade SYMBOL exit_price reason")

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
