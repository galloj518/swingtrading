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
    python -m swing_engine exits        # Evaluate all open positions
    python -m swing_engine exits NVDA   # Evaluate one open position
    python -m swing_engine portfolio    # Show portfolio exposure / Greeks
    python -m swing_engine backtest     # Run walk-forward backtest
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
from . import calibration
from . import alerts
from . import correlation
from . import exits
from . import portfolio


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


def _open_position_risk_map() -> dict[str, float]:
    """Return open risk dollars by symbol for live correlation-aware sizing."""
    try:
        open_trades = db.get_open_trades()
    except Exception:
        return {}

    risk_map: dict[str, float] = {}
    for trade in open_trades:
        symbol = str(trade.get("symbol", "")).upper()
        entry = float(trade.get("entry_price") or 0)
        stop = float(trade.get("current_stop") or 0) or float(trade.get("stop_price") or 0)
        shares = int(trade.get("shares") or 0)
        if not symbol or entry <= 0 or stop <= 0 or shares <= 0:
            continue
        risk_map[symbol] = risk_map.get(symbol, 0.0) + abs(entry - stop) * shares
    return risk_map


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
    print("\n[1/8] Loading benchmark data...")
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

    # --- Macro signals (VIX term structure, credit spreads, yield curve) ---
    print("  Loading macro signals...", end=" ")
    try:
        macro_signals = mdata.load_macro_signals(force=force)
        print(f"VIX3M/VIX={macro_signals.get('vix_term_structure', '?')} | "
              f"Credit={macro_signals.get('credit_signal', '?')} | "
              f"Curve inv={macro_signals.get('curve_inverted', '?')}")
    except Exception as e:
        macro_signals = {}
        print(f"(unavailable: {e})")

    # --- Regime (with macro overlay) ---
    print("\n[2/8] Calculating market regime...")
    regime_result = regime_mod.calc_regime(
        spy=bench_states.get("SPY", {}),
        qqq=bench_states.get("QQQ", {}),
        soxx=bench_states.get("SOXX", {}),
        dia=bench_states.get("DIA", {}),
        vix_close=vix_close,
        event_risk=events.get_event_context(),
        macro_signals=macro_signals,
    )
    regime_line = regime_mod.regime_summary_text(regime_result)
    print(f"  {regime_line}")

    # --- Load watchlist data ---
    print("\n[3/8] Loading watchlist data...")  # step numbers reflow handled below
    all_packets = {}
    data_store = dict(bench_data)  # start with benchmark data
    watchlist_data = {}
    for sym in cfg.WATCHLIST:
        print(f"  {sym}...", end=" ")
        data = mdata.load_all(sym, force=force)
        data_store[sym] = data
        watchlist_data[sym] = data
        print(f"({len(data.get('daily', []))} bars)")

    # Also build benchmark packets for the dashboard
    print("  Building dynamic correlation matrix...", end=" ")
    corr_matrix = None
    corr_summary = {}
    if getattr(cfg, "USE_DYNAMIC_CORRELATION", False):
        try:
            corr_matrix = correlation.build_dynamic_correlation_matrix(data_store)
            if corr_matrix is not None:
                corr_summary = correlation.correlation_summary(corr_matrix, cfg.WATCHLIST)
                n_pairs = corr_summary.get("high_correlation_pairs", 0)
                print(f"{n_pairs} high-correlation pairs (>{cfg.DYNAMIC_CORR_THRESHOLD})")
            else:
                print("insufficient data")
        except Exception as e:
            print(f"(unavailable: {e})")
    else:
        print("disabled")

    open_position_risk = _open_position_risk_map()

    print("  Building watchlist packets...")
    for sym in cfg.WATCHLIST:
        pkt = packets.build_packet(
            sym,
            watchlist_data[sym],
            spy_daily,
            corr_matrix=corr_matrix,
            open_positions=open_position_risk,
            regime=regime_result,
        )
        all_packets[sym] = pkt
        sc = pkt["score"]
        print(f"    {sym}: score={sc['score']}/100 ({sc['quality']}), "
              f"bias={sc['action_bias']}, "
              f"setup={pkt['setup']['type']}")
        packets.save_packet(sym, pkt)

    for sym in cfg.BENCHMARKS:
        pkt = packets.build_packet(sym, bench_data[sym], spy_daily, regime=regime_result)
        all_packets[sym] = pkt

    # --- Cross-symbol expert context (group strength / rescoring) ---
    packets.enrich_group_strength(all_packets, regime=regime_result)
    calibration_profile = calibration.build_calibration_profile()
    packets.enrich_calibration(all_packets, calibration_profile, regime=regime_result)
    for sym in cfg.WATCHLIST + cfg.BENCHMARKS:
        if sym in all_packets:
            packets.save_packet(sym, all_packets[sym])

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
        sc_val = all_packets[sym]["score"].get("tradeability", {}).get("score", all_packets[sym]["score"]["score"])
        if sc_val >= 60:
            checklist.print_checklist(cl)

    ranked_watchlist = sorted(
        cfg.WATCHLIST,
        key=lambda sym: (
            all_checklists[sym].get("actionability", {}).get("rank", 99),
            -all_packets[sym].get("score", {}).get("tradeability", {}).get("score", all_packets[sym].get("score", {}).get("confidence_adjusted_score", all_packets[sym].get("score", {}).get("score", 0))),
            -all_packets[sym].get("score", {}).get("confidence_adjusted_score", all_packets[sym].get("score", {}).get("score", 0)),
            -all_packets[sym].get("score", {}).get("idea_quality_score", 0),
            -all_packets[sym].get("calibration", {}).get("score", 50),
            -all_packets[sym].get("score", {}).get("entry_timing_score", 0),
            -all_packets[sym].get("score", {}).get("score", 0),
            sym,
        ),
    )

    top_execution = ranked_watchlist[:cfg.TOP_EXECUTION_COUNT]
    top_execution_intraday = [
        sym for sym in top_execution
        if all_packets[sym].get("score", {}).get("tradeability", {}).get("score", all_packets[sym].get("score", {}).get("score", 0)) >= 60
    ][:cfg.TOP_EXECUTION_INTRADAY_COUNT]
    narrative_candidates = ranked_watchlist[:cfg.TOP_NARRATIVE_COUNT]
    top_chart_symbols = ranked_watchlist[:cfg.TOP_CHART_COUNT]

    print("\n  PRIORITY LISTS:")
    print(f"    Top execution: {', '.join(top_execution) if top_execution else 'None'}")
    print(f"    Intraday ladder: {', '.join(top_execution_intraday) if top_execution_intraday else 'None'}")
    print(f"    Narrative set: {', '.join(narrative_candidates) if narrative_candidates else 'None'}")
    print(f"    Chart set: {', '.join(top_chart_symbols) if top_chart_symbols else 'None'}")

    # --- Exit scan: evaluate all open positions ---
    print("\n[6/8] Exit scan (open positions)...")
    try:
        exit_recs = exits.run_exit_scan(data_store=data_store)
        if exit_recs:
            exits.print_exit_report(exit_recs)
    except Exception as e:
        print(f"  Exit scan error: {e}")

    # --- Portfolio exposure / Greeks ---
    print("[6b] Portfolio exposure...")
    portfolio_exposure = {}
    try:
        open_trades = db.get_open_trades()
        portfolio_exposure = portfolio.calc_portfolio_exposure(
            open_trades, all_packets, spy_daily, data_store=data_store
        )
        portfolio.print_exposure_report(portfolio_exposure)
        if portfolio_exposure.get("open_positions", 0) > 0:
            db.save_portfolio_snapshot(portfolio_exposure)
    except Exception as e:
        print(f"  Portfolio exposure error: {e}")

    # --- Alerts ---
    print("[6c] Dispatching alerts...")
    try:
        alerts.dispatch_alerts(all_packets, regime_result)
    except Exception as e:
        print(f"  Alert dispatch error: {e}")

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
        min_score=None,
        selected_symbols=narrative_candidates,
        max_count=cfg.TOP_NARRATIVE_COUNT,
    )

    # --- Charts ---
    print("\n[7/8] Generating charts...")
    from . import charts
    all_symbols = list(dict.fromkeys(cfg.BENCHMARKS + top_chart_symbols))
    chart_data = charts.generate_all_charts(
        all_symbols,
        data_store,
        all_packets,
        intraday_emphasis_symbols=top_execution_intraday,
    )

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
        "top_execution_intraday": top_execution_intraday,
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
            "data_quality": p.get("data_quality", {}),
            "chart_quality": p.get("chart_quality", {}),
            "base_quality": p.get("base_quality", {}),
            "continuation_pattern": p.get("continuation_pattern", {}),
            "institutional_sponsorship": p.get("institutional_sponsorship", {}),
            "overhead_supply": p.get("overhead_supply", {}),
            "clean_air": p.get("clean_air", {}),
            "group_strength": p.get("group_strength", {}),
            "avwap_map": p.get("avwap_map", {}),
            "avwap_context": p.get("avwap_context", {}),
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

    elif args[0] == "exits":
        # python -m swing_engine exits [SYMBOL]
        symbol_filter = args[1].upper() if len(args) >= 2 else None
        recs = exits.run_exit_scan(symbol_filter=symbol_filter)
        exits.print_exit_report(recs)

    elif args[0] == "portfolio":
        spy_daily = _load_spy()
        data = {}  # lazy load only needed symbols
        open_trades = db.get_open_trades()
        if open_trades:
            for t in open_trades:
                sym = t.get("symbol", "")
                if sym and sym not in data:
                    data[sym] = mdata.load_all(sym)
        exposure = portfolio.calc_portfolio_exposure(open_trades, {}, spy_daily, data_store=data)
        portfolio.print_exposure_report(exposure)

    elif args[0] == "backtest":
        from . import backtest as bt_mod
        force = "--force" in args
        start = None
        symbols_arg = []
        i = 1
        while i < len(args):
            arg = args[i]
            if arg == "--start":
                if i + 1 < len(args):
                    start = args[i + 1]
                    i += 2
                    continue
                break
            if arg == "--force":
                i += 1
                continue
            if arg.startswith("-"):
                i += 1
                continue
            symbols_arg.append(arg.upper())
            i += 1
        symbols_to_run = symbols_arg or cfg.WATCHLIST

        print(f"\n  Running walk-forward backtest on {len(symbols_to_run)} symbol(s)...")
        spy_daily = _load_spy(force)
        data_store = {}
        for sym in symbols_to_run:
            data_store[sym] = mdata.load_all(sym, force=force)

        engine = bt_mod.WalkForwardEngine(
            symbols=symbols_to_run,
            daily_data_store=data_store,
            spy_df=spy_daily,
            start_date=start,
        )
        results = engine.run()
        report_path = engine.save_report(results)
        summary = engine.summary(results)
        if not summary.empty:
            print("\n  Walk-forward summary:")
            print(summary[["window", "is_start", "oos_start",
                            "in_sample_win_rate", "out_of_sample_win_rate",
                            "in_sample_avg_r", "out_of_sample_avg_r"]].to_string(index=False))
        print(f"\n  Full report: {report_path}")

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
