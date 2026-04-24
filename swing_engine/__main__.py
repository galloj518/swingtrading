"""
CLI entry point for Swing Engine.

Production mode (lightweight, GitHub-friendly):
    python -m swing_engine run-decision-report
    python -m swing_engine run-dashboard

Research mode (heavy, local):
    python -m swing_engine backtest-walkforward
    python -m swing_engine research-signals
    python -m swing_engine research-models
    python -m swing_engine research-taxonomy
    python -m swing_engine run-near-action-analysis
    python -m swing_engine run-near-action-path-analysis
    python -m swing_engine run-gate-diagnostics
    python -m swing_engine run-root-cause-diagnostics
    python -m swing_engine run-pivot-consistency-audit
    python -m swing_engine run-pivot-pass-analysis

Usage:
    python -m swing_engine run
    python -m swing_engine run-structural
    python -m swing_engine run-breakout-watch
    python -m swing_engine run-triggers
    python -m swing_engine run-narratives
    python -m swing_engine check NVDA
    python -m swing_engine soxx
    python -m swing_engine review
    python -m swing_engine backfill
    python -m swing_engine backtest-events
    python -m swing_engine backtest-walkforward
    python -m swing_engine calibrate-thresholds
    python -m swing_engine review-backtest
    python -m swing_engine research-signals
    python -m swing_engine research-models
    python -m swing_engine research-taxonomy
    python -m swing_engine run-decision-report
    python -m swing_engine run-dashboard
    python -m swing_engine run-gate-diagnostics
    python -m swing_engine run-root-cause-diagnostics
    python -m swing_engine run-pivot-consistency-audit
    python -m swing_engine run-pivot-pass-analysis
    python -m swing_engine run-near-action-analysis
    python -m swing_engine run-near-action-path-analysis
    python -m swing_engine db-sync
    python -m swing_engine exits
    python -m swing_engine portfolio
    python -m swing_engine smoke
"""
from __future__ import annotations
from typing import Optional, List, Tuple

import sys

from . import checklist
from . import backtest as bt_mod
from . import config as cfg
from . import data as mdata
from . import decision_report
from . import db
from . import gate_diagnostics
from . import near_action_analysis
from . import near_action_path_analysis
from . import packets
from . import pivot_consistency_audit
from . import pivot_pass_analysis
from . import research as research_mod
from . import review
from . import root_cause_diagnostics
from . import scan_modes
from . import signals
from . import smoke
from . import soxx_tactical
from .runtime_logging import configure_logging


COMMAND_HELP = {
    "run": "Usage: python -m swing_engine run [--force] [--with-narratives]",
    "run-structural": "Usage: python -m swing_engine run-structural [--force]",
    "run-breakout-watch": "Usage: python -m swing_engine run-breakout-watch [--force]",
    "run-triggers": "Usage: python -m swing_engine run-triggers [--force]",
    "run-narratives": "Usage: python -m swing_engine run-narratives [--force]",
    "check": "Usage: python -m swing_engine check SYMBOL [--force]",
    "soxx": "Usage: python -m swing_engine soxx [--force]",
    "review": "Usage: python -m swing_engine review",
    "backfill": "Usage: python -m swing_engine backfill",
    "backtest-events": "Usage: python -m swing_engine backtest-events [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--smoke] [SYMBOL ...]",
    "backtest-walkforward": "Usage: python -m swing_engine backtest-walkforward [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--smoke] [SYMBOL ...]",
    "calibrate-thresholds": "Usage: python -m swing_engine calibrate-thresholds [--mode backtest-events|backtest-walkforward]",
    "review-backtest": "Usage: python -m swing_engine review-backtest [--mode backtest-events|backtest-walkforward]",
    "research-signals": "Usage: python -m swing_engine research-signals [--mode backtest-events|backtest-walkforward]",
    "research-models": "Usage: python -m swing_engine research-models [--mode backtest-events|backtest-walkforward]",
    "research-taxonomy": "Usage: python -m swing_engine research-taxonomy [--mode backtest-events|backtest-walkforward]",
    "run-decision-report": "Usage: python -m swing_engine run-decision-report [--force]",
    "run-dashboard": "Usage: python -m swing_engine run-dashboard [--force]",
    "run-gate-diagnostics": "Usage: python -m swing_engine run-gate-diagnostics [--force]",
    "run-root-cause-diagnostics": "Usage: python -m swing_engine run-root-cause-diagnostics [--force]",
    "run-pivot-consistency-audit": "Usage: python -m swing_engine run-pivot-consistency-audit [--force]",
    "run-pivot-pass-analysis": "Usage: python -m swing_engine run-pivot-pass-analysis [--force]",
    "run-near-action-analysis": "Usage: python -m swing_engine run-near-action-analysis",
    "run-near-action-path-analysis": "Usage: python -m swing_engine run-near-action-path-analysis",
    "db-sync": "Usage: python -m swing_engine db-sync",
    "portfolio": "Usage: python -m swing_engine portfolio [--force]",
    "backtest": "Usage: python -m swing_engine backtest [SYMBOL ...] [--start YYYY-MM-DD] [--force]",
    "smoke": "Usage: python -m swing_engine smoke",
}


def check_symbol(symbol: str, force: bool = False):
    spy_daily = mdata.load_daily("SPY", force=force)
    data = mdata.load_all(symbol, force=force)
    context = scan_modes.build_scan_context(force=force)
    packet = packets.build_packet(symbol, data, spy_daily, regime=context["regime"])
    cl = checklist.generate_checklist(packet, context["regime"])
    checklist.print_checklist(cl)
    print(f"  Price: {packet['daily'].get('last_close', '?')}")
    print(f"  Setup: {packet['setup'].get('setup_family')} / {packet['setup'].get('state')}")
    print(f"  Trigger: {packet.get('intraday_trigger', {}).get('primary', {}).get('detail')}")
    print(f"  Freshness: {packet.get('data_quality', {}).get('intraday_freshness_label')} ({packet.get('data_quality', {}).get('intraday_freshness_minutes')}m)")


def run_backfill():
    print("  Backfilling signal outcomes...")
    count = signals.backfill_outcomes()
    print(f"  Backfilled {count} signals")


def run_db_sync():
    print("  Syncing CSV history into SQLite...")
    db.initialize()
    db.sync_csv_to_db()
    print(f"  Database ready: {cfg.DB_PATH}")


def _parse_backtest_args(args: List[str]) -> Tuple[List[str], str, str, bool,Optional[str]]:
    symbols_arg = []
    start = cfg.BACKTEST_START_DATE
    end = cfg.BACKTEST_END_DATE
    smoke_mode = "--smoke" in args
    mode = None
    i = 1
    while i < len(args):
        arg = args[i]
        if arg == "--start" and i + 1 < len(args):
            start = args[i + 1]
            i += 2
            continue
        if arg == "--end" and i + 1 < len(args):
            end = args[i + 1]
            i += 2
            continue
        if arg == "--mode" and i + 1 < len(args):
            mode = args[i + 1]
            i += 2
            continue
        if arg.startswith("-"):
            i += 1
            continue
        symbols_arg.append(arg.upper())
        i += 1
    return symbols_arg or cfg.BACKTEST_SYMBOLS, start, end, smoke_mode, mode


def main():
    configure_logging()
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    if args[0] in {"-h", "--help", "help"}:
        print(__doc__)
        return

    cmd = args[0] if args else "run"
    force = "--force" in args
    include_narratives = "--with-narratives" in args

    if "-h" in args[1:] or "--help" in args[1:]:
        print(COMMAND_HELP.get(cmd, __doc__))
        return

    if cmd == "run":
        scan_modes.run_combined(force=force, include_narratives=include_narratives)
    elif cmd == "run-structural":
        scan_modes.run_structural(force=force)
    elif cmd == "run-breakout-watch":
        scan_modes.run_breakout_watch(force=force)
    elif cmd == "run-triggers":
        scan_modes.run_triggers(force=force)
    elif cmd == "run-narratives":
        scan_modes.run_narratives(force=force)
    elif cmd == "check" and len(args) >= 2:
        check_symbol(args[1].upper(), force=force)
    elif cmd == "soxx":
        soxx_tactical.run_tactical(force=force)
    elif cmd == "review":
        review.run_review()
    elif cmd == "backfill":
        run_backfill()
    elif cmd == "db-sync":
        run_db_sync()
    elif cmd == "log-trade" and len(args) >= 7:
        signals.log_trade(args[1].upper(), args[2], float(args[3]), float(args[4]), int(args[5]), args[6])
    elif cmd == "close-trade" and len(args) >= 4:
        signals.close_trade(args[1].upper(), float(args[2]), args[3])
    elif cmd == "exits":
        from . import exits
        symbol_filter = args[1].upper() if len(args) >= 2 else None
        recs = exits.run_exit_scan(symbol_filter=symbol_filter)
        exits.print_exit_report(recs)
    elif cmd == "portfolio":
        from . import portfolio
        spy_daily = mdata.load_daily("SPY", force=force)
        data_store = {}
        open_trades = db.get_open_trades()
        if open_trades:
            for trade in open_trades:
                symbol = trade.get("symbol")
                if symbol and symbol not in data_store:
                    data_store[symbol] = mdata.load_all(symbol, force=force)
        exposure = portfolio.calc_portfolio_exposure(open_trades, {}, spy_daily, data_store=data_store)
        portfolio.print_exposure_report(exposure)
    elif cmd == "backtest-events":
        symbols_to_run, start, end, smoke_mode, _ = _parse_backtest_args(args)
        _, report_path = bt_mod.run_event_backtest(symbols_to_run, start_date=start, end_date=end, smoke_mode=smoke_mode)
        print(f"  Event-study report: {report_path}")
    elif cmd == "backtest-walkforward":
        symbols_to_run, start, end, smoke_mode, _ = _parse_backtest_args(args)
        _, report_path = bt_mod.run_walkforward_backtest(symbols_to_run, start_date=start, end_date=end, smoke_mode=smoke_mode)
        print(f"  Walk-forward report: {report_path}")
    elif cmd == "calibrate-thresholds":
        _, _, _, _, mode = _parse_backtest_args(args)
        _, report_path = bt_mod.calibrate_thresholds_from_backtest(replay_mode=mode)
        print(f"  Threshold profile: {report_path}")
    elif cmd == "review-backtest":
        _, _, _, _, mode = _parse_backtest_args(args)
        _, report_path = bt_mod.review_backtest_results(replay_mode=mode)
        print(f"  Backtest review: {report_path}")
    elif cmd == "research-signals":
        _, _, _, _, mode = _parse_backtest_args(args)
        _, outputs = research_mod.run_research_signals(replay_mode=mode or "backtest-walkforward")
        print(f"  Research grouped summaries: {outputs['grouped_json']}")
        print(f"  Research feature analysis: {outputs['feature_json']}")
    elif cmd == "research-models":
        _, _, _, _, mode = _parse_backtest_args(args)
        _, report_path = research_mod.run_research_models(replay_mode=mode or "backtest-walkforward")
        print(f"  Research models: {report_path}")
    elif cmd == "research-taxonomy":
        _, _, _, _, mode = _parse_backtest_args(args)
        _, outputs = research_mod.run_research_taxonomy(replay_mode=mode or "backtest-walkforward")
        print(f"  Taxonomy research: {outputs['taxonomy_path']}")
        print(f"  Strategy recommendations: {outputs['strategy_path']}")
    elif cmd == "run-decision-report":
        result = decision_report.run_decision_report(force=force, save=True)
        if result.get("output_path"):
            print(f"\n  Decision report saved: {result['output_path']}")
        if result.get("dashboard_path"):
            print(f"  Dashboard written: {result['dashboard_path']}")
    elif cmd == "run-dashboard":
        result = decision_report.run_decision_report(force=force, save=True)
        if result.get("dashboard_path"):
            print(f"\n  Dashboard written: {result['dashboard_path']}")
        if result.get("output_path"):
            print(f"  Decision report saved: {result['output_path']}")
    elif cmd == "run-gate-diagnostics":
        result = gate_diagnostics.run_gate_diagnostics(force=force, save=True)
        if result.get("output_path"):
            print(f"\n  Gate diagnostics saved: {result['output_path']}")
    elif cmd == "run-root-cause-diagnostics":
        result = root_cause_diagnostics.run_root_cause_diagnostics(force=force, save=True)
        if result.get("output_path"):
            print(f"\n  Root-cause diagnostics saved: {result['output_path']}")
    elif cmd == "run-pivot-consistency-audit":
        result = pivot_consistency_audit.run_pivot_consistency_audit(force=force, save=True)
        if result.get("output_path"):
            print(f"\n  Pivot consistency audit saved: {result['output_path']}")
    elif cmd == "run-pivot-pass-analysis":
        result = pivot_pass_analysis.run_pivot_pass_analysis(force=force, save=True)
        if result.get("output_path"):
            print(f"\n  Pivot-pass analysis saved: {result['output_path']}")
    elif cmd == "run-near-action-analysis":
        result = near_action_analysis.run_near_action_analysis(save=True)
        if result.get("output_path"):
            print(f"\n  Near-action analysis saved: {result['output_path']}")
    elif cmd == "run-near-action-path-analysis":
        result = near_action_path_analysis.run_near_action_path_analysis(save=True)
        if result.get("output_path"):
            print(f"\n  Near-action path analysis saved: {result['output_path']}")
    elif cmd == "backtest":
        symbols_to_run, start, end, smoke_mode, _ = _parse_backtest_args(args)
        _, report_path = bt_mod.run_walkforward_backtest(symbols_to_run, start_date=start, end_date=end, smoke_mode=smoke_mode)
        print(f"  Walk-forward report: {report_path}")
    elif cmd == "smoke":
        result = smoke.run_offline_smoke()
        print(f"  Offline smoke health: {result['health_path']}")
        if result.get("dashboard_path"):
            print(f"  Offline smoke dashboard: {result['dashboard_path']}")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
