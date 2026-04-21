"""
CLI entry point for Swing Engine.

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
    python -m swing_engine db-sync
    python -m swing_engine exits
    python -m swing_engine portfolio
    python -m swing_engine smoke
"""
from __future__ import annotations

import sys

from . import checklist
from . import config as cfg
from . import data as mdata
from . import db
from . import packets
from . import review
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
    elif cmd == "backtest":
        from . import backtest as bt_mod
        start = None
        for i, a in enumerate(args):
            if a == "--start" and i + 1 < len(args):
                start = args[i + 1]
        symbols_arg = [a.upper() for a in args[1:] if not a.startswith("-") and a != "--start"]
        symbols_arg = []
        i = 1
        while i < len(args):
            arg = args[i]
            if arg == "--start" and i + 1 < len(args):
                start = args[i + 1]
                i += 2
                continue
            if arg.startswith("-"):
                i += 1
                continue
            symbols_arg.append(arg.upper())
            i += 1
        symbols_to_run = symbols_arg or cfg.WATCHLIST
        spy_daily = mdata.load_daily("SPY", force=force)
        data_store = {symbol: mdata.load_all(symbol, force=force) for symbol in symbols_to_run}
        engine = bt_mod.WalkForwardEngine(symbols=symbols_to_run, daily_data_store=data_store, spy_df=spy_daily, start_date=start)
        results = engine.run()
        report_path = engine.save_report(results)
        print(f"  Full report: {report_path}")
    elif cmd == "smoke":
        result = smoke.run_offline_smoke()
        print(f"  Offline smoke health: {result['health_path']}")
        if result.get("dashboard_path"):
            print(f"  Offline smoke dashboard: {result['dashboard_path']}")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
