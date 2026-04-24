"""
Review script with setup-family and trigger slicing.
"""
from __future__ import annotations

import sqlite3

import pandas as pd

from . import calibration
from . import calibration_setups
from . import config as cfg
from . import db
from .signals import JOURNAL_LOG, SIGNAL_LOG, load_signal_history


def _load_signals() -> pd.DataFrame:
    db.initialize()
    if SIGNAL_LOG.exists():
        return load_signal_history()
    if cfg.DB_PATH.exists():
        with sqlite3.connect(cfg.DB_PATH) as conn:
            return load_signal_history()
    return pd.DataFrame()


def _load_trades() -> pd.DataFrame:
    db.initialize()
    if cfg.DB_PATH.exists():
        with sqlite3.connect(cfg.DB_PATH) as conn:
            return pd.read_sql_query("SELECT * FROM trades", conn)
    if JOURNAL_LOG.exists():
        df = pd.read_csv(JOURNAL_LOG)
        if "date" in df.columns and "open_date" not in df.columns:
            df = df.rename(columns={"date": "open_date"})
        return df
    return pd.DataFrame()


def run_review() -> dict:
    print("=" * 60)
    print("  WEEKLY REVIEW")
    print("=" * 60)
    results = {}

    trades = _load_trades()
    if not trades.empty and "exit_date" in trades.columns:
        closed = trades[trades["exit_date"].notna()].copy()
        if not closed.empty:
            wins = closed[closed["r_multiple"] > 0]
            print(f"\n  TRADE JOURNAL ({len(closed)} closed trades)")
            print(f"  Win Rate: {len(wins) / len(closed) * 100:.1f}%")
            print(f"  Avg R: {closed['r_multiple'].mean():+.2f}R")
            print(f"  Total P&L: ${closed['pnl_dollars'].sum():+,.2f}")
            results["journal"] = {
                "total_trades": len(closed),
                "win_rate": round(len(wins) / len(closed) * 100, 1),
                "avg_r": round(closed["r_multiple"].mean(), 2),
                "total_pnl": round(closed["pnl_dollars"].sum(), 2),
            }

    signals = _load_signals()
    if not signals.empty:
        setup_summary = calibration_setups.summarize_by_setup(signals)
        best = calibration_setups.best_segments(signals)
        if setup_summary.get("status") == "ok":
            print(f"\n  SIGNAL OUTCOMES ({setup_summary.get('sample_size', 0)} matured signals)")
            for field, heading in (("setup_state", "BY SETUP STATE"), ("setup_family", "BY SETUP FAMILY"), ("trigger_type", "BY TRIGGER TYPE"), ("actionability_label", "BY ACTIONABILITY")):
                groups = setup_summary.get("groups", {}).get(field, {})
                if not groups:
                    continue
                print(f"\n  {heading}:")
                for name, stats in groups.items():
                    print(
                        f"  {name}: n={stats['sample_size']}, avg5d={stats.get('avg_forward_5d', 0):+.2f}% "
                        f"median5d={stats.get('median_forward_5d', 0):+.2f}% win={((stats.get('win_rate') or 0) * 100):.1f}% "
                        f"mfe={stats.get('avg_mfe', 0):+.2f}% mae={stats.get('avg_mae', 0):+.2f}% "
                        f"avgR={stats.get('avg_realized_r', 0):+.2f} exp={stats.get('expectancy', 0):+.2f}"
                    )
            if best:
                print("\n  BEST SEGMENTS:")
                for row in best[:5]:
                    print(
                        f"  {row.get('setup_state')} / {row.get('setup_family')} / {row.get('trigger_type')}: "
                        f"n={row.get('sample_size')} exp={row.get('expectancy', 0):+.2f} avgR={row.get('avg_realized_r', 0):+.2f}"
                    )
            results["signals"] = {"total_signals": setup_summary.get("sample_size", 0), "summary": setup_summary, "best_segments": best[:10]}
        else:
            print("\n  SIGNAL OUTCOMES")
            print("  Insufficient matured history for outcome-calibrated review.")
            results["signals"] = setup_summary

    cal = calibration.build_calibration_profile()
    if cal.get("available"):
        global_stats = cal.get("global", {})
        print("\n  CALIBRATION")
        print(f"  Global success: {global_stats.get('success_rate', 0) * 100:.1f}% | avg outcome {global_stats.get('avg_outcome', 0):+.2f} | n={global_stats.get('sample_size', 0)}")

    print(f"\n{'='*60}")
    return results
