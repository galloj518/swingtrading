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
from .signals import JOURNAL_LOG, SIGNAL_LOG


def _load_signals() -> pd.DataFrame:
    db.initialize()
    if SIGNAL_LOG.exists():
        return pd.read_csv(SIGNAL_LOG)
    if cfg.DB_PATH.exists():
        with sqlite3.connect(cfg.DB_PATH) as conn:
            return pd.read_sql_query("SELECT * FROM signals", conn)
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
    if not signals.empty and "fwd_5d_ret" in signals.columns:
        filled = signals[signals["fwd_5d_ret"].notna()].copy()
        if not filled.empty:
            print(f"\n  SIGNAL PERFORMANCE ({len(filled)} signals with outcomes)")
            bucket_stats = filled.groupby(pd.cut(filled["score"], bins=[0, 40, 55, 70, 85, 100], labels=["0-40", "40-55", "55-70", "70-85", "85-100"]))[["fwd_5d_ret"]].mean().round(2)
            for bucket, row in bucket_stats.iterrows():
                print(f"  Score {bucket}: avg 5d ret {row['fwd_5d_ret']:+.2f}%")
            setup_summary = calibration_setups.summarize_by_setup(filled)
            best = calibration_setups.best_segments(filled)
            for field, heading in (("setup_family", "BY SETUP FAMILY"), ("trigger_type", "BY TRIGGER TYPE"), ("actionability_label", "BY ACTIONABILITY")):
                if field in setup_summary:
                    print(f"\n  {heading}:")
                    for name, stats in setup_summary[field].items():
                        print(f"  {name}: n={stats['count']}, avg 5d={stats.get('avg_5d_ret', 0):+.2f}%")
            if best:
                print("\n  BEST SEGMENTS:")
                for row in best[:5]:
                    print(f"  {row.get('setup_family')} / {row.get('trigger_type')} / {row.get('actionability_label')}: n={row.get('count')} avg={row.get('avg_5d_ret', 0):+.2f}%")
            results["signals"] = {"total_signals": len(filled), "summary": setup_summary, "best_segments": best[:10]}

    cal = calibration.build_calibration_profile()
    if cal.get("available"):
        global_stats = cal.get("global", {})
        print("\n  CALIBRATION")
        print(f"  Global success: {global_stats.get('success_rate', 0) * 100:.1f}% | avg outcome {global_stats.get('avg_outcome', 0):+.2f} | n={global_stats.get('sample_size', 0)}")

    print(f"\n{'='*60}")
    return results
