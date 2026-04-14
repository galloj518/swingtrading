"""
Weekly Review Script.

Prefers SQLite as the system of record and falls back to CSV only if needed.
"""
import sqlite3

import pandas as pd

from . import config as cfg
from . import db
from .signals import SIGNAL_LOG, JOURNAL_LOG


def _load_signals() -> pd.DataFrame:
    """Load signals from SQLite when available, otherwise from CSV."""
    db.initialize()
    if cfg.DB_PATH.exists():
        with sqlite3.connect(cfg.DB_PATH) as conn:
            return pd.read_sql_query("SELECT * FROM signals", conn)
    if SIGNAL_LOG.exists():
        return pd.read_csv(SIGNAL_LOG)
    return pd.DataFrame()


def _load_trades() -> pd.DataFrame:
    """Load trades from SQLite when available, otherwise from CSV."""
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
    """
    Run weekly performance review.
    Prints summary and returns structured results.
    """
    print("=" * 60)
    print("  WEEKLY REVIEW")
    print("=" * 60)

    results = {}

    # --- Trade Journal Review ---
    j = _load_trades()
    if not j.empty and "exit_date" in j.columns:
        closed = j[j["exit_date"].notna()].copy()

        if not closed.empty:
            total = len(closed)
            wins = len(closed[closed["r_multiple"] > 0])
            losses = len(closed[closed["r_multiple"] <= 0])
            win_rate = round(wins / total * 100, 1) if total > 0 else 0
            avg_r = round(closed["r_multiple"].mean(), 2)
            avg_win_r = round(closed[closed["r_multiple"] > 0]["r_multiple"].mean(), 2) if wins > 0 else 0
            avg_loss_r = round(closed[closed["r_multiple"] <= 0]["r_multiple"].mean(), 2) if losses > 0 else 0
            total_pnl = round(closed["pnl_dollars"].sum(), 2)
            best = closed.loc[closed["r_multiple"].idxmax()]
            worst = closed.loc[closed["r_multiple"].idxmin()]

            checklist_trades = closed[closed["reason"] == "checklist"]
            gut_trades = closed[closed["reason"] != "checklist"]
            cl_avg_r = round(checklist_trades["r_multiple"].mean(), 2) if len(checklist_trades) > 0 else None
            gut_avg_r = round(gut_trades["r_multiple"].mean(), 2) if len(gut_trades) > 0 else None

            results["journal"] = {
                "total_trades": total,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "avg_r": avg_r,
                "avg_win_r": avg_win_r,
                "avg_loss_r": avg_loss_r,
                "total_pnl": total_pnl,
                "best_trade": f"{best['symbol']} {best['r_multiple']:+.1f}R",
                "worst_trade": f"{worst['symbol']} {worst['r_multiple']:+.1f}R",
                "checklist_avg_r": cl_avg_r,
                "gut_feel_avg_r": gut_avg_r,
            }

            print(f"\n  TRADE JOURNAL ({total} closed trades)")
            print(f"  {'-'*45}")
            print(f"  Win Rate:       {win_rate}% ({wins}W / {losses}L)")
            print(f"  Avg R:          {avg_r:+.2f}R")
            print(f"  Avg Win:        {avg_win_r:+.2f}R")
            print(f"  Avg Loss:       {avg_loss_r:+.2f}R")
            print(f"  Total P&L:      ${total_pnl:+,.2f}")
            print(f"  Best Trade:     {best['symbol']} {best['r_multiple']:+.1f}R")
            print(f"  Worst Trade:    {worst['symbol']} {worst['r_multiple']:+.1f}R")

            if cl_avg_r is not None and gut_avg_r is not None:
                print(f"\n  BEHAVIORAL SPLIT:")
                print(f"  Checklist trades: {cl_avg_r:+.2f}R avg ({len(checklist_trades)} trades)")
                print(f"  Non-checklist:    {gut_avg_r:+.2f}R avg ({len(gut_trades)} trades)")
                diff = (cl_avg_r or 0) - (gut_avg_r or 0)
                if diff > 0:
                    print(f"  >>> Checklist edge: +{diff:.2f}R per trade")
                else:
                    print(f"  >>> No checklist edge detected ({diff:+.2f}R)")
        else:
            print("\n  No closed trades yet.")
    else:
        print("\n  No trade journal found.")

    # --- Signal Review ---
    s = _load_signals()
    if not s.empty and "fwd_5d_ret" in s.columns:
        filled = s[s["fwd_5d_ret"].notna()].copy()

        if not filled.empty:
            filled["score_bucket"] = pd.cut(
                filled["score"], bins=[0, 40, 55, 70, 85, 100],
                labels=["0-40", "40-55", "55-70", "70-85", "85-100"]
            )

            print(f"\n  SIGNAL PERFORMANCE ({len(filled)} signals with outcomes)")
            print(f"  {'-'*45}")

            bucket_stats = filled.groupby("score_bucket", observed=True).agg(
                count=("score", "count"),
                avg_score=("score", "mean"),
                avg_5d_ret=("fwd_5d_ret", "mean"),
                trigger_rate=("triggered", "mean"),
            ).round(2)

            for bucket, row in bucket_stats.iterrows():
                print(
                    f"  Score {bucket}: n={row['count']:.0f}, "
                    f"avg 5d ret={row['avg_5d_ret']:+.1f}%, "
                    f"trigger rate={row['trigger_rate']*100:.0f}%"
                )

            results["signals"] = {
                "total_signals": len(filled),
                "by_bucket": bucket_stats.to_dict("index"),
            }

            setup_stats = filled.groupby("setup_type").agg(
                count=("score", "count"),
                avg_5d_ret=("fwd_5d_ret", "mean"),
            ).round(2).sort_values("avg_5d_ret", ascending=False)

            print(f"\n  BY SETUP TYPE:")
            for stype, row in setup_stats.iterrows():
                print(f"  {stype}: n={row['count']:.0f}, avg 5d={row['avg_5d_ret']:+.1f}%")

            triggered = filled[filled["triggered"] == 1].copy()
            if not triggered.empty:
                print(f"\n  LEVEL LEARNING ({len(triggered)} triggered signals)")
                print(f"  {'-'*45}")
                for label in ["hit_target_1", "hit_target_2", "hit_pivot_r1", "hit_pivot_r2", "hit_pivot_r3"]:
                    if label in triggered.columns:
                        hit_rate = round(triggered[label].fillna(0).astype(float).mean() * 100, 1)
                        print(f"  {label}: {hit_rate:.1f}%")

                print(f"\n  SEQUENCING:")
                for label in ["stop_before_target_1", "target_1_before_stop", "target_2_before_stop", "target_3_before_stop"]:
                    if label in triggered.columns:
                        valid = triggered[label].dropna()
                        if not valid.empty:
                            rate = round(valid.astype(float).mean() * 100, 1)
                            print(f"  {label}: {rate:.1f}%")

                if "first_target_hit" in triggered.columns:
                    first_target = triggered["first_target_hit"].dropna()
                    if not first_target.empty:
                        print(f"\n  FIRST TARGET HIT:")
                        for level, count in first_target.value_counts().head(3).items():
                            print(f"  {level}: {count}")

                if "first_resistance" in triggered.columns:
                    first_res = triggered["first_resistance"].dropna()
                    if not first_res.empty:
                        print(f"\n  FIRST RESISTANCE SEEN:")
                        for level, count in first_res.value_counts().head(3).items():
                            print(f"  {level}: {count}")

                if "first_support" in triggered.columns:
                    first_sup = triggered["first_support"].dropna()
                    if not first_sup.empty:
                        print(f"\n  FIRST SUPPORT TESTED:")
                        for level, count in first_sup.value_counts().head(3).items():
                            print(f"  {level}: {count}")

                if {"max_favorable_excursion_pct", "max_adverse_excursion_pct"}.issubset(set(triggered.columns)):
                    mfe = round(triggered["max_favorable_excursion_pct"].dropna().mean(), 2)
                    mae = round(triggered["max_adverse_excursion_pct"].dropna().mean(), 2)
                    print(f"\n  Avg MFE: {mfe:+.2f}%")
                    print(f"  Avg MAE: {mae:+.2f}%")
        else:
            print("\n  No signal outcomes backfilled yet.")
    else:
        print("\n  No signal log found.")

    print(f"\n{'='*60}")
    return results
