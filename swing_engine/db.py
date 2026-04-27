"""
SQLite persistence for signal calls and executed trades.

CSV files remain available for portability, but the database acts as the
durable store the app can update over time.
"""
from typing import Optional, List
import json
import sqlite3
from datetime import datetime

import pandas as pd

from . import config as cfg


SIGNAL_UNIQUE_KEY = ("signal_date", "symbol")
_INITIALIZED = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(cfg.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize() -> None:
    """Create tables and backfill from CSVs if they already exist."""
    global _INITIALIZED
    if _INITIALIZED:
        return

    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                score REAL,
                quality TEXT,
                idea_quality_score REAL,
                idea_quality TEXT,
                entry_timing_score REAL,
                entry_timing TEXT,
                action_bias TEXT,
                setup_type TEXT,
                setup_family TEXT,
                setup_stage TEXT,
                setup_state TEXT,
                trigger_type TEXT,
                triggered_now_bucket TEXT,
                structural_score REAL,
                breakout_readiness_score REAL,
                trigger_readiness_score REAL,
                weekly_gate INTEGER,
                daily_gate INTEGER,
                entry_low REAL,
                entry_high REAL,
                stop REAL,
                target_1 REAL,
                target_2 REAL,
                price_at_signal REAL,
                atr REAL,
                rs_20d REAL,
                pivot_level REAL,
                trigger_level REAL,
                pivot_distance_pct REAL,
                extension_atr REAL,
                reward_risk_now REAL,
                short_ma_rising INTEGER,
                tightening_to_short_ma INTEGER,
                larger_ma_supportive INTEGER,
                avwap_supportive INTEGER,
                avwap_resistance INTEGER,
                avwap_context TEXT,
                active_avwap_anchors TEXT,
                nearest_support_avwap_anchor TEXT,
                nearest_resistance_avwap_anchor TEXT,
                avwap_distance_pct REAL,
                avwap_resistance_filter_flag INTEGER,
                avwap_resistance_filter_reason TEXT,
                avwap_resistance_anchor TEXT,
                avwap_resistance_distance_pct REAL,
                avwap_location_quality TEXT,
                expansion_score REAL,
                range_ratio REAL,
                volume_ratio REAL,
                atr_ratio REAL,
                expansion_quality TEXT,
                rsi_14 REAL,
                rsi_bucket TEXT,
                rsi_trend TEXT,
                overhead_supply_score REAL,
                overhead_supply_detail TEXT,
                regime TEXT,
                event_risk INTEGER,
                rvol REAL,
                freshness_label TEXT,
                freshness_minutes REAL,
                actionability_label TEXT,
                data_quality_score REAL,
                run_mode TEXT,
                calibration_provenance TEXT,
                calibration_confidence TEXT,
                slippage_est_bps REAL,
                cost_dollars_est REAL,
                net_rr_t1_est REAL,
                triggered INTEGER,
                trigger_date TEXT,
                trigger_price REAL,
                fwd_1d_ret REAL,
                fwd_3d_ret REAL,
                fwd_5d_ret REAL,
                fwd_10d_ret REAL,
                fwd_20d_ret REAL,
                entry_model_date TEXT,
                entry_model_price REAL,
                stop_model_price REAL,
                target_model_price REAL,
                triggered_model INTEGER,
                days_to_entry INTEGER,
                days_to_stop INTEGER,
                days_to_target INTEGER,
                realized_r REAL,
                outcome_r REAL,
                outcome_status TEXT,
                hit_target_1 INTEGER,
                hit_target_2 INTEGER,
                hit_pivot_r1 INTEGER,
                hit_pivot_r2 INTEGER,
                hit_pivot_r3 INTEGER,
                stop_hit INTEGER,
                stop_before_target_1 INTEGER,
                target_1_before_stop INTEGER,
                target_2_before_stop INTEGER,
                target_3_before_stop INTEGER,
                first_target_hit TEXT,
                first_resistance TEXT,
                first_support TEXT,
                max_favorable_excursion_pct REAL,
                max_adverse_excursion_pct REAL,
                packet_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(signal_date, symbol)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                open_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT,
                entry_price REAL,
                stop_price REAL,
                shares INTEGER,
                reason TEXT,
                notes TEXT,
                exit_date TEXT,
                exit_price REAL,
                exit_reason TEXT,
                r_multiple REAL,
                pnl_dollars REAL,
                status TEXT NOT NULL DEFAULT 'open',
                partial_1_taken INTEGER DEFAULT 0,
                partial_2_taken INTEGER DEFAULT 0,
                current_stop REAL,
                trailing_stop REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                open_positions INTEGER,
                total_dollar_exposure REAL,
                net_beta_exposure REAL,
                net_beta_pct REAL,
                portfolio_beta REAL,
                open_risk_dollars REAL,
                open_risk_pct REAL,
                vix_1pt_impact_dollars REAL,
                max_single_position_pct REAL,
                largest_sector_exposure REAL,
                largest_sector_pct REAL,
                snapshot_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_date
            ON portfolio_snapshots(snapshot_date);

            CREATE INDEX IF NOT EXISTS idx_signals_symbol_date
            ON signals(symbol, signal_date);

            CREATE INDEX IF NOT EXISTS idx_trades_symbol_status
            ON trades(symbol, status, open_date);

            CREATE TABLE IF NOT EXISTS backtest_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_mode TEXT NOT NULL,
                evaluation_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                setup_state TEXT,
                setup_family TEXT,
                setup_stage TEXT,
                actionability_label TEXT,
                trigger_type TEXT,
                structural_score REAL,
                breakout_readiness_score REAL,
                trigger_readiness_score REAL,
                pivot_level REAL,
                trigger_level REAL,
                production_score REAL,
                sizing_tier TEXT,
                pivot_zone TEXT,
                trigger_band TEXT,
                breakout_band TEXT,
                structural_band TEXT,
                dominant_negative_flags TEXT,
                interaction_cluster_flags TEXT,
                readiness_rebalance_flags TEXT,
                pivot_position TEXT,
                pivot_distance_pct REAL,
                extension_atr REAL,
                reward_risk_now REAL,
                short_ma_rising INTEGER,
                tightening_to_short_ma INTEGER,
                larger_ma_supportive INTEGER,
                avwap_supportive INTEGER,
                avwap_resistance INTEGER,
                avwap_context TEXT,
                active_avwap_anchors TEXT,
                nearest_support_avwap_anchor TEXT,
                nearest_resistance_avwap_anchor TEXT,
                avwap_distance_pct REAL,
                expansion_score REAL,
                range_ratio REAL,
                volume_ratio REAL,
                atr_ratio REAL,
                expansion_quality TEXT,
                rsi_14 REAL,
                rsi_bucket TEXT,
                rsi_trend TEXT,
                overhead_supply_score REAL,
                contraction_score REAL,
                freshness_label TEXT,
                calibration_provenance TEXT,
                calibration_confidence TEXT,
                fwd_1d_ret REAL,
                fwd_3d_ret REAL,
                fwd_5d_ret REAL,
                fwd_10d_ret REAL,
                return_5d REAL,
                return_10d REAL,
                fwd_20d_ret REAL,
                max_favorable_excursion_pct REAL,
                max_adverse_excursion_pct REAL,
                realized_r REAL,
                outcome_status TEXT,
                target_1_before_stop INTEGER,
                stop_before_target_1 INTEGER,
                payload_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(replay_mode, evaluation_date, symbol)
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_events_mode_date
            ON backtest_events(replay_mode, evaluation_date);
            """
        )
        _ensure_signal_columns(conn)
        _ensure_trade_columns(conn)
        _ensure_backtest_event_columns(conn)

    _INITIALIZED = True
    sync_csv_to_db()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_signal_columns(conn: sqlite3.Connection) -> None:
    """Add any missing columns required by the current signals schema."""
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(signals)").fetchall()
    }
    desired = {
        "score": "REAL",
        "quality": "TEXT",
        "idea_quality_score": "REAL",
        "idea_quality": "TEXT",
        "entry_timing_score": "REAL",
        "entry_timing": "TEXT",
        "action_bias": "TEXT",
        "setup_type": "TEXT",
        "setup_family": "TEXT",
        "setup_stage": "TEXT",
        "setup_state": "TEXT",
        "trigger_type": "TEXT",
        "triggered_now_bucket": "TEXT",
        "structural_score": "REAL",
        "breakout_readiness_score": "REAL",
        "trigger_readiness_score": "REAL",
        "weekly_gate": "INTEGER",
        "daily_gate": "INTEGER",
        "entry_low": "REAL",
        "entry_high": "REAL",
        "stop": "REAL",
        "target_1": "REAL",
        "target_2": "REAL",
        "price_at_signal": "REAL",
        "atr": "REAL",
        "rs_20d": "REAL",
        "pivot_level": "REAL",
        "trigger_level": "REAL",
        "pivot_distance_pct": "REAL",
        "extension_atr": "REAL",
        "reward_risk_now": "REAL",
        "short_ma_rising": "INTEGER",
        "tightening_to_short_ma": "INTEGER",
        "larger_ma_supportive": "INTEGER",
        "avwap_supportive": "INTEGER",
        "avwap_resistance": "INTEGER",
        "avwap_context": "TEXT",
        "active_avwap_anchors": "TEXT",
        "nearest_support_avwap_anchor": "TEXT",
        "nearest_resistance_avwap_anchor": "TEXT",
        "avwap_distance_pct": "REAL",
        "avwap_resistance_filter_flag": "INTEGER",
        "avwap_resistance_filter_reason": "TEXT",
        "avwap_resistance_anchor": "TEXT",
        "avwap_resistance_distance_pct": "REAL",
        "avwap_location_quality": "TEXT",
        "expansion_score": "REAL",
        "range_ratio": "REAL",
        "volume_ratio": "REAL",
        "atr_ratio": "REAL",
        "expansion_quality": "TEXT",
        "rsi_14": "REAL",
        "rsi_bucket": "TEXT",
        "rsi_trend": "TEXT",
        "overhead_supply_score": "REAL",
        "overhead_supply_detail": "TEXT",
        "regime": "TEXT",
        "event_risk": "INTEGER",
        "rvol": "REAL",
        "freshness_label": "TEXT",
        "freshness_minutes": "REAL",
        "actionability_label": "TEXT",
        "data_quality_score": "REAL",
        "run_mode": "TEXT",
        "calibration_provenance": "TEXT",
        "calibration_confidence": "TEXT",
        "slippage_est_bps": "REAL",
        "cost_dollars_est": "REAL",
        "net_rr_t1_est": "REAL",
        "triggered": "INTEGER",
        "trigger_date": "TEXT",
        "trigger_price": "REAL",
        "fwd_1d_ret": "REAL",
        "fwd_3d_ret": "REAL",
        "fwd_5d_ret": "REAL",
        "fwd_10d_ret": "REAL",
        "fwd_20d_ret": "REAL",
        "entry_model_date": "TEXT",
        "entry_model_price": "REAL",
        "stop_model_price": "REAL",
        "target_model_price": "REAL",
        "triggered_model": "INTEGER",
        "days_to_entry": "INTEGER",
        "days_to_stop": "INTEGER",
        "days_to_target": "INTEGER",
        "realized_r": "REAL",
        "outcome_r": "REAL",
        "outcome_status": "TEXT",
        "hit_target_1": "INTEGER",
        "hit_target_2": "INTEGER",
        "hit_pivot_r1": "INTEGER",
        "hit_pivot_r2": "INTEGER",
        "hit_pivot_r3": "INTEGER",
        "stop_hit": "INTEGER",
        "stop_before_target_1": "INTEGER",
        "target_1_before_stop": "INTEGER",
        "target_2_before_stop": "INTEGER",
        "target_3_before_stop": "INTEGER",
        "first_target_hit": "TEXT",
        "first_resistance": "TEXT",
        "first_support": "TEXT",
        "max_favorable_excursion_pct": "REAL",
        "max_adverse_excursion_pct": "REAL",
        "packet_json": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    }
    for column, col_type in desired.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {column} {col_type}")


def _ensure_trade_columns(conn: sqlite3.Connection) -> None:
    """Add new trade-management columns to an existing trades table."""
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(trades)").fetchall()
    }
    desired = {
        "partial_1_taken": "INTEGER DEFAULT 0",
        "partial_2_taken": "INTEGER DEFAULT 0",
        "current_stop": "REAL",
        "trailing_stop": "REAL",
    }
    for column, col_type in desired.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {column} {col_type}")

    conn.execute(
        """
        UPDATE trades
        SET
            partial_1_taken = COALESCE(partial_1_taken, 0),
            partial_2_taken = COALESCE(partial_2_taken, 0),
            current_stop = COALESCE(current_stop, stop_price)
        """
    )


def _ensure_backtest_event_columns(conn: sqlite3.Connection) -> None:
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(backtest_events)").fetchall()
    }
    desired = {
        "production_score": "REAL",
        "sizing_tier": "TEXT",
        "pivot_zone": "TEXT",
        "trigger_band": "TEXT",
        "breakout_band": "TEXT",
        "structural_band": "TEXT",
        "dominant_negative_flags": "TEXT",
        "interaction_cluster_flags": "TEXT",
        "readiness_rebalance_flags": "TEXT",
        "return_5d": "REAL",
        "return_10d": "REAL",
        "active_avwap_anchors": "TEXT",
        "nearest_support_avwap_anchor": "TEXT",
        "nearest_resistance_avwap_anchor": "TEXT",
        "avwap_distance_pct": "REAL",
        "avwap_resistance_filter_flag": "INTEGER",
        "avwap_resistance_filter_reason": "TEXT",
        "avwap_resistance_anchor": "TEXT",
        "avwap_resistance_distance_pct": "REAL",
        "avwap_location_quality": "TEXT",
        "expansion_score": "REAL",
        "range_ratio": "REAL",
        "volume_ratio": "REAL",
        "atr_ratio": "REAL",
        "expansion_quality": "TEXT",
        "rsi_14": "REAL",
        "rsi_bucket": "TEXT",
        "rsi_trend": "TEXT",
    }
    for column, col_type in desired.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE backtest_events ADD COLUMN {column} {col_type}")


def _coerce_bool(value):
    if value in (None, "", "nan"):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(bool(value))
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return 1
    if text in {"0", "0.0", "false", "no"}:
        return 0
    return None


def _coerce_float(value):
    if value in (None, "", "nan"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value):
    if value in (None, "", "nan"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def upsert_signal(row: dict, packet: dict = None) -> None:
    """Insert or update a signal call."""
    initialize()
    now = _now_iso()
    payload = {
        "signal_date": row.get("date"),
        "symbol": row.get("symbol"),
        "score": _coerce_float(row.get("score")),
        "quality": row.get("quality"),
        "idea_quality_score": _coerce_float(row.get("idea_quality_score")),
        "idea_quality": row.get("idea_quality"),
        "entry_timing_score": _coerce_float(row.get("entry_timing_score")),
        "entry_timing": row.get("entry_timing"),
        "action_bias": row.get("action_bias"),
        "setup_type": row.get("setup_type"),
        "setup_family": row.get("setup_family"),
        "setup_stage": row.get("setup_stage"),
        "setup_state": row.get("setup_state"),
        "trigger_type": row.get("trigger_type"),
        "triggered_now_bucket": row.get("triggered_now_bucket"),
        "structural_score": _coerce_float(row.get("structural_score")),
        "breakout_readiness_score": _coerce_float(row.get("breakout_readiness_score")),
        "trigger_readiness_score": _coerce_float(row.get("trigger_readiness_score")),
        "weekly_gate": _coerce_bool(row.get("weekly_gate")),
        "daily_gate": _coerce_bool(row.get("daily_gate")),
        "entry_low": _coerce_float(row.get("entry_low")),
        "entry_high": _coerce_float(row.get("entry_high")),
        "stop": _coerce_float(row.get("stop")),
        "target_1": _coerce_float(row.get("target_1")),
        "target_2": _coerce_float(row.get("target_2")),
        "price_at_signal": _coerce_float(row.get("price_at_signal")),
        "atr": _coerce_float(row.get("atr")),
        "rs_20d": _coerce_float(row.get("rs_20d")),
        "pivot_level": _coerce_float(row.get("pivot_level")),
        "trigger_level": _coerce_float(row.get("trigger_level")),
        "pivot_distance_pct": _coerce_float(row.get("pivot_distance_pct")),
        "extension_atr": _coerce_float(row.get("extension_atr")),
        "reward_risk_now": _coerce_float(row.get("reward_risk_now")),
        "short_ma_rising": _coerce_bool(row.get("short_ma_rising")),
        "tightening_to_short_ma": _coerce_bool(row.get("tightening_to_short_ma")),
        "larger_ma_supportive": _coerce_bool(row.get("larger_ma_supportive")),
        "avwap_supportive": _coerce_bool(row.get("avwap_supportive")),
        "avwap_resistance": _coerce_bool(row.get("avwap_resistance")),
        "avwap_context": row.get("avwap_context"),
        "active_avwap_anchors": row.get("active_avwap_anchors"),
        "nearest_support_avwap_anchor": row.get("nearest_support_avwap_anchor"),
        "nearest_resistance_avwap_anchor": row.get("nearest_resistance_avwap_anchor"),
        "avwap_distance_pct": _coerce_float(row.get("avwap_distance_pct")),
        "avwap_resistance_filter_flag": _coerce_bool(row.get("avwap_resistance_filter_flag")),
        "avwap_resistance_filter_reason": row.get("avwap_resistance_filter_reason"),
        "avwap_resistance_anchor": row.get("avwap_resistance_anchor"),
        "avwap_resistance_distance_pct": _coerce_float(row.get("avwap_resistance_distance_pct")),
        "avwap_location_quality": row.get("avwap_location_quality"),
        "expansion_score": _coerce_float(row.get("expansion_score")),
        "range_ratio": _coerce_float(row.get("range_ratio")),
        "volume_ratio": _coerce_float(row.get("volume_ratio")),
        "atr_ratio": _coerce_float(row.get("atr_ratio")),
        "expansion_quality": row.get("expansion_quality"),
        "rsi_14": _coerce_float(row.get("rsi_14")),
        "rsi_bucket": row.get("rsi_bucket"),
        "rsi_trend": row.get("rsi_trend"),
        "overhead_supply_score": _coerce_float(row.get("overhead_supply_score")),
        "overhead_supply_detail": row.get("overhead_supply_detail"),
        "regime": row.get("regime"),
        "event_risk": _coerce_bool(row.get("event_risk")),
        "rvol": _coerce_float(row.get("rvol")),
        "freshness_label": row.get("freshness_label"),
        "freshness_minutes": _coerce_float(row.get("freshness_minutes")),
        "actionability_label": row.get("actionability_label"),
        "data_quality_score": _coerce_float(row.get("data_quality_score")),
        "run_mode": row.get("run_mode"),
        "calibration_provenance": row.get("calibration_provenance"),
        "calibration_confidence": row.get("calibration_confidence"),
        "slippage_est_bps": _coerce_float(row.get("slippage_est_bps")),
        "cost_dollars_est": _coerce_float(row.get("cost_dollars_est")),
        "net_rr_t1_est": _coerce_float(row.get("net_rr_t1_est")),
        "triggered": _coerce_bool(row.get("triggered")),
        "trigger_date": row.get("trigger_date"),
        "trigger_price": _coerce_float(row.get("trigger_price")),
        "fwd_1d_ret": _coerce_float(row.get("fwd_1d_ret")),
        "fwd_3d_ret": _coerce_float(row.get("fwd_3d_ret")),
        "fwd_5d_ret": _coerce_float(row.get("fwd_5d_ret")),
        "fwd_10d_ret": _coerce_float(row.get("fwd_10d_ret")),
        "fwd_20d_ret": _coerce_float(row.get("fwd_20d_ret")),
        "entry_model_date": row.get("entry_model_date"),
        "entry_model_price": _coerce_float(row.get("entry_model_price")),
        "stop_model_price": _coerce_float(row.get("stop_model_price")),
        "target_model_price": _coerce_float(row.get("target_model_price")),
        "triggered_model": _coerce_bool(row.get("triggered_model")),
        "days_to_entry": _coerce_int(row.get("days_to_entry")),
        "days_to_stop": _coerce_int(row.get("days_to_stop")),
        "days_to_target": _coerce_int(row.get("days_to_target")),
        "realized_r": _coerce_float(row.get("realized_r")),
        "outcome_r": _coerce_float(row.get("outcome_r")),
        "outcome_status": row.get("outcome_status"),
        "hit_target_1": _coerce_bool(row.get("hit_target_1")),
        "hit_target_2": _coerce_bool(row.get("hit_target_2")),
        "hit_pivot_r1": _coerce_bool(row.get("hit_pivot_r1")),
        "hit_pivot_r2": _coerce_bool(row.get("hit_pivot_r2")),
        "hit_pivot_r3": _coerce_bool(row.get("hit_pivot_r3")),
        "stop_hit": _coerce_bool(row.get("stop_hit")),
        "stop_before_target_1": _coerce_bool(row.get("stop_before_target_1")),
        "target_1_before_stop": _coerce_bool(row.get("target_1_before_stop")),
        "target_2_before_stop": _coerce_bool(row.get("target_2_before_stop")),
        "target_3_before_stop": _coerce_bool(row.get("target_3_before_stop")),
        "first_target_hit": row.get("first_target_hit"),
        "first_resistance": row.get("first_resistance"),
        "first_support": row.get("first_support"),
        "max_favorable_excursion_pct": _coerce_float(row.get("max_favorable_excursion_pct")),
        "max_adverse_excursion_pct": _coerce_float(row.get("max_adverse_excursion_pct")),
        "packet_json": json.dumps(packet, default=str) if packet is not None else None,
        "created_at": now,
        "updated_at": now,
    }

    columns = list(payload.keys())
    update_columns = [c for c in columns if c not in {"signal_date", "symbol", "created_at"}]

    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO signals ({", ".join(columns)})
            VALUES ({", ".join(":" + c for c in columns)})
            ON CONFLICT(signal_date, symbol) DO UPDATE SET
                {", ".join(f"{c}=excluded.{c}" for c in update_columns)}
            """,
            payload,
        )


def insert_trade(row: dict) -> None:
    """Insert a trade entry."""
    initialize()
    now = _now_iso()
    payload = {
        "open_date": row.get("date"),
        "symbol": row.get("symbol"),
        "action": row.get("action"),
        "entry_price": _coerce_float(row.get("entry_price")),
        "stop_price": _coerce_float(row.get("stop_price")),
        "shares": _coerce_int(row.get("shares")),
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "exit_date": row.get("exit_date"),
        "exit_price": _coerce_float(row.get("exit_price")),
        "exit_reason": row.get("exit_reason"),
        "r_multiple": _coerce_float(row.get("r_multiple")),
        "pnl_dollars": _coerce_float(row.get("pnl_dollars")),
        "status": "closed" if row.get("exit_date") else "open",
        "partial_1_taken": 0,
        "partial_2_taken": 0,
        "current_stop": _coerce_float(row.get("stop_price")),
        "trailing_stop": None,
        "created_at": now,
        "updated_at": now,
    }

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                open_date, symbol, action, entry_price, stop_price, shares,
                reason, notes, exit_date, exit_price, exit_reason,
                r_multiple, pnl_dollars, status, partial_1_taken, partial_2_taken,
                current_stop, trailing_stop, created_at, updated_at
            )
            VALUES (
                :open_date, :symbol, :action, :entry_price, :stop_price, :shares,
                :reason, :notes, :exit_date, :exit_price, :exit_reason,
                :r_multiple, :pnl_dollars, :status, :partial_1_taken, :partial_2_taken,
                :current_stop, :trailing_stop, :created_at, :updated_at
            )
            """,
            payload,
        )


def close_trade(symbol: str, exit_date: str, exit_price: float, exit_reason: str,
                r_multiple: float, pnl_dollars: float, trade_date: str = None) -> bool:
    """Close the latest open trade for a symbol in the database."""
    initialize()
    with _connect() as conn:
        params = {"symbol": symbol}
        sql = """
            SELECT id
            FROM trades
            WHERE symbol = :symbol AND status = 'open'
        """
        if trade_date:
            sql += " AND open_date = :trade_date"
            params["trade_date"] = trade_date
        sql += " ORDER BY open_date DESC, id DESC LIMIT 1"
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return False

        conn.execute(
            """
            UPDATE trades
            SET exit_date = :exit_date,
                exit_price = :exit_price,
                exit_reason = :exit_reason,
                r_multiple = :r_multiple,
                pnl_dollars = :pnl_dollars,
                status = 'closed',
                updated_at = :updated_at
            WHERE id = :id
            """,
            {
                "id": row["id"],
                "exit_date": exit_date,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "r_multiple": r_multiple,
                "pnl_dollars": pnl_dollars,
                "updated_at": _now_iso(),
            },
        )
        return True


def sync_csv_to_db() -> None:
    """Import legacy CSV rows into SQLite when missing."""
    signals_path = cfg.DATA_DIR / "signals.csv"
    journal_path = cfg.DATA_DIR / "journal.csv"

    if signals_path.exists():
        df = pd.read_csv(signals_path)
        for row in df.to_dict("records"):
            upsert_signal(row)

    if journal_path.exists():
        df = pd.read_csv(journal_path)
        with _connect() as conn:
            existing = {
                (
                    r["open_date"], r["symbol"], r["action"],
                    r["entry_price"], r["stop_price"], r["shares"]
                )
                for r in conn.execute(
                    """
                    SELECT open_date, symbol, action, entry_price, stop_price, shares
                    FROM trades
                    """
                ).fetchall()
            }

        for row in df.to_dict("records"):
            key = (
                row.get("date"),
                row.get("symbol"),
                row.get("action"),
                _coerce_float(row.get("entry_price")),
                _coerce_float(row.get("stop_price")),
                _coerce_int(row.get("shares")),
            )
            if key in existing:
                continue
            insert_trade(row)


def get_open_trades() -> List[dict]:
    """Return all open trades as a list of dicts."""
    initialize()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM trades WHERE status = 'open' ORDER BY open_date ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def update_trade_stop(trade_id: int, new_stop: float, partial_1: bool = None, partial_2: bool = None) -> bool:
    """Update the trailing stop (and optionally partial-exit flags) for an open trade."""
    initialize()
    fields = ["current_stop = :stop", "trailing_stop = :stop", "updated_at = :ts"]
    params: dict = {"id": trade_id, "stop": new_stop, "ts": _now_iso()}

    if partial_1 is not None:
        fields.append("partial_1_taken = :p1")
        params["p1"] = int(partial_1)
    if partial_2 is not None:
        fields.append("partial_2_taken = :p2")
        params["p2"] = int(partial_2)

    with _connect() as conn:
        conn.execute(
            f"UPDATE trades SET {', '.join(fields)} WHERE id = :id",
            params,
        )
    return True


def save_portfolio_snapshot(exposure: dict) -> None:
    """Persist a portfolio exposure snapshot for trend analysis."""
    initialize()
    import json as _json
    now = _now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO portfolio_snapshots (
                snapshot_date, open_positions, total_dollar_exposure,
                net_beta_exposure, net_beta_pct, portfolio_beta,
                open_risk_dollars, open_risk_pct, vix_1pt_impact_dollars,
                max_single_position_pct, largest_sector_exposure,
                largest_sector_pct, snapshot_json, created_at
            ) VALUES (
                :snapshot_date, :open_positions, :total_dollar_exposure,
                :net_beta_exposure, :net_beta_pct, :portfolio_beta,
                :open_risk_dollars, :open_risk_pct, :vix_1pt_impact_dollars,
                :max_single_position_pct, :largest_sector_exposure,
                :largest_sector_pct, :snapshot_json, :created_at
            )
            """,
            {
                "snapshot_date": exposure.get("snapshot_date"),
                "open_positions": exposure.get("open_positions"),
                "total_dollar_exposure": exposure.get("total_dollar_exposure"),
                "net_beta_exposure": exposure.get("net_beta_exposure"),
                "net_beta_pct": exposure.get("net_beta_pct"),
                "portfolio_beta": exposure.get("portfolio_beta"),
                "open_risk_dollars": exposure.get("open_risk_dollars"),
                "open_risk_pct": exposure.get("open_risk_pct"),
                "vix_1pt_impact_dollars": exposure.get("vix_1pt_impact_dollars"),
                "max_single_position_pct": exposure.get("max_single_position_pct"),
                "largest_sector_exposure": exposure.get("largest_sector_exposure"),
                "largest_sector_pct": exposure.get("largest_sector_pct"),
                "snapshot_json": _json.dumps(exposure, default=str),
                "created_at": now,
            },
        )


def load_signal_packet(signal_date: str, symbol: str):
    """Load the stored packet JSON for a signal when available."""
    initialize()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT packet_json
            FROM signals
            WHERE signal_date = ? AND symbol = ?
            """,
            (signal_date, symbol),
        ).fetchone()
    if row is None or not row["packet_json"]:
        return None
    try:
        return json.loads(row["packet_json"])
    except json.JSONDecodeError:
        return None


def upsert_backtest_event(row: dict) -> None:
    initialize()
    now = _now_iso()
    payload = {
        "replay_mode": row.get("replay_mode"),
        "evaluation_date": row.get("evaluation_date"),
        "symbol": row.get("symbol"),
        "setup_state": row.get("setup_state"),
        "setup_family": row.get("setup_family"),
        "setup_stage": row.get("setup_stage"),
        "actionability_label": row.get("actionability_label"),
        "trigger_type": row.get("trigger_type"),
        "structural_score": _coerce_float(row.get("structural_score")),
        "breakout_readiness_score": _coerce_float(row.get("breakout_readiness_score")),
        "trigger_readiness_score": _coerce_float(row.get("trigger_readiness_score")),
        "pivot_level": _coerce_float(row.get("pivot_level")),
        "trigger_level": _coerce_float(row.get("trigger_level")),
        "production_score": _coerce_float(row.get("production_score")),
        "sizing_tier": row.get("sizing_tier"),
        "pivot_zone": row.get("pivot_zone"),
        "trigger_band": row.get("trigger_band"),
        "breakout_band": row.get("breakout_band"),
        "structural_band": row.get("structural_band"),
        "dominant_negative_flags": json.dumps(row.get("dominant_negative_flags", []), default=str),
        "interaction_cluster_flags": json.dumps(row.get("interaction_cluster_flags", []), default=str),
        "readiness_rebalance_flags": json.dumps(row.get("readiness_rebalance_flags", []), default=str),
        "pivot_position": row.get("pivot_position"),
        "pivot_distance_pct": _coerce_float(row.get("pivot_distance_pct")),
        "extension_atr": _coerce_float(row.get("extension_atr")),
        "reward_risk_now": _coerce_float(row.get("reward_risk_now")),
        "short_ma_rising": _coerce_bool(row.get("short_ma_rising")),
        "tightening_to_short_ma": _coerce_bool(row.get("tightening_to_short_ma")),
        "larger_ma_supportive": _coerce_bool(row.get("larger_ma_supportive")),
        "avwap_supportive": _coerce_bool(row.get("avwap_supportive")),
        "avwap_resistance": _coerce_bool(row.get("avwap_resistance")),
        "avwap_context": row.get("avwap_context"),
        "active_avwap_anchors": json.dumps(row.get("active_avwap_anchors", []), default=str),
        "nearest_support_avwap_anchor": row.get("nearest_support_avwap_anchor"),
        "nearest_resistance_avwap_anchor": row.get("nearest_resistance_avwap_anchor"),
        "avwap_distance_pct": _coerce_float(row.get("avwap_distance_pct")),
        "avwap_resistance_filter_flag": _coerce_bool(row.get("avwap_resistance_filter_flag")),
        "avwap_resistance_filter_reason": row.get("avwap_resistance_filter_reason"),
        "avwap_resistance_anchor": row.get("avwap_resistance_anchor"),
        "avwap_resistance_distance_pct": _coerce_float(row.get("avwap_resistance_distance_pct")),
        "avwap_location_quality": row.get("avwap_location_quality"),
        "expansion_score": _coerce_float(row.get("expansion_score")),
        "range_ratio": _coerce_float(row.get("range_ratio")),
        "volume_ratio": _coerce_float(row.get("volume_ratio")),
        "atr_ratio": _coerce_float(row.get("atr_ratio")),
        "expansion_quality": row.get("expansion_quality"),
        "rsi_14": _coerce_float(row.get("rsi_14")),
        "rsi_bucket": row.get("rsi_bucket"),
        "rsi_trend": row.get("rsi_trend"),
        "overhead_supply_score": _coerce_float(row.get("overhead_supply_score")),
        "contraction_score": _coerce_float(row.get("contraction_score")),
        "freshness_label": row.get("freshness_label"),
        "calibration_provenance": row.get("calibration_provenance"),
        "calibration_confidence": row.get("calibration_confidence"),
        "fwd_1d_ret": _coerce_float(row.get("fwd_1d_ret")),
        "fwd_3d_ret": _coerce_float(row.get("fwd_3d_ret")),
        "fwd_5d_ret": _coerce_float(row.get("fwd_5d_ret")),
        "fwd_10d_ret": _coerce_float(row.get("fwd_10d_ret")),
        "return_5d": _coerce_float(row.get("return_5d")),
        "return_10d": _coerce_float(row.get("return_10d")),
        "fwd_20d_ret": _coerce_float(row.get("fwd_20d_ret")),
        "max_favorable_excursion_pct": _coerce_float(row.get("max_favorable_excursion_pct")),
        "max_adverse_excursion_pct": _coerce_float(row.get("max_adverse_excursion_pct")),
        "realized_r": _coerce_float(row.get("realized_r")),
        "outcome_status": row.get("outcome_status"),
        "target_1_before_stop": _coerce_bool(row.get("target_1_before_stop")),
        "stop_before_target_1": _coerce_bool(row.get("stop_before_target_1")),
        "payload_json": json.dumps(row, default=str),
        "created_at": now,
        "updated_at": now,
    }
    columns = list(payload.keys())
    update_columns = [c for c in columns if c not in {"replay_mode", "evaluation_date", "symbol", "created_at"}]
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO backtest_events ({", ".join(columns)})
            VALUES ({", ".join(":" + c for c in columns)})
            ON CONFLICT(replay_mode, evaluation_date, symbol) DO UPDATE SET
                {", ".join(f"{c}=excluded.{c}" for c in update_columns)}
            """,
            payload,
        )


def load_backtest_events(replay_mode:Optional[str] = None) -> pd.DataFrame:
    initialize()
    query = "SELECT * FROM backtest_events"
    params: tuple = ()
    if replay_mode:
        query += " WHERE replay_mode = ?"
        params = (replay_mode,)
    with _connect() as conn:
        return pd.read_sql_query(query, conn, params=params)
