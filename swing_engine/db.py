"""
SQLite persistence for signal calls and executed trades.

CSV files remain available for portability, but the database acts as the
durable store the app can update over time.
"""
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
                regime TEXT,
                event_risk INTEGER,
                rvol REAL,
                triggered INTEGER,
                trigger_date TEXT,
                trigger_price REAL,
                fwd_1d_ret REAL,
                fwd_3d_ret REAL,
                fwd_5d_ret REAL,
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
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signals_symbol_date
            ON signals(symbol, signal_date);

            CREATE INDEX IF NOT EXISTS idx_trades_symbol_status
            ON trades(symbol, status, open_date);
            """
        )
        _ensure_signal_columns(conn)

    _INITIALIZED = True
    sync_csv_to_db()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_signal_columns(conn: sqlite3.Connection) -> None:
    """Add new learning columns to an existing signals table."""
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(signals)").fetchall()
    }
    desired = {
        "idea_quality_score": "REAL",
        "idea_quality": "TEXT",
        "entry_timing_score": "REAL",
        "entry_timing": "TEXT",
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
    }
    for column, col_type in desired.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {column} {col_type}")


def _coerce_bool(value):
    if value in (None, "", "nan"):
        return None
    if isinstance(value, bool):
        return int(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return 1
    if text in {"0", "false", "no"}:
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
        "regime": row.get("regime"),
        "event_risk": _coerce_bool(row.get("event_risk")),
        "rvol": _coerce_float(row.get("rvol")),
        "triggered": _coerce_bool(row.get("triggered")),
        "trigger_date": row.get("trigger_date"),
        "trigger_price": _coerce_float(row.get("trigger_price")),
        "fwd_1d_ret": _coerce_float(row.get("fwd_1d_ret")),
        "fwd_3d_ret": _coerce_float(row.get("fwd_3d_ret")),
        "fwd_5d_ret": _coerce_float(row.get("fwd_5d_ret")),
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
        "created_at": now,
        "updated_at": now,
    }

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                open_date, symbol, action, entry_price, stop_price, shares,
                reason, notes, exit_date, exit_price, exit_reason,
                r_multiple, pnl_dollars, status, created_at, updated_at
            )
            VALUES (
                :open_date, :symbol, :action, :entry_price, :stop_price, :shares,
                :reason, :notes, :exit_date, :exit_price, :exit_reason,
                :r_multiple, :pnl_dollars, :status, :created_at, :updated_at
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
