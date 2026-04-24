"""
Historical path analysis for NEAR ACTION entries.

This report reads persisted walk-forward rows only. It does not rebuild
packets, rescore symbols, or infer alternate state transitions.
"""
from __future__ import annotations

import json
import math
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import config as cfg
from .near_action_analysis import REQUIRED_DECISION_FIELDS


ACTIONABLE_STATES = {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM"}


def _complete_backtest_paths() -> List[Path]:
    paths = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    complete = []
    for path in paths:
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(rows, list) and rows and REQUIRED_DECISION_FIELDS.issubset(set(rows[0].keys())):
            complete.append(path)
    if not complete:
        raise FileNotFoundError("No complete backtest_walkforward_*.json reports with decision-layer fields found")
    return complete


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _safe_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _as_list(value: Any) -> List[str]:
    if _is_missing(value):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if not _is_missing(item)]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                return [str(item) for item in decoded if not _is_missing(item)]
        except json.JSONDecodeError:
            pass
        return [part.strip() for part in text.split(",") if part.strip()]
    return [str(value)]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not math.isnan(float(value)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes"}


def _load_rows() -> List[dict]:
    rows = []
    for path in _complete_backtest_paths():
        for raw in json.loads(path.read_text(encoding="utf-8")):
            if isinstance(raw, dict):
                rows.append(_normalize_row(raw, path))
    rows.sort(key=lambda row: (row["symbol"], row["date"]))
    return rows


def _normalize_row(raw: dict, path: Path) -> dict:
    return {
        "symbol": str(raw.get("symbol", "")),
        "date": str(raw.get("evaluation_date") or raw.get("date") or ""),
        "setup_state": str(raw.get("setup_state") or ""),
        "production_score": _safe_float(raw.get("production_score")),
        "sizing_tier": str(raw.get("sizing_tier") or ""),
        "pivot_zone": str(raw.get("pivot_zone") or ""),
        "trigger_band": str(raw.get("trigger_band") or ""),
        "breakout_band": str(raw.get("breakout_band") or ""),
        "structural_band": str(raw.get("structural_band") or ""),
        "dominant_negative_flags": _as_list(raw.get("dominant_negative_flags")),
        "interaction_cluster_flags": _as_list(raw.get("interaction_cluster_flags")),
        "readiness_rebalance_flags": _as_list(raw.get("readiness_rebalance_flags")),
        "realized_r": _safe_float(raw.get("realized_r")),
        "return_5d": _safe_float(raw.get("return_5d", raw.get("fwd_5d_ret"))),
        "return_10d": _safe_float(raw.get("return_10d", raw.get("fwd_10d_ret"))),
        "target_1_before_stop": _truthy(raw.get("target_1_before_stop")),
        "hit_target_1": _truthy(raw.get("hit_target_1")),
        "max_favorable_excursion_pct": _safe_float(raw.get("max_favorable_excursion_pct")),
        "max_adverse_excursion_pct": _safe_float(raw.get("max_adverse_excursion_pct")),
        "source_path": str(path),
    }


def _is_near_action(row: dict) -> bool:
    if row["dominant_negative_flags"]:
        return False
    return row["sizing_tier"] == "watchlist" or row["setup_state"] == "TRIGGER_WATCH"


def _entry_rows(rows: List[dict]) -> List[dict]:
    by_symbol: Dict[str, List[dict]] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row)
    entries = []
    for symbol_rows in by_symbol.values():
        symbol_rows.sort(key=lambda row: row["date"])
        for row in symbol_rows:
            if _is_near_action(row):
                entries.append(row)
                break
    return sorted(entries, key=lambda row: (row["date"], row["symbol"]))


def _forward_window(rows: List[dict], entry: dict, days: int = 10) -> List[dict]:
    symbol_rows = [row for row in rows if row["symbol"] == entry["symbol"]]
    symbol_rows.sort(key=lambda row: row["date"])
    for idx, row in enumerate(symbol_rows):
        if row["date"] == entry["date"]:
            return symbol_rows[idx + 1 : idx + 1 + days]
    return []


def _classify_path(entry: dict, forward_rows: List[dict]) -> str:
    if any(row["setup_state"] in ACTIONABLE_STATES for row in forward_rows):
        return "PROMOTED"
    if any(row["target_1_before_stop"] or row["hit_target_1"] for row in [entry] + forward_rows):
        return "MISSED BREAKOUT"
    outcome = entry["realized_r"]
    if outcome is not None and outcome < 0:
        return "FAILED"
    return_10d = entry["return_10d"]
    if return_10d is not None and return_10d < 0:
        return "FAILED"
    return "STAGNANT"


def _blockers(forward_rows: List[dict]) -> List[str]:
    blockers = []
    rows = forward_rows or []
    if any(row["trigger_band"] != "favorable" for row in rows):
        blockers.append("trigger_band_not_favorable")
    if any(row["breakout_band"] != "favorable" for row in rows):
        blockers.append("breakout_band_not_favorable")
    if any(row["structural_band"] != "favorable" for row in rows):
        blockers.append("structural_band_not_favorable")
    if any(row["dominant_negative_flags"] for row in rows):
        blockers.append("dominant_negative_appeared")
    if any(row["pivot_zone"] == "far_below" for row in rows):
        blockers.append("pivot_zone_deteriorated")
    if any(row["sizing_tier"] == "none" for row in rows):
        blockers.append("sizing_remained_none")
    if any(row["setup_state"] in {"FORMING", "FAILED"} for row in rows):
        blockers.append("state_stayed_forming_or_failed")
    return blockers


def _path_string(rows: List[dict]) -> str:
    if not rows:
        return "no later rows"
    return " -> ".join(
        f"{row['date']}:{row['setup_state']}/{row['sizing_tier']}/{row['pivot_zone']}"
        for row in rows
    )


def analyze_near_action_paths() -> dict:
    rows = _load_rows()
    entries = _entry_rows(rows)
    analyzed = []
    for entry in entries:
        forward_rows = _forward_window(rows, entry)
        path = _classify_path(entry, forward_rows)
        blockers = [] if path == "PROMOTED" else _blockers(forward_rows)
        analyzed.append(
            {
                "entry": entry,
                "forward_rows": forward_rows,
                "path": path,
                "blockers": blockers,
            }
        )
    outcome_counts = Counter(item["path"] for item in analyzed)
    blocker_counts = Counter(blocker for item in analyzed for blocker in item["blockers"])
    winner_blockers = Counter(
        blocker
        for item in analyzed
        if item["path"] == "MISSED BREAKOUT"
        for blocker in item["blockers"]
    )
    return {
        "source_paths": [str(path) for path in _complete_backtest_paths()],
        "total_entries": len(analyzed),
        "outcome_counts": dict(outcome_counts),
        "blocker_counts": dict(blocker_counts),
        "winner_blockers": dict(winner_blockers),
        "examples": analyzed[:5],
    }


def _format_counts(counts: dict, labels: List[str]) -> List[str]:
    return [f"{label}: {counts.get(label, 0)}" for label in labels]


def render_report(summary: dict) -> str:
    blocker_counts = summary["blocker_counts"]
    top_winner_blockers = sorted(
        summary["winner_blockers"].items(),
        key=lambda item: (-item[1], item[0]),
    )
    key_insight = "None"
    if top_winner_blockers:
        key_insight = ", ".join(f"{name} ({count})" for name, count in top_winner_blockers[:3])

    lines = [
        "=" * 60,
        "NEAR ACTION PATH ANALYSIS",
        "=" * 60,
        f"SOURCE: {', '.join(summary.get('source_paths', []))}",
        f"TOTAL NEAR ACTION ENTRIES: {summary.get('total_entries')}",
        "",
        "-" * 60,
        "OUTCOME DISTRIBUTION",
        "-" * 60,
        *_format_counts(summary["outcome_counts"], ["PROMOTED", "MISSED BREAKOUT", "FAILED", "STAGNANT"]),
        "",
        "-" * 60,
        "PROMOTION BLOCKERS",
        "-" * 60,
        f"trigger_band failures: {blocker_counts.get('trigger_band_not_favorable', 0)}",
        f"breakout_band failures: {blocker_counts.get('breakout_band_not_favorable', 0)}",
        f"structure failures: {blocker_counts.get('structural_band_not_favorable', 0)}",
        f"dominant negatives: {blocker_counts.get('dominant_negative_appeared', 0)}",
        f"pivot deterioration: {blocker_counts.get('pivot_zone_deteriorated', 0)}",
        f"sizing remained none: {blocker_counts.get('sizing_remained_none', 0)}",
        f"state stayed FORMING/FAILED: {blocker_counts.get('state_stayed_forming_or_failed', 0)}",
        "",
        "-" * 60,
        "KEY INSIGHT",
        "-" * 60,
        f"Top reason(s) winners failed to become ACTIONABLE: {key_insight}",
        "",
        "-" * 60,
        "EXAMPLES",
        "-" * 60,
    ]
    if not summary["examples"]:
        lines.append("None")
    for item in summary["examples"]:
        entry = item["entry"]
        lines.extend(
            [
                f"{entry['symbol']}:",
                f"initial state: {entry['date']} {entry['setup_state']} / {entry['sizing_tier']} / {entry['pivot_zone']}",
                f"forward path: {_path_string(item['forward_rows'])}",
                f"outcome: {item['path']} (realized_R={entry.get('realized_r')}, return_10d={entry.get('return_10d')})",
                f"blocker: {', '.join(item['blockers']) if item['blockers'] else '--'}",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def _output_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"near_action_path_{date.today().isoformat()}.txt"


def run_near_action_path_analysis(save: bool = True) -> dict:
    summary = analyze_near_action_paths()
    report_text = render_report(summary)
    print(report_text)
    output_path: Optional[Path] = None
    if save:
        output_path = _output_path()
        output_path.write_text(report_text, encoding="utf-8")
    return {
        "summary": summary,
        "report_text": report_text,
        "output_path": str(output_path) if output_path else None,
    }
