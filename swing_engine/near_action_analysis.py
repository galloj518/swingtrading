"""
Backtest-only evaluation of NEAR ACTION conversion and outcomes.

This module reads persisted walk-forward JSON reports and does not rebuild
signals, rescore packets, or infer missing live-only metadata.
"""
from __future__ import annotations

import json
import math
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from . import config as cfg


ACTIONABLE_STATES = {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM"}
REQUIRED_DECISION_FIELDS = {
    "sizing_tier",
    "pivot_zone",
    "trigger_band",
    "breakout_band",
    "structural_band",
    "dominant_negative_flags",
    "interaction_cluster_flags",
    "readiness_rebalance_flags",
    "production_score",
}


def _backtest_paths() -> List[Path]:
    paths = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    if not paths:
        raise FileNotFoundError("No reports/backtests/backtest_walkforward_*.json files found")
    complete_paths = []
    for path in paths:
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(rows, list) or not rows:
            continue
        if REQUIRED_DECISION_FIELDS.issubset(set(rows[0].keys())):
            complete_paths.append(path)
    return complete_paths or [paths[-1]]


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


def _load_rows(path: Optional[Path] = None) -> List[dict]:
    report_paths = [path] if path else _backtest_paths()
    rows = []
    for report_path in report_paths:
        data = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"Expected list rows in {report_path}")
        for raw in data:
            if isinstance(raw, dict):
                row = dict(raw)
                row["_source_path"] = str(report_path)
                rows.append(row)
    return rows


def _candidate_row(row: dict) -> dict:
    return {
        "symbol": str(row.get("symbol", "")),
        "date": str(row.get("evaluation_date") or row.get("date") or ""),
        "setup_state": str(row.get("setup_state") or ""),
        "sizing_tier": str(row.get("sizing_tier") or ""),
        "pivot_zone": str(row.get("pivot_zone") or "unavailable"),
        "trigger_band": str(row.get("trigger_band") or "unavailable"),
        "breakout_band": str(row.get("breakout_band") or "unavailable"),
        "structural_band": str(row.get("structural_band") or "unavailable"),
        "interaction_cluster_flags": _as_list(row.get("interaction_cluster_flags")),
        "dominant_negative_flags": _as_list(row.get("dominant_negative_flags")),
        "production_score": _safe_float(row.get("production_score", row.get("score"))),
        "realized_r": _safe_float(row.get("realized_r", row.get("outcome_r"))),
        "return_5d": _safe_float(row.get("return_5d", row.get("fwd_5d_ret"))),
        "return_10d": _safe_float(row.get("return_10d", row.get("fwd_10d_ret"))),
    }


def _is_near_action(row: dict) -> bool:
    if row["dominant_negative_flags"]:
        return False
    return row["sizing_tier"] == "watchlist" or row["setup_state"] == "TRIGGER_WATCH"


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return round(sum(cleaned) / len(cleaned), 4)


def _median(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return round(float(median(cleaned)), 4)


def _metrics(rows: List[dict]) -> dict:
    realized = [row["realized_r"] for row in rows if row["realized_r"] is not None]
    wins = [value > 0 for value in realized]
    return {
        "sample_size": len(rows),
        "avg_realized_R": _avg(realized),
        "median_realized_R": _median(realized),
        "win_rate": round(sum(wins) / len(wins), 4) if wins else None,
        "avg_return_5d": _avg(row["return_5d"] for row in rows),
        "avg_return_10d": _avg(row["return_10d"] for row in rows),
    }


def _group_metrics(rows: List[dict], field: str) -> Dict[str, dict]:
    groups: Dict[str, List[dict]] = {}
    for row in rows:
        groups.setdefault(str(row.get(field) or "unavailable"), []).append(row)
    return {name: _metrics(group_rows) for name, group_rows in sorted(groups.items())}


def _cluster_metrics(rows: List[dict]) -> Dict[str, dict]:
    groups = {"has_clusters": [], "no_clusters": []}
    for row in rows:
        key = "has_clusters" if row["interaction_cluster_flags"] else "no_clusters"
        groups[key].append(row)
    return {name: _metrics(group_rows) for name, group_rows in groups.items()}


def _conversion_rows(rows: List[dict], near_rows: List[dict]) -> List[dict]:
    by_symbol: Dict[str, List[dict]] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row)
    for symbol_rows in by_symbol.values():
        symbol_rows.sort(key=lambda item: item["date"])

    converted_rows = []
    for row in near_rows:
        later_rows = [
            candidate
            for candidate in by_symbol.get(row["symbol"], [])
            if candidate["date"] > row["date"]
        ]
        converted = any(candidate["setup_state"] in ACTIONABLE_STATES for candidate in later_rows)
        converted_rows.append({**row, "converted_to_actionable": converted})
    return converted_rows


def _format_metric_block(metrics: dict) -> List[str]:
    return [
        f"sample size: {metrics.get('sample_size')}",
        f"avg R: {metrics.get('avg_realized_R')}",
        f"median R: {metrics.get('median_realized_R')}",
        f"win rate: {_pct(metrics.get('win_rate'))}",
        f"avg return 5d: {metrics.get('avg_return_5d')}",
        f"avg return 10d: {metrics.get('avg_return_10d')}",
    ]


def _pct(value: Optional[float]) -> str:
    if value is None:
        return "None"
    return f"{round(value * 100, 1)}%"


def analyze_near_action(path: Optional[Path] = None) -> dict:
    raw_rows = _load_rows(path)
    rows = [_candidate_row(row) for row in raw_rows]
    near_rows = [row for row in rows if _is_near_action(row)]
    other_rows = [row for row in rows if row not in near_rows]
    converted_rows = _conversion_rows(rows, near_rows)
    converted = [row for row in converted_rows if row["converted_to_actionable"]]
    not_converted = [row for row in converted_rows if not row["converted_to_actionable"]]

    missing_fields = {
        field: sum(1 for row in rows if row.get(field) == "unavailable" or row.get(field) == "")
        for field in ("sizing_tier", "pivot_zone", "trigger_band", "breakout_band", "structural_band")
    }

    return {
        "source_path": str(path) if path else ", ".join(str(item) for item in _backtest_paths()),
        "total_rows": len(rows),
        "near_action": _metrics(near_rows),
        "baseline_all_other": _metrics(other_rows),
        "subgroups": {
            "pivot_zone": _group_metrics(near_rows, "pivot_zone"),
            "trigger_band": _group_metrics(near_rows, "trigger_band"),
            "breakout_band": _group_metrics(near_rows, "breakout_band"),
            "structural_band": _group_metrics(near_rows, "structural_band"),
            "interaction_cluster_flags": _cluster_metrics(near_rows),
        },
        "conversion": {
            "conversion_count": len(converted),
            "conversion_rate": round(len(converted) / len(converted_rows), 4) if converted_rows else None,
            "converted": _metrics(converted),
            "not_converted": _metrics(not_converted),
        },
        "near_action_state_counts": dict(Counter(row["setup_state"] for row in near_rows)),
        "missing_field_counts": missing_fields,
    }


def _format_group(title: str, groups: Dict[str, dict]) -> List[str]:
    lines = [f"{title}:"]
    if not groups:
        lines.append("  None")
        return lines
    for name, metrics in groups.items():
        lines.append(
            f"  {name}: n={metrics.get('sample_size')}, avg_R={metrics.get('avg_realized_R')}, "
            f"win_rate={_pct(metrics.get('win_rate'))}, avg_5d={metrics.get('avg_return_5d')}"
        )
    return lines


def render_report(summary: dict) -> str:
    near = summary["near_action"]
    baseline = summary["baseline_all_other"]
    conversion = summary["conversion"]
    lines = [
        "=" * 60,
        "NEAR ACTION CONVERSION ANALYSIS",
        "=" * 60,
        f"SOURCE: {summary.get('source_path')}",
        f"SAMPLE SIZE: {near.get('sample_size')}",
        "",
        "-" * 60,
        "PERFORMANCE",
        "-" * 60,
        *_format_metric_block(near),
        "",
        "-" * 60,
        "BASELINE COMPARISON",
        "-" * 60,
        "NEAR ACTION:",
        *_format_metric_block(near),
        "",
        "ALL OTHER:",
        *_format_metric_block(baseline),
        "",
        "-" * 60,
        "SUBGROUP INSIGHTS",
        "-" * 60,
        *_format_group("pivot_zone", summary["subgroups"]["pivot_zone"]),
        *_format_group("trigger_band", summary["subgroups"]["trigger_band"]),
        *_format_group("breakout_band", summary["subgroups"]["breakout_band"]),
        *_format_group("structural_band", summary["subgroups"]["structural_band"]),
        *_format_group("clusters", summary["subgroups"]["interaction_cluster_flags"]),
        "",
        "-" * 60,
        "CONVERSION",
        "-" * 60,
        f"conversion rate to ACTIONABLE: {_pct(conversion.get('conversion_rate'))}",
        f"conversion count: {conversion.get('conversion_count')}",
        f"avg R (converted): {conversion.get('converted', {}).get('avg_realized_R')}",
        f"avg R (not converted): {conversion.get('not_converted', {}).get('avg_realized_R')}",
        "",
        "-" * 60,
        "DATA AVAILABILITY",
        "-" * 60,
        f"near action state counts: {summary.get('near_action_state_counts')}",
        f"missing field counts: {summary.get('missing_field_counts')}",
    ]
    return "\n".join(lines)


def _output_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"near_action_analysis_{date.today().isoformat()}.txt"


def run_near_action_analysis(save: bool = True) -> dict:
    summary = analyze_near_action()
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
