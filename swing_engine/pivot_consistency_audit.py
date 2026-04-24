"""
Pivot consistency audit built from existing packet outputs.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from typing import List, Optional

from . import config as cfg
from . import scan_modes


def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _row(symbol: str, packet: dict) -> dict:
    score = packet.get("score", {})
    production = score.get("production_promotion", {})
    pivot_position = score.get("pivot_position", {}) or packet.get("breakout_features", {}).get("pivot_position", {}) or {}
    return {
        "symbol": symbol,
        "setup_state": str(score.get("setup_state", "")),
        "production_score": _safe_float(production.get("production_score"), 0.0) or 0.0,
        "pivot_position": str(pivot_position.get("classification") or "unavailable"),
        "pivot_distance_pct": _safe_float(pivot_position.get("distance_pct")),
        "pivot_zone": str(production.get("pivot_zone") or ""),
        "dominant_negative_flags": list(production.get("dominant_negative_flags", [])),
    }


def collect_rows(context: dict) -> List[dict]:
    packets_map = context.get("packets", {})
    rows: List[dict] = []
    for symbol in cfg.WATCHLIST:
        packet = packets_map.get(symbol)
        if not packet:
            continue
        row = _row(symbol, packet)
        inconsistent = (
            (row["pivot_position"] == "at_pivot" and row["pivot_zone"] == "far_below")
            or (row["pivot_position"] == "below_pivot_but_near" and row["pivot_zone"] == "far_below")
        )
        far_flag_inconsistent = (
            "far_from_pivot" in row["dominant_negative_flags"]
            and row["pivot_position"] in {"at_pivot", "below_pivot_but_near"}
            and row["pivot_zone"] != "far_below"
        )
        row["pivot_inconsistent"] = inconsistent
        row["far_flag_inconsistent"] = far_flag_inconsistent
        rows.append(row)
    return rows


def _format_row(row: dict) -> str:
    return (
        f"{row['symbol']} "
        f"(state={row['setup_state']}, score={round(float(row['production_score']), 1)}, "
        f"pivot_position={row['pivot_position']}, pivot_zone={row['pivot_zone']}, "
        f"pivot_distance_pct={row['pivot_distance_pct']}, "
        f"dominant_negatives={row['dominant_negative_flags']})"
    )


def analyze(rows: List[dict]) -> dict:
    inconsistent_rows = [row for row in rows if row["pivot_inconsistent"]]
    far_flag_inconsistent_rows = [row for row in rows if row["far_flag_inconsistent"]]
    pivot_position_counts = dict(sorted(Counter(row["pivot_position"] for row in rows).items()))
    pivot_zone_counts = dict(sorted(Counter(row["pivot_zone"] for row in rows).items()))
    far_from_pivot_count = sum(1 for row in rows if "far_from_pivot" in row["dominant_negative_flags"])
    return {
        "total_symbols": len(rows),
        "pivot_position_counts": pivot_position_counts,
        "pivot_zone_counts": pivot_zone_counts,
        "inconsistent_pivot_rows_count": len(inconsistent_rows),
        "inconsistent_examples": inconsistent_rows[:10],
        "far_from_pivot_count": far_from_pivot_count,
        "far_flag_inconsistent_count": len(far_flag_inconsistent_rows),
        "far_flag_inconsistent_examples": far_flag_inconsistent_rows[:10],
    }


def render_report(summary: dict) -> str:
    lines = [
        "=" * 60,
        "PIVOT CONSISTENCY AUDIT",
        "=" * 60,
        "",
        f"TOTAL SYMBOLS: {summary['total_symbols']}",
        f"pivot_position counts: {summary['pivot_position_counts']}",
        f"pivot_zone counts: {summary['pivot_zone_counts']}",
        f"inconsistent_pivot_rows count: {summary['inconsistent_pivot_rows_count']}",
        f"far_from_pivot dominant-negative count: {summary['far_from_pivot_count']}",
        f"far_from_pivot inconsistent count: {summary['far_flag_inconsistent_count']}",
        "",
        "inconsistent pivot rows:",
    ]
    if not summary["inconsistent_examples"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_row(row)}" for row in summary["inconsistent_examples"])
    lines.append("far_from_pivot inconsistent rows:")
    if not summary["far_flag_inconsistent_examples"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_row(row)}" for row in summary["far_flag_inconsistent_examples"])
    return "\n".join(lines)


def _report_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"pivot_consistency_audit_{date.today().isoformat()}.txt"


def run_pivot_consistency_audit(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force)
    rows = collect_rows(context)
    summary = analyze(rows)
    report_text = render_report(summary)
    print(report_text)
    output_path = None
    if save:
        output_path = _report_path()
        output_path.write_text(report_text, encoding="utf-8")
    return {
        "rows": rows,
        "summary": summary,
        "report_text": report_text,
        "output_path": str(output_path) if output_path else None,
    }
