"""
Pivot-pass survivor diagnostics built strictly from existing packet outputs.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, List

from . import config as cfg
from . import scan_modes


ALLOWED_BANDS = {"favorable", "acceptable"}
FAILURE_GATES = [
    "dominant_negative",
    "sizing",
    "trigger_band",
    "breakout_band",
    "structural_band",
]


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _candidate_row(symbol: str, packet: dict) -> dict:
    score = packet.get("score", {})
    production = score.get("production_promotion", {})
    band_profile = production.get("band_profile", {})
    sizing = packet.get("position_sizing", {})
    return {
        "symbol": symbol,
        "setup_state": str(score.get("setup_state", "")),
        "production_score": _safe_float(production.get("production_score"), 0.0),
        "pivot_zone": str(production.get("pivot_zone") or ""),
        "sizing_tier": str(sizing.get("sizing_tier") or "none"),
        "dominant_negative_flags": list(production.get("dominant_negative_flags", [])),
        "trigger_band": str((band_profile.get("trigger_readiness_score") or {}).get("label", "")),
        "breakout_band": str((band_profile.get("breakout_readiness_score") or {}).get("label", "")),
        "structural_band": str((band_profile.get("structural_score") or {}).get("label", "")),
        "interaction_cluster_flags": list(production.get("interaction_cluster_flags", [])),
        "readiness_rebalance_flags": list(production.get("readiness_rebalance_flags", [])),
        "trigger_readiness_score": _safe_float(score.get("trigger_readiness_score"), 0.0),
        "breakout_readiness_score": _safe_float(score.get("breakout_readiness_score"), 0.0),
    }


def _failed_gates(row: dict) -> List[str]:
    failed: List[str] = []
    if row["dominant_negative_flags"]:
        failed.append("dominant_negative")
    if row["sizing_tier"] == "none":
        failed.append("sizing")
    if row["trigger_band"] not in ALLOWED_BANDS:
        failed.append("trigger_band")
    if row["breakout_band"] not in ALLOWED_BANDS:
        failed.append("breakout_band")
    if row["structural_band"] not in ALLOWED_BANDS:
        failed.append("structural_band")
    return failed


def collect_pivot_pass_rows(context: dict) -> List[dict]:
    packets_map = context.get("packets", {})
    rows: List[dict] = []
    for symbol in cfg.WATCHLIST:
        packet = packets_map.get(symbol)
        if not packet:
            continue
        row = _candidate_row(symbol, packet)
        if row["pivot_zone"] not in {"prime", "near"}:
            continue
        row["failed_gates"] = _failed_gates(row)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            -row["production_score"],
            -row["breakout_readiness_score"],
            -row["trigger_readiness_score"],
            row["symbol"],
        )
    )
    return rows


def analyze_pivot_pass(rows: List[dict]) -> dict:
    state_counts = dict(sorted(Counter(row["setup_state"] for row in rows).items()))
    failure_counts = {gate: sum(1 for row in rows if gate in row["failed_gates"]) for gate in FAILURE_GATES}
    combo_counts = Counter(tuple(row["failed_gates"]) for row in rows if row["failed_gates"])
    combos = [
        {"failures": list(combo), "count": count}
        for combo, count in combo_counts.most_common(5)
    ]
    near_misses = []
    for row in rows:
        if row["setup_state"] not in {"TRIGGER_WATCH", "FORMING"}:
            continue
        if row["dominant_negative_flags"]:
            continue
        non_dominant_failures = [gate for gate in row["failed_gates"] if gate != "dominant_negative"]
        if len(non_dominant_failures) == 1:
            near_misses.append(row)
    near_misses.sort(
        key=lambda row: (
            -row["production_score"],
            -row["breakout_readiness_score"],
            -row["trigger_readiness_score"],
            row["symbol"],
        )
    )
    return {
        "total_pivot_pass": len(rows),
        "state_distribution": state_counts,
        "failure_counts": failure_counts,
        "top_failure_combinations": combos,
        "near_misses": near_misses[:10],
        "top_candidates": rows[:10],
    }


def _format_candidate(row: dict) -> str:
    return (
        f"{row['symbol']} "
        f"(state={row['setup_state']}, score={round(float(row['production_score']), 1)}, "
        f"pivot_zone={row['pivot_zone']}, trigger_band={row['trigger_band']}, "
        f"breakout_band={row['breakout_band']}, structural_band={row['structural_band']}, "
        f"dominant_negatives={row['dominant_negative_flags']}, sizing_tier={row['sizing_tier']})"
    )


def render_report(summary: dict) -> str:
    lines = [
        "=" * 60,
        "PIVOT-PASS SURVIVOR ANALYSIS",
        "=" * 60,
        "",
        f"TOTAL PIVOT-PASS: {summary['total_pivot_pass']}",
        "",
        "-" * 60,
        "STATE DISTRIBUTION",
        "-" * 60,
    ]
    for state, count in summary["state_distribution"].items():
        lines.append(f"{state}: {count}")
    lines.extend(
        [
            "",
            "-" * 60,
            "FAILURE BREAKDOWN (PIVOT-PASS ONLY)",
            "-" * 60,
        ]
    )
    for gate in FAILURE_GATES:
        lines.append(f"{gate}: {summary['failure_counts'][gate]}")
    lines.extend(
        [
            "",
            "-" * 60,
            "TOP FAILURE COMBINATIONS",
            "-" * 60,
        ]
    )
    if not summary["top_failure_combinations"]:
        lines.append("None")
    else:
        for item in summary["top_failure_combinations"]:
            label = " + ".join(item["failures"]) if item["failures"] else "none"
            lines.append(f"{label} -> {item['count']}")
    lines.extend(
        [
            "",
            "-" * 60,
            "NEAR MISSES (ONLY 1 FAILURE)",
            "-" * 60,
        ]
    )
    if not summary["near_misses"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_candidate(row)}" for row in summary["near_misses"])
    lines.extend(
        [
            "",
            "-" * 60,
            "TOP 10 PIVOT-PASS CANDIDATES",
            "-" * 60,
        ]
    )
    if not summary["top_candidates"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_candidate(row)}" for row in summary["top_candidates"])
    return "\n".join(lines)


def _report_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"pivot_pass_analysis_{date.today().isoformat()}.txt"


def run_pivot_pass_analysis(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force)
    rows = collect_pivot_pass_rows(context)
    summary = analyze_pivot_pass(rows)
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
