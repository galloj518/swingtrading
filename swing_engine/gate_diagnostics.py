"""
Gate diagnostics report built strictly from existing packet outputs.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, List

from . import config as cfg
from . import scan_modes


GATE_STATE = "state_gate"
GATE_SIZING = "sizing_gate"
GATE_DOMINANT_NEGATIVE = "dominant_negative_gate"
GATE_PIVOT = "pivot_gate"
GATE_TRIGGER = "trigger_readiness_gate"
GATE_BREAKOUT = "breakout_readiness_gate"
GATE_STRUCTURE = "structure_gate"

GATE_ORDER = [
    GATE_STATE,
    GATE_SIZING,
    GATE_DOMINANT_NEGATIVE,
    GATE_PIVOT,
    GATE_TRIGGER,
    GATE_BREAKOUT,
    GATE_STRUCTURE,
]

GATE_TITLES = {
    GATE_STATE: "STATE GATE",
    GATE_SIZING: "SIZING GATE",
    GATE_DOMINANT_NEGATIVE: "DOMINANT NEGATIVE GATE",
    GATE_PIVOT: "PIVOT GATE",
    GATE_TRIGGER: "TRIGGER READINESS GATE",
    GATE_BREAKOUT: "BREAKOUT READINESS GATE",
    GATE_STRUCTURE: "STRUCTURE GATE",
}

ALLOWED_BANDS = {"favorable", "acceptable"}


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
        "trigger_band": str((band_profile.get("trigger_readiness_score") or {}).get("label", "")),
        "breakout_band": str((band_profile.get("breakout_readiness_score") or {}).get("label", "")),
        "structural_band": str((band_profile.get("structural_score") or {}).get("label", "")),
        "trigger_readiness_score": _safe_float(score.get("trigger_readiness_score"), 0.0),
        "breakout_readiness_score": _safe_float(score.get("breakout_readiness_score"), 0.0),
        "interaction_cluster_flags": list(production.get("interaction_cluster_flags", [])),
        "readiness_rebalance_flags": list(production.get("readiness_rebalance_flags", [])),
        "sizing_tier": str(sizing.get("sizing_tier") or "none"),
        "dominant_negative_flags": list(production.get("dominant_negative_flags", [])),
    }


def _failed_gates(row: dict) -> List[str]:
    failed: List[str] = []
    if row["setup_state"] not in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "TRIGGER_WATCH"}:
        failed.append(GATE_STATE)
    if row["sizing_tier"] == "none":
        failed.append(GATE_SIZING)
    if row["dominant_negative_flags"]:
        failed.append(GATE_DOMINANT_NEGATIVE)
    if row["pivot_zone"] == "far_below":
        failed.append(GATE_PIVOT)
    if row["trigger_band"] not in ALLOWED_BANDS:
        failed.append(GATE_TRIGGER)
    if row["breakout_band"] not in ALLOWED_BANDS:
        failed.append(GATE_BREAKOUT)
    if row["structural_band"] not in ALLOWED_BANDS:
        failed.append(GATE_STRUCTURE)
    return failed


def collect_gate_rows(context: dict) -> List[dict]:
    packets_map = context.get("packets", {})
    rows: List[dict] = []
    for symbol in cfg.WATCHLIST:
        packet = packets_map.get(symbol)
        if not packet:
            continue
        row = _candidate_row(symbol, packet)
        row["failed_gates"] = _failed_gates(row)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            len(row["failed_gates"]),
            -row["production_score"],
            -row["breakout_readiness_score"],
            -row["trigger_readiness_score"],
            row["symbol"],
        )
    )
    return rows


def _gate_counts(rows: List[dict]) -> Dict[str, dict]:
    total = max(len(rows), 1)
    counts: Dict[str, dict] = {}
    for gate in GATE_ORDER:
        fail_count = sum(1 for row in rows if gate in row["failed_gates"])
        pass_count = len(rows) - fail_count
        counts[gate] = {
            "pass_count": pass_count,
            "fail_count": fail_count,
            "fail_percentage": round((fail_count / total) * 100.0, 1),
        }
    return counts


def _top_failure_combinations(rows: List[dict], limit: int = 5) -> List[dict]:
    total = max(len(rows), 1)
    combo_counts = Counter(tuple(row["failed_gates"]) for row in rows if row["failed_gates"])
    ranked = combo_counts.most_common(limit)
    return [
        {
            "gates": list(combo),
            "count": count,
            "percentage": round((count / total) * 100.0, 1),
        }
        for combo, count in ranked
    ]


def _near_miss_counts(rows: List[dict]) -> Dict[str, int]:
    near_miss = {gate: 0 for gate in GATE_ORDER}
    for row in rows:
        if len(row["failed_gates"]) == 1:
            near_miss[row["failed_gates"][0]] += 1
    return near_miss


def analyze_gate_diagnostics(rows: List[dict]) -> dict:
    full_pass = [row for row in rows if not row["failed_gates"]]
    near_miss = _near_miss_counts(rows)
    return {
        "total_symbols": len(rows),
        "gate_counts": _gate_counts(rows),
        "top_failure_combinations": _top_failure_combinations(rows),
        "near_miss_counts": near_miss,
        "full_pass_count": len(full_pass),
        "full_pass_symbols": [row["symbol"] for row in full_pass[:10]],
        "one_gate_away_count": sum(near_miss.values()),
    }


def _diagnostics_output_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"gate_diagnostics_{date.today().isoformat()}.txt"


def render_gate_report(summary: dict) -> str:
    lines = [
        "=" * 60,
        "GATE DIAGNOSTICS REPORT",
        "=" * 60,
        "",
        f"TOTAL SYMBOLS: {summary['total_symbols']}",
        "",
        "-" * 60,
        "INDIVIDUAL GATE FAILURES",
        "-" * 60,
    ]
    for gate in GATE_ORDER:
        metrics = summary["gate_counts"][gate]
        lines.extend(
            [
                f"{GATE_TITLES[gate]}:",
                f"  pass: {metrics['pass_count']}",
                f"  fail: {metrics['fail_count']}",
                f"  fail %: {metrics['fail_percentage']}%",
                "",
            ]
        )
    lines.extend(
        [
            "-" * 60,
            "TOP FAILURE COMBINATIONS",
            "-" * 60,
        ]
    )
    combos = summary["top_failure_combinations"]
    if not combos:
        lines.append("None")
    else:
        for idx, combo in enumerate(combos, start=1):
            label = " + ".join(combo["gates"])
            lines.append(f"{idx}. {label} -> {combo['count']} ({combo['percentage']}%)")
    lines.extend(
        [
            "",
            "-" * 60,
            "NEAR MISSES (FAILED ONLY 1 GATE)",
            "-" * 60,
        ]
    )
    for gate in GATE_ORDER:
        lines.append(f"Failed {gate} only: {summary['near_miss_counts'][gate]}")
    lines.extend(
        [
            "",
            "-" * 60,
            "FULL PASS (ALL GATES)",
            "-" * 60,
            f"Count: {summary['full_pass_count']}",
            f"Symbols: {summary['full_pass_symbols']}",
        ]
    )
    return "\n".join(lines)


def run_gate_diagnostics(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force)
    rows = collect_gate_rows(context)
    summary = analyze_gate_diagnostics(rows)
    report_text = render_gate_report(summary)
    print(report_text)
    output_path = None
    if save:
        output_path = _diagnostics_output_path()
        output_path.write_text(report_text, encoding="utf-8")
    return {
        "rows": rows,
        "summary": summary,
        "report_text": report_text,
        "output_path": str(output_path) if output_path else None,
    }
