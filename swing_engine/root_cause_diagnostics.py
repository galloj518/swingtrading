"""
Root-cause diagnostics report built strictly from existing packet outputs.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional

from . import config as cfg
from . import scan_modes


def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _quantile(sorted_values: List[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _candidate_row(symbol: str, packet: dict) -> dict:
    score = packet.get("score", {})
    production = score.get("production_promotion", {})
    band_profile = production.get("band_profile", {})
    pivot_position = score.get("pivot_position", {}) or packet.get("breakout_features", {}).get("pivot_position", {}) or {}
    breakout_features = packet.get("breakout_features", {})
    avwap = breakout_features.get("avwap", {}) or {}
    overhead_supply = packet.get("overhead_supply", {}) or {}
    sizing = packet.get("position_sizing", {}) or {}
    actionability = packet.get("actionability", {}) or {}
    dominant_negative_flags = list(production.get("dominant_negative_flags", []))
    pivot_distance_pct = _safe_float(pivot_position.get("distance_pct"))
    return {
        "symbol": symbol,
        "setup_state": str(score.get("setup_state", "")),
        "production_score": _safe_float(production.get("production_score"), 0.0) or 0.0,
        "pivot_zone": str(production.get("pivot_zone") or ""),
        "pivot_position": str(pivot_position.get("classification") or "unavailable"),
        "pivot_distance_pct": pivot_distance_pct,
        "dominant_negative_flags": dominant_negative_flags,
        "trigger_band": str((band_profile.get("trigger_readiness_score") or {}).get("label", "")),
        "breakout_band": str((band_profile.get("breakout_readiness_score") or {}).get("label", "")),
        "structural_band": str((band_profile.get("structural_score") or {}).get("label", "")),
        "sizing_tier": str(sizing.get("sizing_tier") or "none"),
        "actionability_label": str(actionability.get("label") or ""),
        "overhead_supply_score": _safe_float(overhead_supply.get("score")),
        "avwap_resistance": bool(avwap.get("overhead_resistance")),
        "extension_atr": _safe_float(pivot_position.get("extension_atr")),
        "reward_risk_now": _safe_float(pivot_position.get("risk_reward_now")),
    }


def collect_root_cause_rows(context: dict) -> List[dict]:
    packets_map = context.get("packets", {})
    rows: List[dict] = []
    for symbol in cfg.WATCHLIST:
        packet = packets_map.get(symbol)
        if not packet:
            continue
        row = _candidate_row(symbol, packet)
        row["fails_pivot_gate"] = row["pivot_zone"] == "far_below"
        row["fails_dominant_negative_gate"] = bool(row["dominant_negative_flags"])
        rows.append(row)
    return rows


def _pivot_distance_summary(rows: List[dict]) -> dict:
    values = sorted(row["pivot_distance_pct"] for row in rows if row["pivot_distance_pct"] is not None)
    if not values:
        return {"min": None, "q1": None, "median": None, "q3": None, "max": None}
    return {
        "min": round(values[0], 4),
        "q1": round(_quantile(values, 0.25), 4),
        "median": round(median(values), 4),
        "q3": round(_quantile(values, 0.75), 4),
        "max": round(values[-1], 4),
    }


def _format_symbol_line(row: dict) -> str:
    dist = "--" if row["pivot_distance_pct"] is None else round(float(row["pivot_distance_pct"]), 4)
    return (
        f"{row['symbol']} "
        f"(state={row['setup_state']}, score={round(float(row['production_score']), 1)}, "
        f"pivot_zone={row['pivot_zone']}, pivot_position={row['pivot_position']}, "
        f"pivot_distance_pct={dist}, dominant_negatives={row['dominant_negative_flags']})"
    )


def analyze_root_causes(rows: List[dict]) -> dict:
    pivot_zone_counts = dict(sorted(Counter(row["pivot_zone"] for row in rows).items()))
    pivot_position_counts = dict(sorted(Counter(row["pivot_position"] for row in rows).items()))
    flag_counts = Counter()
    flag_combo_counts = Counter()
    negative_count_buckets = {"1": 0, "2": 0, "3+": 0}
    for row in rows:
        flags = row["dominant_negative_flags"]
        flag_counts.update(flags)
        if flags:
            flag_combo_counts.update([tuple(flags)])
            if len(flags) == 1:
                negative_count_buckets["1"] += 1
            elif len(flags) == 2:
                negative_count_buckets["2"] += 1
            else:
                negative_count_buckets["3+"] += 1

    pivot_failers = [row for row in rows if row["fails_pivot_gate"]]
    closest_pivot = sorted(
        pivot_failers,
        key=lambda row: (
            -(row["pivot_distance_pct"] if row["pivot_distance_pct"] is not None else float("-inf")),
            len(row["dominant_negative_flags"]),
            -row["production_score"],
            row["symbol"],
        ),
    )[:10]
    farthest_pivot = sorted(
        pivot_failers,
        key=lambda row: (
            row["pivot_distance_pct"] if row["pivot_distance_pct"] is not None else float("inf"),
            len(row["dominant_negative_flags"]),
            row["symbol"],
        ),
    )[:10]

    fewest_negatives = sorted(
        rows,
        key=lambda row: (
            len(row["dominant_negative_flags"]),
            -(row["pivot_distance_pct"] if row["pivot_distance_pct"] is not None else float("-inf")),
            -row["production_score"],
            row["symbol"],
        ),
    )[:10]
    most_negatives = sorted(
        rows,
        key=lambda row: (
            -len(row["dominant_negative_flags"]),
            row["pivot_distance_pct"] if row["pivot_distance_pct"] is not None else float("inf"),
            row["symbol"],
        ),
    )[:10]

    pivot_only = [row for row in rows if row["fails_pivot_gate"] and not row["fails_dominant_negative_gate"]]
    negative_only = [row for row in rows if row["fails_dominant_negative_gate"] and not row["fails_pivot_gate"]]
    fail_both = [row for row in rows if row["fails_pivot_gate"] and row["fails_dominant_negative_gate"]]
    closest_both = sorted(
        fail_both,
        key=lambda row: (
            len(row["dominant_negative_flags"]),
            -(row["pivot_distance_pct"] if row["pivot_distance_pct"] is not None else float("-inf")),
            -row["production_score"],
            row["symbol"],
        ),
    )[:10]

    return {
        "total_symbols": len(rows),
        "pivot_zone_counts": pivot_zone_counts,
        "pivot_position_counts": pivot_position_counts,
        "pivot_distance_summary": _pivot_distance_summary(rows),
        "closest_to_passing_pivot": closest_pivot,
        "farthest_from_passing_pivot": farthest_pivot,
        "dominant_negative_flag_frequencies": dict(flag_counts.most_common()),
        "dominant_negative_combinations": [
            {"flags": list(combo), "count": count}
            for combo, count in flag_combo_counts.most_common(5)
        ],
        "dominant_negative_count_buckets": negative_count_buckets,
        "fewest_dominant_negatives": fewest_negatives,
        "most_dominant_negatives": most_negatives,
        "intersection": {
            "fail_pivot_only": len(pivot_only),
            "fail_dominant_negative_only": len(negative_only),
            "fail_both": len(fail_both),
            "closest_to_passing_both": closest_both,
        },
    }


def _report_path() -> Path:
    out_dir = cfg.REPORTS_DIR / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"root_cause_diagnostics_{date.today().isoformat()}.txt"


def render_root_cause_report(summary: dict) -> str:
    lines = [
        "=" * 60,
        "ROOT-CAUSE DIAGNOSTICS REPORT",
        "=" * 60,
        "",
        f"TOTAL SYMBOLS: {summary['total_symbols']}",
        "",
        "-" * 60,
        "PIVOT ANALYSIS",
        "-" * 60,
        f"pivot_zone counts: {summary['pivot_zone_counts']}",
        f"pivot_position counts: {summary['pivot_position_counts']}",
        f"pivot_distance_pct summary: {summary['pivot_distance_summary']}",
        "closest to passing pivot:",
    ]
    if not summary["closest_to_passing_pivot"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_symbol_line(row)}" for row in summary["closest_to_passing_pivot"])
    lines.append("farthest from passing pivot:")
    if not summary["farthest_from_passing_pivot"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_symbol_line(row)}" for row in summary["farthest_from_passing_pivot"])
    lines.extend(
        [
            "",
            "-" * 60,
            "DOMINANT NEGATIVE ANALYSIS",
            "-" * 60,
            f"flag frequencies: {summary['dominant_negative_flag_frequencies']}",
            f"top flag combinations: {summary['dominant_negative_combinations']}",
            f"symbols with 1, 2, 3+ dominant negatives: {summary['dominant_negative_count_buckets']}",
            "fewest dominant negatives:",
        ]
    )
    lines.extend(f"  - {_format_symbol_line(row)}" for row in summary["fewest_dominant_negatives"])
    lines.append("most dominant negatives:")
    lines.extend(f"  - {_format_symbol_line(row)}" for row in summary["most_dominant_negatives"])
    lines.extend(
        [
            "",
            "-" * 60,
            "INTERSECTION",
            "-" * 60,
            f"fail pivot only: {summary['intersection']['fail_pivot_only']}",
            f"fail dominant-negative only: {summary['intersection']['fail_dominant_negative_only']}",
            f"fail both: {summary['intersection']['fail_both']}",
            "closest to passing both:",
        ]
    )
    if not summary["intersection"]["closest_to_passing_both"]:
        lines.append("None")
    else:
        lines.extend(f"  - {_format_symbol_line(row)}" for row in summary["intersection"]["closest_to_passing_both"])
    return "\n".join(lines)


def run_root_cause_diagnostics(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force)
    rows = collect_root_cause_rows(context)
    summary = analyze_root_causes(rows)
    report_text = render_root_cause_report(summary)
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
