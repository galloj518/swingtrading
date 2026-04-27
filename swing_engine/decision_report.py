"""
Final decision report surface built strictly from existing packet outputs.
"""
from __future__ import annotations

from typing import List, Dict, Optional

from datetime import date
from pathlib import Path
import re

from . import charts
from . import config as cfg
from . import dashboard
from . import run_health
from . import scan_modes


SECTION_ACTIONABLE = "ACTIONABLE_NOW"
SECTION_NEAR = "NEAR_ACTION"
SECTION_RESEARCH = "RESEARCH_ONLY"
SECTION_REJECT = "REJECT"


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
    trigger_primary = packet.get("intraday_trigger", {}).get("primary", {})
    rsi_context = packet.get("breakout_features", {}).get("rsi", {}) or {}
    pivot_position = score.get("pivot_position", {})
    sizing = packet.get("position_sizing", {})
    execution = packet.get("execution_policy", {})
    band_profile = production.get("band_profile", {})
    dominant_negatives = list(production.get("dominant_negative_flags", []))
    interaction_flags = list(production.get("interaction_cluster_flags", []))
    rebalance_flags = list(production.get("readiness_rebalance_flags", []))
    notes = interaction_flags + rebalance_flags
    if production.get("pivot_zone"):
        notes.append(f"pivot_zone:{production.get('pivot_zone')}")
    if dominant_negatives:
        notes.extend(f"negative:{flag}" for flag in dominant_negatives)

    return {
        "symbol": symbol,
        "setup_state": str(score.get("setup_state", "")),
        "production_score": _safe_float(production.get("production_score"), 0.0),
        "structure_score": int(production.get("structure_score") or 0),
        "expansion_score": int(production.get("expansion_score") or 0),
        "range_ratio": production.get("range_ratio"),
        "volume_ratio": production.get("volume_ratio"),
        "expansion_quality": str(production.get("expansion_quality") or "weak"),
        "rsi_14": rsi_context.get("rsi_14"),
        "rsi_bucket": str(rsi_context.get("rsi_bucket") or ""),
        "rsi_trend": str(rsi_context.get("rsi_trend") or ""),
        "pivot_zone": str(production.get("pivot_zone") or ""),
        "trigger_band": str((band_profile.get("trigger_readiness_score") or {}).get("label", "")),
        "breakout_band": str((band_profile.get("breakout_readiness_score") or {}).get("label", "")),
        "structural_band": str((band_profile.get("structural_score") or {}).get("label", "")),
        "trigger_readiness_score": _safe_float(score.get("trigger_readiness_score"), 0.0),
        "breakout_readiness_score": _safe_float(score.get("breakout_readiness_score"), 0.0),
        "interaction_cluster_flags": interaction_flags,
        "readiness_rebalance_flags": rebalance_flags,
        "dominant_negative_flags": dominant_negatives,
        "sizing_tier": str(sizing.get("sizing_tier") or "none"),
        "avwap_location_quality": str(production.get("avwap_location_quality") or ""),
        "avwap_effect_on_decision": str(production.get("avwap_effect_on_decision") or "none"),
        "avwap_resistance_filter_flag": bool(production.get("avwap_resistance_filter_flag")),
        "avwap_resistance_filter_reason": str(production.get("avwap_resistance_filter_reason") or ""),
        "avwap_resistance_anchor": str(production.get("avwap_resistance_anchor") or ""),
        "avwap_resistance_distance_pct": production.get("avwap_resistance_distance_pct"),
        "execution_lane": str(execution.get("execution_lane") or sizing.get("execution_lane") or ""),
        "execution_posture": str(execution.get("execution_posture") or sizing.get("execution_posture") or ""),
        "recommended_lane": str(execution.get("recommended_lane") or sizing.get("recommended_lane") or ""),
        "recommended_size_class": str(execution.get("recommended_size_class") or sizing.get("recommended_size_class") or ""),
        "recommended_action": str(execution.get("recommended_action") or sizing.get("recommended_action") or ""),
        "near_action_status": str(execution.get("near_action_status") or sizing.get("near_action_status") or ""),
        "trigger_level": trigger_primary.get("trigger_level"),
        "stop": trigger_primary.get("invalidation_level") or packet.get("entry_zone", {}).get("stop"),
        "target": packet.get("entry_zone", {}).get("target_1"),
        "reward_risk_now": pivot_position.get("risk_reward_now"),
        "notes": notes,
    }


def _classify_section(row: dict) -> str:
    state = row["setup_state"]
    sizing_tier = row["sizing_tier"]
    structure_score = int(row.get("structure_score") or 0)
    avwap_effect = str(row.get("avwap_effect_on_decision") or "none")
    dominant_negatives = list(row["dominant_negative_flags"])
    non_avwap_negatives = [flag for flag in dominant_negatives if flag != "avwap_blocked"]
    if state in {"FAILED", "BLOCKED", "EXTENDED", "DATA_UNAVAILABLE"} or non_avwap_negatives:
        return SECTION_REJECT
    if avwap_effect == "hard_block":
        return SECTION_REJECT if structure_score <= 0 else SECTION_RESEARCH
    if row.get("execution_lane") == "actionable":
        return SECTION_ACTIONABLE
    if row.get("execution_lane") == "near_action" or state == "TRIGGER_WATCH" or sizing_tier == "watchlist":
        return SECTION_NEAR
    if state in {"STALKING", "FORMING"} or sizing_tier == "none":
        return SECTION_RESEARCH
    return SECTION_RESEARCH


def collect_candidates(context: dict) -> Dict[str, List[dict]]:
    sections: Dict[str, List[dict]] = {
        SECTION_ACTIONABLE: [],
        SECTION_NEAR: [],
        SECTION_RESEARCH: [],
        SECTION_REJECT: [],
    }
    packets_map = context.get("packets", {})
    for symbol in cfg.WATCHLIST:
        packet = packets_map.get(symbol)
        if not packet:
            continue
        row = _candidate_row(symbol, packet)
        row["section"] = _classify_section(row)
        sections[row["section"]].append(row)
    for rows in sections.values():
        rows.sort(
            key=lambda row: (
                -row["production_score"],
                -row["breakout_readiness_score"],
                -row["trigger_readiness_score"],
                row["symbol"],
            )
        )
    return sections


def _format_section(title: str, rows: List[dict]) -> str:
    lines = [
        "=" * 60,
        title.replace("_", " "),
        "=" * 60,
    ]
    if not rows:
        lines.append("None")
        return "\n".join(lines)
    for idx, row in enumerate(rows, start=1):
        trigger = row["trigger_level"] if row["trigger_level"] is not None else "--"
        stop = row["stop"] if row["stop"] is not None else "--"
        target = row["target"] if row["target"] is not None else "--"
        rr = row["reward_risk_now"] if row["reward_risk_now"] is not None else "--"
        clusters = ", ".join(row["interaction_cluster_flags"]) if row["interaction_cluster_flags"] else "--"
        negatives = ", ".join(row["dominant_negative_flags"]) if row["dominant_negative_flags"] else "--"
        notes = ", ".join(row["notes"]) if row["notes"] else "--"
        avwap_distance = row["avwap_resistance_distance_pct"] if row["avwap_resistance_distance_pct"] is not None else "--"
        range_ratio = row["range_ratio"] if row["range_ratio"] is not None else "--"
        volume_ratio = row["volume_ratio"] if row["volume_ratio"] is not None else "--"
        rsi_14 = row["rsi_14"] if row["rsi_14"] is not None else "--"
        lines.extend(
            [
                f"{idx}. {row['symbol']}",
                f"   State: {row['setup_state']}",
                f"   Score: {row['production_score']}",
                f"   Structure Score: {row['structure_score']}",
                f"   Expansion Score: {row['expansion_score']}",
                f"   Expansion Quality: {row['expansion_quality'] or '--'}",
                f"   Range Ratio: {range_ratio}",
                f"   Volume Ratio: {volume_ratio}",
                f"   RSI 14: {rsi_14}",
                f"   RSI Bucket: {row['rsi_bucket'] or '--'}",
                f"   RSI Trend: {row['rsi_trend'] or '--'}",
                f"   Pivot Zone: {row['pivot_zone'] or '--'}",
                f"   Trigger Band: {row['trigger_band'] or '--'}",
                f"   Breakout Band: {row['breakout_band'] or '--'}",
                f"   Structural Band: {row['structural_band'] or '--'}",
                f"   Dominant Negatives: {negatives}",
                f"   Sizing Tier: {row['sizing_tier'] or '--'}",
                f"   AVWAP Location: {row['avwap_location_quality'] or '--'}",
                f"   AVWAP Effect On Decision: {row['avwap_effect_on_decision'] or '--'}",
                f"   AVWAP Resistance Anchor: {row['avwap_resistance_anchor'] or '--'}",
                f"   AVWAP Resistance Distance: {avwap_distance}",
                f"   AVWAP Filter Reason: {row['avwap_resistance_filter_reason'] or '--'}",
                f"   Execution Posture: {row['execution_posture'] or '--'}",
                f"   Recommended Lane: {row['recommended_lane'] or '--'}",
                f"   Recommended Size: {row['recommended_size_class'] or '--'}",
                f"   Recommended Action: {row['recommended_action'] or '--'}",
                f"   Trigger: {trigger}",
                f"   Stop: {stop}",
                f"   Target: {target}",
                f"   RR: {rr}",
                f"   Clusters: {clusters}",
                f"   Notes: {notes}",
            ]
        )
    return "\n".join(lines)


def render_report(sections: Dict[str, List[dict]]) -> str:
    return "\n\n".join(
        [
            _format_section("ACTIONABLE NOW", sections[SECTION_ACTIONABLE]),
            _format_section("NEAR ACTION / STARTER CANDIDATES", sections[SECTION_NEAR]),
            _format_section("RESEARCH ONLY", sections[SECTION_RESEARCH]),
            _format_section("REJECT / AVOID", sections[SECTION_REJECT]),
        ]
    )


def _decision_output_path() -> Path:
    return cfg.DECISION_REPORT_OUTPUT_PATH


def _write_pages_index() -> Path:
    output_path = cfg.PAGES_INDEX_OUTPUT_PATH
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SwingTrading Dashboard</title>
  <meta http-equiv="refresh" content="0; url=dashboard.html">
  <link rel="canonical" href="dashboard.html">
</head>
<body>
  <p>Redirecting to the latest dashboard...</p>
  <p><a href="dashboard.html">Open dashboard</a></p>
  <p><a href="decision_report.txt">Open decision report</a></p>
</body>
</html>
"""
    run_health.atomic_write_text(output_path, html, encoding="utf-8")
    return output_path


def _chart_symbols_for_dashboard(context: dict) -> List[str]:
    watchlists = dashboard._prepare_watchlists(context["packets"], context["checklists"])
    ordered: List[str] = []
    for section_name in ("actionable", "near_trigger", "stalking", "continuation", "avoid"):
        for row in watchlists[section_name]:
            symbol = row["symbol"]
            if symbol not in ordered:
                ordered.append(symbol)
    for symbol in cfg.BENCHMARKS:
        if symbol in context["packets"] and symbol not in ordered:
            ordered.append(symbol)
    return ordered


def _generate_production_chart_payload(context: dict) -> dict:
    symbols = _chart_symbols_for_dashboard(context)
    emphasized = []
    watchlists = dashboard._prepare_watchlists(context["packets"], context["checklists"])
    for section_name in ("actionable", "near_trigger"):
        for row in watchlists[section_name]:
            symbol = row["symbol"]
            if symbol not in emphasized:
                emphasized.append(symbol)
    emphasized = emphasized[: cfg.TOP_EXECUTION_INTRADAY_COUNT]
    output_dir = cfg.CHARTS_OUTPUT_DIR / "production"
    output_dir.mkdir(parents=True, exist_ok=True)
    return charts.generate_all_charts(
        symbols,
        context.get("data_store", {}),
        context["packets"],
        output_dir=output_dir,
        intraday_emphasis_symbols=emphasized,
    )


def _write_production_dashboard(context: dict) -> dict:
    run_summary = {
        "run_mode": "production",
        "runtime_mode": context.get("runtime_mode"),
        "total_symbols_attempted": len(context.get("watch_symbols", [])),
        "packet_build_failures": len(context.get("packet_failures", [])),
        "overall_status": context.get("regime", {}).get("quality", "unknown"),
        "benchmark_status": context.get("benchmark_status", {}),
    }
    chart_images = _generate_production_chart_payload(context)
    output_path = dashboard.generate_dashboard(
        context["regime"],
        context["packets"],
        context["checklists"],
        chart_images=chart_images,
        output_path=cfg.DASHBOARD_OUTPUT_PATH,
        run_summary=run_summary,
    )
    return {"dashboard_path": str(output_path), "chart_images": chart_images}


def _validate_production_outputs(context: dict, dashboard_path: Path, report_path: Path) -> dict:
    validation = {
        "dashboard_exists": dashboard_path.exists(),
        "decision_report_exists": report_path.exists(),
        "chart_reference_count": 0,
        "missing_chart_references": [],
        "card_mismatches": [],
    }
    if not dashboard_path.exists():
        return validation

    html = dashboard_path.read_text(encoding="utf-8")
    image_refs = re.findall(r'<img[^>]+src="([^"]+)"', html)
    validation["chart_reference_count"] = len(image_refs)
    for src in image_refs:
        if src.startswith("data:image/"):
            continue
        candidate = (dashboard_path.parent / src).resolve()
        if not candidate.exists():
            validation["missing_chart_references"].append(src)

    watchlists = dashboard._prepare_watchlists(context["packets"], context["checklists"])
    all_rows = []
    for section_name in ("actionable", "near_trigger", "stalking", "continuation", "avoid"):
        all_rows.extend(watchlists[section_name])

    for row in all_rows:
        display = row.get("display", {})
        symbol = str(display.get("symbol") or row.get("symbol") or "")
        marker = f'data-symbol="{symbol}"'
        start = html.find(marker)
        if start < 0:
            validation["card_mismatches"].append(f"{symbol}: missing dashboard card")
            continue
        next_idx = html.find('data-symbol="', start + len(marker))
        block = html[start: next_idx if next_idx > start else len(html)]
        expected_pairs = {
            "setup_state": str(display.get("setup_state") or "--"),
            "production_score": str(display.get("production_score") if display.get("production_score") is not None else "--"),
            "pivot_zone": str(display.get("pivot_zone") or "--"),
            "trigger_band": str(display.get("trigger_band") or "--"),
            "breakout_band": str(display.get("breakout_band") or "--"),
            "structural_band": str(display.get("structural_band") or "--"),
            "sizing_tier": str(display.get("sizing_tier") or "--"),
            "avwap_location_quality": str(display.get("avwap_location_quality") or "--"),
        }
        for field_name, expected in expected_pairs.items():
            if expected not in block:
                validation["card_mismatches"].append(f"{symbol}: {field_name} missing/inconsistent")
        dominant_negative_flags = display.get("dominant_negative_flags") or []
        if dominant_negative_flags:
            for flag in dominant_negative_flags:
                if str(flag) not in block:
                    validation["card_mismatches"].append(f"{symbol}: dominant negative {flag} missing")
        execution_policy = display.get("execution_policy") or []
        for part in execution_policy:
            if str(part) not in block:
                validation["card_mismatches"].append(f"{symbol}: execution policy {part} missing")
                break

    return validation


def run_decision_report(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force, runtime_mode=scan_modes.RUNTIME_MODE_PRODUCTION)
    sections = collect_candidates(context)
    report_text = render_report(sections)
    print(report_text)
    output_path: Optional[Path] = None
    dashboard_path: Optional[str] = None
    validation: Optional[dict] = None
    if save:
        output_path = _decision_output_path()
        run_health.atomic_write_text(output_path, report_text, encoding="utf-8")
        dashboard_result = _write_production_dashboard(context)
        dashboard_path = dashboard_result["dashboard_path"]
        _write_pages_index()
        validation = _validate_production_outputs(context, Path(dashboard_path), output_path)
        print(
            "Dashboard validation:"
            f" dashboard_exists={validation['dashboard_exists']}"
            f" decision_report_exists={validation['decision_report_exists']}"
            f" chart_refs={validation['chart_reference_count']}"
            f" missing_chart_refs={len(validation['missing_chart_references'])}"
            f" card_mismatches={len(validation['card_mismatches'])}"
        )
    return {
        "context": context,
        "sections": sections,
        "report_text": report_text,
        "output_path": str(output_path) if output_path else None,
        "dashboard_path": dashboard_path,
        "validation": validation,
    }
