"""
Final decision report surface built strictly from existing packet outputs.
"""
from __future__ import annotations

from typing import List, Dict, Optional

from datetime import date
from pathlib import Path

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
    dominant_negatives = row["dominant_negative_flags"]
    if dominant_negatives or state == "BLOCKED":
        return SECTION_REJECT
    if state in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM"} and sizing_tier in {"medium", "full"}:
        return SECTION_ACTIONABLE
    if state in {"FAILED", "EXTENDED"}:
        return SECTION_REJECT
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
        lines.extend(
            [
                f"{idx}. {row['symbol']}",
                f"   State: {row['setup_state']}",
                f"   Score: {row['production_score']}",
                f"   Pivot Zone: {row['pivot_zone'] or '--'}",
                f"   Trigger Band: {row['trigger_band'] or '--'}",
                f"   Breakout Band: {row['breakout_band'] or '--'}",
                f"   Structural Band: {row['structural_band'] or '--'}",
                f"   Dominant Negatives: {negatives}",
                f"   Sizing Tier: {row['sizing_tier'] or '--'}",
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


def _write_production_dashboard(context: dict) -> str:
    run_summary = {
        "run_mode": "production",
        "runtime_mode": context.get("runtime_mode"),
        "total_symbols_attempted": len(context.get("watch_symbols", [])),
        "packet_build_failures": len(context.get("packet_failures", [])),
        "overall_status": context.get("regime", {}).get("quality", "unknown"),
        "benchmark_status": context.get("benchmark_status", {}),
    }
    output_path = dashboard.generate_dashboard(
        context["regime"],
        context["packets"],
        context["checklists"],
        chart_images={},
        output_path=cfg.DASHBOARD_OUTPUT_PATH,
        run_summary=run_summary,
    )
    return str(output_path)


def run_decision_report(force: bool = False, save: bool = True) -> dict:
    context = scan_modes.build_scan_context(force=force, runtime_mode=scan_modes.RUNTIME_MODE_PRODUCTION)
    sections = collect_candidates(context)
    report_text = render_report(sections)
    print(report_text)
    output_path: Optional[Path] = None
    dashboard_path: Optional[str] = None
    if save:
        output_path = _decision_output_path()
        run_health.atomic_write_text(output_path, report_text, encoding="utf-8")
        dashboard_path = _write_production_dashboard(context)
    return {
        "context": context,
        "sections": sections,
        "report_text": report_text,
        "output_path": str(output_path) if output_path else None,
        "dashboard_path": dashboard_path,
    }
