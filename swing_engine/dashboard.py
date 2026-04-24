"""
Dashboard generator using the Jinja template in templates/dashboard.html.
"""
from __future__ import annotations
from typing import Optional, Dict

from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from . import checklist
from . import config as cfg
from . import run_health


def _trade_score(packet: dict) -> float:
    score = packet.get("score", {})
    return float(score.get("tradeability", {}).get("score", score.get("score", 0)) or 0)


def _actionability(packet: dict, cl:Optional[dict]) -> dict:
    if cl and cl.get("actionability"):
        return cl["actionability"]
    if packet.get("actionability"):
        return packet["actionability"]
    return checklist.evaluate_actionability(packet)


def _prepare_watchlists(packets: dict, checklists: dict) -> dict:
    actionable = []
    near_trigger = []
    stalking = []
    continuation = []
    avoid = []
    for symbol, packet in packets.items():
        if symbol in cfg.BENCHMARKS:
            continue
        action = _actionability(packet, checklists.get(symbol))
        production_meta = packet.get("score", {}).get("production_promotion", {})
        row = {
            "symbol": symbol,
            "packet": packet,
            "action": action,
            "score": _trade_score(packet),
            "production_score": float(production_meta.get("production_score", 0.0) or 0.0),
            "priority_rank": int(production_meta.get("priority_rank", 99) or 99),
        }
        state = packet.get("score", {}).get("setup_state")
        tier = production_meta.get("tier")
        if state in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"} and tier == "production":
            actionable.append(row)
        elif state == "TRIGGER_WATCH" and tier == "production":
            near_trigger.append(row)
        elif state in {"STALKING", "FORMING"}:
            stalking.append(row)
        elif tier == "continuation":
            continuation.append(row)
        elif state in {"FAILED", "BLOCKED", "DATA_UNAVAILABLE"} or (state == "EXTENDED" and production_meta.get("extended_subtype") == "EXTENDED_LATE"):
            avoid.append(row)
        else:
            stalking.append(row)
    sort_key = lambda item: (item["priority_rank"], -item["production_score"], -item["score"], item["symbol"])
    return {
        "actionable": sorted(actionable, key=sort_key)[: cfg.TRIGGERED_NOW_COUNT + cfg.BREAKOUT_WATCH_COUNT],
        "near_trigger": sorted(near_trigger, key=sort_key)[: cfg.BREAKOUT_WATCH_COUNT],
        "stalking": sorted(stalking, key=sort_key)[: cfg.STRUCTURAL_LEADER_COUNT + cfg.BREAKOUT_WATCH_COUNT],
        "continuation": sorted(continuation, key=sort_key)[: cfg.BREAKOUT_WATCH_COUNT],
        "avoid": sorted(avoid, key=sort_key),
    }


def _normalize_chart_images(chart_images:Optional[dict], output_path: Path) -> dict:
    output_dir = output_path.parent
    normalized: Dict[str, dict] = {}
    for symbol, payload in (chart_images or {}).items():
        entry = dict(payload or {})
        for key, value in list(entry.items()):
            if key.endswith("_path") and value:
                path = Path(value)
                try:
                    entry[key.replace("_path", "_url")] = str(path.relative_to(output_dir)).replace("\\", "/")
                except ValueError:
                    entry[key.replace("_path", "_url")] = path.as_posix()
        normalized[symbol] = entry
    return normalized


def generate_dashboard(regime: dict, packets: dict, checklists: dict, soxx_decision=None, narratives=None, leveraged=None, chart_images=None, output_path:Optional[Path] = None, run_summary:Optional[dict] = None):
    output_path = output_path or cfg.DASHBOARD_OUTPUT_PATH
    narratives = narratives or {}
    sections = _prepare_watchlists(packets, checklists)
    chart_payload = _normalize_chart_images(chart_images, output_path)
    setup_families = sorted({packet.get("score", {}).get("setup_family", "none") for symbol, packet in packets.items() if symbol not in cfg.BENCHMARKS})
    env = Environment(loader=FileSystemLoader(str(cfg.TEMPLATES_DIR)), autoescape=select_autoescape(["html", "xml"]))
    template = env.get_template("dashboard.html")
    html = template.render(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        regime=regime,
        run_summary=run_summary or {},
        sections=sections,
        packets=packets,
        checklists=checklists,
        chart_images=chart_payload,
        setup_states=cfg.SETUP_STATES,
        setup_families=setup_families,
        narratives=narratives,
        benchmarks=[packets[symbol] for symbol in cfg.BENCHMARKS if symbol in packets],
    )
    run_health.atomic_write_text(output_path, html, encoding="utf-8")
    print(f"  Dashboard written: {output_path}")
    return output_path
