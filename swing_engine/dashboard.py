"""
Dashboard generator using the Jinja template in templates/dashboard.html.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from . import checklist
from . import config as cfg
from . import run_health


def _trade_score(packet: dict) -> float:
    score = packet.get("score", {})
    return float(score.get("tradeability", {}).get("score", score.get("score", 0)) or 0)


def _actionability(packet: dict, cl: dict | None) -> dict:
    if cl and cl.get("actionability"):
        return cl["actionability"]
    if packet.get("actionability"):
        return packet["actionability"]
    return checklist.evaluate_actionability(packet)


def _prepare_watchlists(packets: dict, checklists: dict) -> dict:
    structural = []
    breakout = []
    triggered = []
    failed = []
    for symbol, packet in packets.items():
        if symbol in cfg.BENCHMARKS:
            continue
        action = _actionability(packet, checklists.get(symbol))
        row = {
            "symbol": symbol,
            "packet": packet,
            "action": action,
            "score": _trade_score(packet),
        }
        state = packet.get("score", {}).get("setup_state")
        if state in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RETEST", "ACTIONABLE_RECLAIM"}:
            triggered.append(row)
        elif state in {"FAILED", "BLOCKED"}:
            failed.append(row)
        elif packet.get("score", {}).get("breakout_readiness_score", 0) >= cfg.BREAKOUT_WATCH_MIN_SCORE:
            breakout.append(row)
        else:
            structural.append(row)
    sort_key = lambda item: (-item["score"], item["symbol"])
    return {
        "structural": sorted(structural, key=sort_key)[: cfg.STRUCTURAL_LEADER_COUNT],
        "breakout": sorted(breakout, key=sort_key)[: cfg.BREAKOUT_WATCH_COUNT],
        "triggered": sorted(triggered, key=sort_key)[: cfg.TRIGGERED_NOW_COUNT],
        "failed": sorted(failed, key=sort_key),
    }


def _normalize_chart_images(chart_images: dict | None, output_path: Path) -> dict:
    output_dir = output_path.parent
    normalized: dict[str, dict] = {}
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


def generate_dashboard(regime: dict, packets: dict, checklists: dict, soxx_decision=None, narratives=None, leveraged=None, chart_images=None, output_path: Path | None = None, run_summary: dict | None = None):
    output_path = output_path or cfg.DASHBOARD_OUTPUT_PATH
    narratives = narratives or {}
    sections = _prepare_watchlists(packets, checklists)
    chart_payload = _normalize_chart_images(chart_images, output_path)
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
        narratives=narratives,
        benchmarks=[packets[symbol] for symbol in cfg.BENCHMARKS if symbol in packets],
    )
    run_health.atomic_write_text(output_path, html, encoding="utf-8")
    print(f"  Dashboard written: {output_path}")
    return output_path
