"""
Run health collection and atomic output helpers.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from . import config as cfg


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding=encoding, newline="") as handle:
        handle.write(content)
        tmp_name = handle.name
    os.replace(tmp_name, path)
    return path


def atomic_write_json(path: Path, payload: dict) -> Path:
    return atomic_write_text(path, json.dumps(payload, indent=2, default=str), encoding="utf-8")


def start_timer() -> float:
    return time.perf_counter()


def _source_counts(data_status: dict, symbols: list[str]) -> tuple[int, int, int]:
    live = 0
    cache_fallback = 0
    unavailable = 0
    for symbol in symbols:
        status = data_status.get(symbol, {})
        sources = {status.get("daily_source"), status.get("intraday_source")}
        if "unavailable" in sources:
            unavailable += 1
        elif "cache_fallback" in sources:
            cache_fallback += 1
        elif sources and sources.issubset({"yfinance", "fixture", None}):
            live += 1
        else:
            unavailable += 1
    return live, cache_fallback, unavailable


def collect_run_health(mode: str, context: dict, started_at: float) -> dict:
    packets = context.get("packets", {})
    watch_symbols = [symbol for symbol in context.get("watch_symbols", []) if symbol in packets]
    benchmark_symbols = [symbol for symbol in context.get("benchmark_symbols", []) if symbol in packets]
    data_status = context.get("data_status", {})
    packet_failures = context.get("packet_failures", [])
    benchmark_status = context.get("benchmark_status", {})
    regime = context.get("regime", {})

    live, cache_fallback, unavailable = _source_counts(data_status, watch_symbols)
    trigger_degraded = sum(
        1
        for symbol in watch_symbols
        if packets.get(symbol, {}).get("intraday_trigger", {}).get("trigger_state") == "data_unavailable"
        or packets.get(symbol, {}).get("data_quality", {}).get("intraday_freshness_label") in {"missing", "stale", "very_stale"}
    )
    setup_state_counts = Counter(packets.get(symbol, {}).get("score", {}).get("setup_state", "UNKNOWN") for symbol in watch_symbols)
    actionability_counts = Counter(packets.get(symbol, {}).get("actionability", {}).get("label", "UNKNOWN") for symbol in watch_symbols)
    benchmark_available_count = sum(1 for available in benchmark_status.values() if available)
    regime_degraded = bool(regime.get("quality") == "degraded" or benchmark_available_count < len(benchmark_symbols))

    total_symbols = max(len(watch_symbols), 1)
    unavailable_ratio = unavailable / total_symbols
    trigger_ratio = trigger_degraded / total_symbols
    if total_symbols == 0 or unavailable_ratio >= cfg.RUN_FAILED_UNAVAILABLE_RATIO:
        overall_status = "failed"
    elif (
        unavailable_ratio >= cfg.RUN_DEGRADED_UNAVAILABLE_RATIO
        or len(packet_failures) >= cfg.RUN_DEGRADED_PACKET_FAILURE_COUNT
        or regime_degraded
        or trigger_ratio >= cfg.RUN_DEGRADED_TRIGGER_RATIO
    ):
        overall_status = "degraded"
    else:
        overall_status = "healthy"

    return {
        "run_mode": mode,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.perf_counter() - started_at, 3),
        "total_symbols_attempted": len(watch_symbols),
        "symbols_loaded_live": live,
        "symbols_loaded_from_cache_fallback": cache_fallback,
        "symbols_unavailable": unavailable,
        "packet_build_failures": len(packet_failures),
        "packet_failure_symbols": sorted(packet_failures),
        "benchmark_availability": benchmark_status,
        "benchmark_available_count": benchmark_available_count,
        "benchmark_symbols": benchmark_symbols,
        "regime_degraded": regime_degraded,
        "intraday_trigger_degraded_count": trigger_degraded,
        "intraday_trigger_degraded_ratio": round(trigger_ratio, 3),
        "overall_status": overall_status,
        "setup_state_counts": dict(sorted(setup_state_counts.items())),
        "actionability_counts": dict(sorted(actionability_counts.items())),
    }


def persist_run_health(run_health: dict) -> Path:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = cfg.RUN_HEALTH_OUTPUT_DIR / f"run_health_{run_health['run_mode']}_{timestamp}.json"
    return atomic_write_json(path, run_health)
