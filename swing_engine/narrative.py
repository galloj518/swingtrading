"""
Optional narrative generation. Never called from frequent scan paths unless the
user explicitly runs the narrative mode.
"""
from __future__ import annotations

import json
import os
from typing import Optional

from . import config as cfg


SYSTEM_PROMPT = """You are a senior swing trader writing a concise desk note.
The packet is already deterministic. Do not recalculate anything. Explain the
setup family, stage, trigger, freshness caveats, and exact levels. Keep it
under 220 words and write in plain text."""


def generate_narrative(packet: dict, regime: dict) -> Optional[str]:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return None
    try:
        from openai import OpenAI
    except ImportError:
        return None
    context = {
        "symbol": packet.get("symbol"),
        "regime": regime,
        "score": packet.get("score"),
        "setup": packet.get("setup"),
        "breakout_patterns": packet.get("breakout_patterns", {}).get("primary"),
        "intraday_trigger": packet.get("intraday_trigger", {}).get("primary"),
        "entry_zone": packet.get("entry_zone"),
        "data_quality": packet.get("data_quality"),
        "relative_strength": packet.get("relative_strength"),
        "avwap_context": packet.get("avwap_context"),
    }
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=cfg.OPENAI_MODEL,
            temperature=0.2,
            max_tokens=400,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(context, indent=2, default=str)},
            ],
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return None


def generate_narratives(packets: dict, regime: dict, min_score: int | None = None, selected_symbols: list[str] | None = None, max_count: int | None = None) -> dict:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return {}
    selected = set(selected_symbols or [])
    candidates = [
        (symbol, packet) for symbol, packet in packets.items()
        if symbol not in cfg.BENCHMARKS and (min_score is None or packet.get("score", {}).get("score", 0) >= min_score)
        and (not selected or symbol in selected)
    ]
    candidates.sort(key=lambda item: item[1].get("score", {}).get("score", 0), reverse=True)
    if max_count is not None:
        candidates = candidates[:max_count]
    narratives = {}
    for symbol, packet in candidates:
        text = generate_narrative(packet, regime)
        if text:
            narratives[symbol] = text
    return narratives
