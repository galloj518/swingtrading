"""
LLM Narrative Layer.

Generates Shannon-style contextual trade plans for actionable symbols (score 50+).
Uses the pre-computed packet — the LLM interprets, it doesn't calculate.

Optional: runs only if OPENAI_API_KEY is set. System works fully without it.
"""
import os
import json
from typing import Optional


SHANNON_SYSTEM_PROMPT = """You are a senior swing trade analyst who thinks exactly like Brian Shannon (Alphtrends).

You are given a pre-computed analysis packet for a stock. ALL the math is already done —
scores, MAs, directions, AVWAPs, pivots, entry zones, stops, targets.

Your job is to synthesize everything into a DENSE, SPECIFIC trade plan. NOT to recalculate.

REQUIRED ELEMENTS in your response:
1. WEEKLY CONTEXT: Is the weekly trend supporting this? Which direction are weekly MAs moving?
2. DAILY TIMING: Where is price relative to the 10/20 SMA? Are short-term MAs rising or falling?
3. AVWAP MAP: Which AVWAPs is price above/below? What do they mean for institutional inventory?
4. VOLUME READ: Is RVol high or low? What does that tell you about this move?
5. THE SETUP: Exactly what type of setup this is and whether the timing is right NOW
6. IF UP TOMORROW: What to do if price gaps up or opens strong
7. IF DOWN TOMORROW: What to do if price gaps down or opens weak — is that an opportunity or a warning?
8. ENTRY/STOP/TARGETS: Reference the specific pivot levels (R1/R2 for targets, S1/S2 for stops)
9. WHAT WOULD MAKE THIS BETTER: Specific conditions that would upgrade the setup
10. EVENT RISK: How should the current macro environment affect position sizing?

RULES:
- Use SPECIFIC prices from the packet. Never say "around X" — say the exact level.
- If the 5 SMA is falling, say so explicitly — momentum is NOT confirmed.
- If price is above the entry zone, say "don't chase" and give the pullback target.
- Be direct and opinionated. You're writing a trading desk note, not a research report.
- Under 250 words. Dense. Every sentence must add information.
- No disclaimers, no "this is not financial advice." The user is a trader.

Respond in plain text paragraphs. Not JSON. Not bullet points. Trading desk note style."""


def generate_narrative(packet: dict, regime: dict) -> Optional[str]:
    """
    Generate a Shannon-style narrative trade plan for one symbol.
    Returns None if OpenAI key not set or call fails.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print(f"  Narrative skipped for {packet.get('symbol', '?')}: OPENAI_API_KEY not set")
        return None

    try:
        from openai import OpenAI
    except ImportError:
        print(f"  Narrative skipped for {packet.get('symbol', '?')}: openai package not installed")
        return None

    # Build compact context for the LLM (token-efficient)
    sym = packet.get("symbol", "?")
    d = packet.get("daily", {})
    w = packet.get("weekly", {})
    ez = packet.get("entry_zone", {})
    sc = packet.get("score", {})
    setup = packet.get("setup", {})
    pivots = packet.get("pivots", {})
    avwaps = packet.get("avwap_map", {})
    avwap_context = packet.get("avwap_context", {})
    session_v = packet.get("session_vwaps", {})
    rs = packet.get("relative_strength", {})
    rh = packet.get("recent_high", {})
    rl = packet.get("recent_low", {})
    events = packet.get("events", {})
    earnings = packet.get("earnings", {})

    # Build AVWAP summary (only nearest ones matter)
    price = d.get("last_close", 0)
    avwap_summary = {}
    for label, data in avwaps.items():
        av = data.get("avwap", 0)
        if av:
            dist = round((price / av - 1) * 100, 1) if price else 0
            avwap_summary[label] = {"level": av, "dist_pct": dist}

    context = {
        "symbol": sym,
        "price": price,
        "score": sc.get("score"),
        "quality": sc.get("quality"),
        "action_bias": sc.get("action_bias"),
        "setup": setup,
        "regime": regime.get("regime"),
        "regime_risk": regime.get("risk_appetite"),
        "regime_bias": regime.get("swing_bias"),
        "entry_zone": ez,
        "pivots": pivots,
        "avwaps_near_price": avwap_summary,
        "avwap_context": avwap_context,
        "session_vwap": session_v,
        "daily_ma": {
            "sma5": d.get("sma_5"), "sma5_dir": d.get("sma_5_direction"),
            "sma10": d.get("sma_10"), "sma10_dir": d.get("sma_10_direction"),
            "sma20": d.get("sma_20"), "sma20_dir": d.get("sma_20_direction"),
            "sma50": d.get("sma_50"), "sma50_dir": d.get("sma_50_direction"),
            "stack": d.get("ma_stack"),
        },
        "weekly_ma": {
            "sma5": w.get("sma_5"), "sma5_dir": w.get("sma_5_direction"),
            "sma10": w.get("sma_10"), "sma10_dir": w.get("sma_10_direction"),
            "sma20": w.get("sma_20"), "sma20_dir": w.get("sma_20_direction"),
            "stack": w.get("ma_stack"),
        },
        "rvol": d.get("rvol"),
        "atr": d.get("atr"),
        "rs_20d": rs.get("rs_20d"),
        "recent_high": rh,
        "recent_low": rl,
        "event_risk": events.get("recommendation"),
        "earnings": earnings.get("note"),
    }

    user_msg = f"Generate a Shannon-style trade plan for:\n{json.dumps(context, indent=2, default=str)}"

    try:
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0.3,
            max_tokens=500,
            messages=[
                {"role": "system", "content": SHANNON_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  Narrative failed for {sym}: {type(e).__name__}: {e}")
        return None


def generate_narratives(packets: dict, regime: dict,
                        min_score: int | None = 50,
                        selected_symbols: list[str] | None = None,
                        max_count: int | None = None) -> dict:
    """
    Generate narratives for all qualifying symbols.
    Returns dict of symbol -> narrative string.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("  (No OPENAI_API_KEY — skipping narratives)")
        return {}

    narratives = {}
    selected_set = set(selected_symbols or [])
    qualifying_items = [
        (s, p) for s, p in packets.items()
        if (min_score is None or p.get("score", {}).get("score", 0) >= min_score)
        and s not in ("SPY", "QQQ", "SOXX", "DIA")
        and (not selected_set or s in selected_set)
    ]

    if selected_symbols:
        order_map = {sym: i for i, sym in enumerate(selected_symbols)}
        qualifying_items.sort(key=lambda item: order_map.get(item[0], 9999))
    else:
        qualifying_items.sort(
            key=lambda item: item[1].get("score", {}).get("score", 0),
            reverse=True,
        )

    if max_count is not None:
        qualifying_items = qualifying_items[:max_count]

    qualifying = dict(qualifying_items)

    if not qualifying:
        threshold_note = "ranked selection" if min_score is None else f"score >= {min_score}"
        print(f"  No symbols qualify for narrative ({threshold_note})")
        return {}

    print(f"  Generating narratives for {len(qualifying)} symbols...")
    import time
    for sym, pkt in qualifying.items():
        print(f"    {sym}...", end=" ")
        narrative = generate_narrative(pkt, regime)
        if narrative:
            narratives[sym] = narrative
            print("done")
        else:
            print("skipped")
        time.sleep(0.3)  # rate limit courtesy

    return narratives
