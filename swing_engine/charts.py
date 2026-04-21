"""
Chart Generator — pure matplotlib, no mplfinance.
Hand-draws candlesticks to avoid all pandas version conflicts.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import pandas as pd
import numpy as np
import base64
from pathlib import Path
from datetime import date
from io import BytesIO

from . import config as cfg
from . import features as feat

SMA_COLORS = {5: "#ffff00", 10: "#00bfff", 20: "#ff6600", 50: "#ff00ff", 200: "#ffffff"}
AVWAP_COLOR = "#8b5cf6"
UP = "#22c55e"
DN = "#ef4444"
BG = "#0a0a0a"
FACE = "#111111"
GRID = "#1a1a1a"
TXT = "#aaaaaa"


def _resample_intraday(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample 5-minute intraday data into a higher intraday timeframe."""
    if df.empty:
        return pd.DataFrame()
    out = df.copy().set_index("date").resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["open", "close"]).reset_index()
    return out


def _draw_candles(ax, dates, o, h, l, c, width=0.6):
    """Draw candlesticks using rectangles and lines."""
    for i in range(len(dates)):
        color = UP if c[i] >= o[i] else DN
        body_lo = min(o[i], c[i])
        body_hi = max(o[i], c[i])
        body_h = body_hi - body_lo
        if body_h < 0.001:
            body_h = 0.001
        # Wick
        ax.plot([i, i], [l[i], h[i]], color=color, linewidth=0.5)
        # Body
        rect = Rectangle((i - width/2, body_lo), width, body_h,
                         facecolor=color, edgecolor=color, linewidth=0.5)
        ax.add_patch(rect)


def _draw_volume(ax, dates, vol, o, c, width=0.6):
    """Draw volume bars."""
    for i in range(len(dates)):
        color = UP if c[i] >= o[i] else DN
        ax.bar(i, vol[i], width=width, color=color, alpha=0.4)


def _add_smas(ax, df, periods, state, n_bars):
    """Add SMA lines, solid if rising, dashed if falling."""
    offset = len(df) - n_bars
    for p in periods:
        col = f"sma_{p}"
        if col not in df.columns:
            continue
        vals = df[col].values[-n_bars:]
        xs = np.arange(len(vals))
        # Mask NaN
        mask = ~np.isnan(vals.astype(float))
        if mask.sum() < 2:
            continue
        direction = state.get(f"sma_{p}_direction", "unknown")
        ls = "--" if direction == "falling" else "-"
        color = SMA_COLORS.get(p, "#888888")
        ax.plot(xs[mask], vals[mask], color=color, linewidth=1.0,
                linestyle=ls, alpha=0.85, label=f"{p} SMA")


def _add_hlines(ax, avwap_map, pivots, session_vwaps, price, n_bars):
    """Add horizontal reference lines for AVWAPs, pivots, session VWAP."""
    # AVWAPs
    if avwap_map:
        for label, data in avwap_map.items():
            v = data.get("avwap")
            if v and price and abs(v / price - 1) < 0.20:
                ax.axhline(y=v, color=AVWAP_COLOR, linewidth=1.4, linestyle=":", alpha=0.85)
                ax.text(n_bars + 0.3, v, f" {label}: {v:.1f}",
                        fontsize=7, color=AVWAP_COLOR, alpha=0.95, va="center")

    # Pivots
    if pivots:
        for k in ("r1", "r2", "r3"):
            v = pivots.get(k)
            if v:
                ax.axhline(y=v, color="#22c55e", linewidth=0.7, linestyle="-.", alpha=0.4)
                ax.text(n_bars + 0.3, v, f" {k.upper()}: {v:.1f}",
                        fontsize=6, color="#22c55e", alpha=0.7, va="center")
        for k in ("s1", "s2", "s3"):
            v = pivots.get(k)
            if v:
                ax.axhline(y=v, color="#ef4444", linewidth=0.7, linestyle="-.", alpha=0.4)
                ax.text(n_bars + 0.3, v, f" {k.upper()}: {v:.1f}",
                        fontsize=6, color="#ef4444", alpha=0.7, va="center")
        if pivots.get("pivot"):
            ax.axhline(y=pivots["pivot"], color="#eab308", linewidth=0.5,
                       linestyle="-.", alpha=0.3)

    # Session VWAP
    if session_vwaps:
        dv = session_vwaps.get("daily_vwap")
        if dv:
            ax.axhline(y=dv, color="#eab308", linewidth=1.0, linestyle="--", alpha=0.8)
            ax.text(n_bars + 0.3, dv, f" VWAP: {dv:.1f}",
                    fontsize=7, color="#eab308", alpha=0.9, va="center")


def _style_axis(ax):
    """Apply dark theme to an axis."""
    ax.set_facecolor(FACE)
    ax.tick_params(colors=TXT, labelsize=7)
    ax.grid(True, color=GRID, linewidth=0.3, alpha=0.5)
    for spine in ax.spines.values():
        spine.set_color(GRID)


def _fig_to_b64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=100, facecolor=BG, bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _make_chart(symbol, df_raw, sma_periods, state, avwap_map, pivots,
                session_vwaps, n_bars, title_suffix, output_dir):
    """Generate one chart (daily or weekly)."""
    df = df_raw.copy().tail(n_bars)
    df = feat.add_smas(df, sma_periods)
    df = df.reset_index(drop=True)

    price = state.get("last_close", 0)
    n = len(df)

    o = df["open"].values.astype(float)
    h = df["high"].values.astype(float)
    l = df["low"].values.astype(float)
    c = df["close"].values.astype(float)
    v = df["volume"].values.astype(float)
    dates = df["date"].values

    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=(16, 8.6), gridspec_kw={"height_ratios": [4.4, 1.2]},
        facecolor=BG)

    _style_axis(ax_price)
    _style_axis(ax_vol)

    # Candles
    _draw_candles(ax_price, dates, o, h, l, c, width=0.6)

    # SMAs
    _add_smas(ax_price, df, sma_periods, state, n)

    _add_hlines(
        ax_price,
        avwap_map,
        pivots if ("daily" in title_suffix.lower() or "execution" in title_suffix.lower()) else {},
        session_vwaps if ("daily" in title_suffix.lower() or "execution" in title_suffix.lower()) else {},
        price,
        n,
    )

    # Volume
    _draw_volume(ax_vol, dates, v, o, c, width=0.6)

    # X-axis date labels
    tick_indices = np.linspace(0, n - 1, min(10, n), dtype=int)
    date_labels = [pd.Timestamp(dates[i]).strftime("%m/%d") if "daily" in title_suffix.lower()
                   else pd.Timestamp(dates[i]).strftime("%Y-%m")
                   for i in tick_indices]
    ax_vol.set_xticks(tick_indices)
    ax_vol.set_xticklabels(date_labels, fontsize=6, color=TXT)
    ax_price.set_xticks([])

    # Title and legend
    ax_price.set_title(f"{symbol} {title_suffix}", color=TXT, fontsize=14, loc="left", pad=10)
    ax_price.set_xlim(-1, n + 5)
    ax_vol.set_xlim(-1, n + 5)

    # SMA legend
    handles = []
    for p in sma_periods:
        col = f"sma_{p}"
        if col in df.columns and df[col].notna().any():
            d = state.get(f"sma_{p}_direction", "?")
            lbl = f"{p} ({d[0].upper()})" if d != "unknown" else str(p)
            ls = "--" if d == "falling" else "-"
            handles.append(plt.Line2D([0], [0], color=SMA_COLORS.get(p, "#888"),
                                       linewidth=1, linestyle=ls, label=lbl))
    if avwap_map:
        handles.append(plt.Line2D([0], [0], color=AVWAP_COLOR, linewidth=1.4, linestyle=":", label="AVWAP"))
    if session_vwaps and session_vwaps.get("daily_vwap"):
        handles.append(plt.Line2D([0], [0], color="#eab308", linewidth=1.0, linestyle="--", label="VWAP"))
    if handles:
        ax_price.legend(handles=handles, fontsize=6, loc="upper left",
                        framealpha=0.3, facecolor=FACE, edgecolor=GRID, labelcolor=TXT)

    ax_vol.set_ylabel("Vol", color=TXT, fontsize=7)
    plt.tight_layout()

    # Save
    fname = f"{symbol}_{title_suffix.lower().replace(' ', '_')}_{date.today().isoformat()}.png"
    path = output_dir / fname
    fig.savefig(path, dpi=150, facecolor=BG, bbox_inches="tight")
    b64 = _fig_to_b64(fig)
    plt.close(fig)

    return path, b64


def generate_chart(symbol, daily_df, weekly_df, daily_state=None, weekly_state=None,
                   avwap_map=None, pivots=None, session_vwaps=None,
                   recent_high=None, recent_low=None, output_dir=None,
                   intraday_df=None, include_intraday_ladder: bool = False):
    output_dir = output_dir or cfg.CACHE_DIR
    daily_state = daily_state or {}
    weekly_state = weekly_state or {}
    results = {}

    if not daily_df.empty and len(daily_df) >= 20:
        path, b64 = _make_chart(symbol, daily_df, cfg.DAILY_SMA_PERIODS, daily_state,
                                 avwap_map or {}, pivots or {}, session_vwaps or {},
                                 60, "Daily", output_dir)
        results["daily_path"] = path
        results["daily_b64"] = b64

    if not weekly_df.empty and len(weekly_df) >= 10:
        path, b64 = _make_chart(symbol, weekly_df, cfg.WEEKLY_SMA_PERIODS, weekly_state,
                                 avwap_map or {}, {}, {},
                                 52, "Weekly", output_dir)
        results["weekly_path"] = path
        results["weekly_b64"] = b64

    if include_intraday_ladder and intraday_df is not None and not intraday_df.empty:
        ladder = [
            ("15m", "15min", cfg.INTRA_SMA_PERIODS, 80),
            ("30m", "30min", cfg.INTRA_SMA_PERIODS, 60),
            ("60m", "60min", [5, 10, 20], 40),
        ]
        for label, rule, periods, bars in ladder:
            resampled = _resample_intraday(intraday_df, rule)
            if len(resampled) < max(periods, default=1) + 5:
                continue
            resampled = feat.add_smas(resampled, periods)
            state = feat.extract_ma_state(resampled, periods, f"intraday_{label}")
            path, b64 = _make_chart(
                symbol, resampled, periods, state,
                avwap_map or {}, {}, session_vwaps or {}, bars, f"{label} Execution", output_dir
            )
            results[f"intra_{label}_path"] = path
            results[f"intra_{label}_b64"] = b64

    return results


def generate_all_charts(symbols, data_store, packets, output_dir=None,
                        intraday_emphasis_symbols=None):
    output_dir = output_dir or cfg.CACHE_DIR
    intraday_emphasis_symbols = set(intraday_emphasis_symbols or [])
    all_charts = {}
    print(f"  Generating charts for {len(symbols)} symbols...")
    for sym in symbols:
        sdata = data_store.get(sym, {})
        pkt = packets.get(sym, {})
        daily_df = sdata.get("daily", pd.DataFrame())
        weekly_df = sdata.get("weekly", pd.DataFrame())
        if daily_df.empty:
            continue
        try:
            all_charts[sym] = generate_chart(
                sym, daily_df, weekly_df,
                pkt.get("daily", {}), pkt.get("weekly", {}),
                pkt.get("avwap_map"), pkt.get("pivots"),
                pkt.get("session_vwaps"), pkt.get("recent_high"), pkt.get("recent_low"),
                output_dir,
                intraday_df=sdata.get("intraday", pd.DataFrame()),
                include_intraday_ladder=sym in intraday_emphasis_symbols,
            )
            print(f"    {sym}: done")
        except Exception as e:
            print(f"    {sym}: chart error - {e}")
    return all_charts
