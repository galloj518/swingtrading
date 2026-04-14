"""
Static HTML Dashboard Generator.

Full trading dashboard with:
- Regime summary with benchmark detail
- Event calendar
- SOXX -> SOXL traffic light
- Watchlist heatmap sorted by score
- Per-symbol detail cards with:
  - AVWAP map from key events
  - Pivot levels (P, R1-R3, S1-S3)
  - MA state across all timeframes
  - SMA5 today and tomorrow target
  - Entry zone, stop, targets
  - Position sizing
  - Relative strength and volume
  - Confluence levels
  - Checklist

Designed for Vercel deployment and mobile viewing.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import config as cfg


def _fmt(val, decimals=2):
    """Format a number safely."""
    if val is None:
        return "--"
    try:
        return f"{float(val):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def _pct(val):
    """Format a percentage."""
    if val is None:
        return "--"
    try:
        v = float(val)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.1f}%"
    except (ValueError, TypeError):
        return str(val)


def _bool_icon(val):
    """Boolean to pass/fail icon."""
    if val is True:
        return '<span style="color:#22c55e;">Y</span>'
    elif val is False:
        return '<span style="color:#ef4444;">N</span>'
    return '<span style="color:#555;">--</span>'


def _build_upgrade_html(conditions):
    """Build HTML for upgrade conditions list."""
    if not conditions:
        return ""
    items = "".join(f"<li>{c}</li>" for c in conditions)
    return (
        '<div style="border-top:1px solid #222;margin-top:4px;padding-top:4px;">'
        '<span style="color:#58a6ff;font-size:0.85em;">UPGRADE CONDITIONS:</span>'
        f'<ul style="color:#888;font-size:0.8em;margin:2px 0 0 16px;">{items}</ul>'
        '</div>'
    )


def _build_leveraged_html(leveraged_results):
    """Build the leveraged benchmark tactical section."""
    if not leveraged_results:
        return ""

    sig_colors = {"GREEN": "#22c55e", "YELLOW": "#eab308", "RED": "#ef4444"}

    rows = ""
    for chart_sym, r in leveraged_results.items():
        l = r.get("long", {})
        s = r.get("short", {})
        lc = sig_colors.get(l.get("signal", "RED"), "#666")
        sc_color = sig_colors.get(s.get("signal", "RED"), "#666")

        # Payoff estimates
        l_pf = l.get("payoff", {})
        s_pf = s.get("payoff", {})
        l_stop_pct = l_pf.get("stop", {}).get("leveraged_pct", "?")
        l_t1_pct = l_pf.get("target_1", {}).get("leveraged_pct", "?")
        s_stop_pct = s_pf.get("stop", {}).get("leveraged_pct", "?")
        s_t1_pct = s_pf.get("target_1", {}).get("leveraged_pct", "?")

        l_sizing = l.get("sizing", {})
        s_sizing = s.get("sizing", {})
        dnht = ", ".join(l.get("do_not_hold_through", [])) or "None"

        rows += f"""
        <div style="border:1px solid #222;border-radius:4px;padding:10px;margin-bottom:8px;">
          <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
            <div>
              <span style="color:#fff;font-weight:700;font-size:1.1em;">{chart_sym}</span>
              <span style="color:#888;margin-left:8px;">{r.get('name', '')}</span>
              <span style="color:#ccc;margin-left:8px;">{_fmt(r.get('price'))}</span>
              <span style="color:#888;margin-left:8px;">Score: {r.get('score', '?')}</span>
              <span style="color:#888;margin-left:8px;">Stack: {r.get('daily_stack', '?')}/{r.get('weekly_stack', '?')}</span>
            </div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px;">
            <div style="border:1px solid #222;border-radius:4px;padding:8px;border-left:3px solid {lc};">
              <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                <div style="width:12px;height:12px;border-radius:50%;background:{lc};"></div>
                <span style="color:{lc};font-weight:700;">LONG {l.get('vehicle', '?')}</span>
              </div>
              <div style="color:#aaa;font-size:0.85em;">{l.get('reason', '')}</div>
              <div style="color:#888;font-size:0.8em;margin-top:4px;">
                Stop: {_fmt(l.get('stop'))} ({l_stop_pct}% lev) |
                T1: {_fmt(l.get('target_1'))} ({l_t1_pct}% lev) |
                Shares: {l_sizing.get('shares', '?')} | Risk: ${_fmt(l_sizing.get('risk_dollars'))}
              </div>
              <div style="color:#666;font-size:0.8em;">Trigger: {l.get('setup_trigger') or 'See watchlist'}</div>
            </div>
            <div style="border:1px solid #222;border-radius:4px;padding:8px;border-left:3px solid {sc_color};">
              <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                <div style="width:12px;height:12px;border-radius:50%;background:{sc_color};"></div>
                <span style="color:{sc_color};font-weight:700;">SHORT {s.get('vehicle', '?')}</span>
              </div>
              <div style="color:#aaa;font-size:0.85em;">{s.get('reason', '')}</div>
              <div style="color:#888;font-size:0.8em;margin-top:4px;">
                Entry: {_fmt(s.get('entry_level'))} |
                Stop: {_fmt(s.get('stop'))} ({s_stop_pct}% lev) |
                T1: {_fmt(s.get('target_1'))} ({s_t1_pct}% lev) |
                Shares: {s_sizing.get('shares', '?')}
              </div>
              <div style="color:#666;font-size:0.8em;">Trigger: {s.get('trigger') or 'N/A'}</div>
            </div>
          </div>
          <div style="color:#666;font-size:0.75em;margin-top:4px;">
            Max hold: {l.get('max_hold_days', 7)}d | DO NOT HOLD: {dnht} | Event: {r.get('event_risk', 'none')}
          </div>
        </div>"""

    return f"""
    <div class="section">
      <h2>LEVERAGED BENCHMARK TACTICAL</h2>
      {rows}
    </div>"""


def _score_color(score):
    if score >= 70:
        return "#22c55e", "#052e16"
    elif score >= 55:
        return "#eab308", "#1a1a00"
    elif score >= 40:
        return "#f97316", "#1a0a00"
    else:
        return "#6b7280", "#111"


def _build_symbol_card(sym, pkt, cl, narrative_text=None, chart_data=None):
    """Build a detailed card for one symbol."""
    chart_data = chart_data or {}
    sc = pkt.get("score", {})
    ez = pkt.get("entry_zone", {})
    setup = pkt.get("setup", {})
    d = pkt.get("daily", {})
    w = pkt.get("weekly", {})
    intra = pkt.get("intraday", {})
    pivots = pkt.get("pivots", {})
    avwaps = pkt.get("avwap_map", {})
    refs = pkt.get("reference_levels", {})
    session_vwaps = pkt.get("session_vwaps", {})
    rs = pkt.get("relative_strength", {})
    ps = pkt.get("position_sizing", {})
    conf = pkt.get("confluence", {})
    earn = pkt.get("earnings", {})
    sma5_tmw = pkt.get("sma5_tomorrow")
    rh = pkt.get("recent_high", {})
    rl = pkt.get("recent_low", {})

    score = sc.get("score", 0)
    color, bg = _score_color(score)
    price = d.get("last_close", 0)

    # --- AVWAP rows ---
    avwap_rows = ""
    if avwaps:
        for label, data in sorted(avwaps.items(), key=lambda x: x[1].get("avwap", 0)):
            av = data.get("avwap", 0)
            anchor = data.get("anchor_date", "")
            dist = ((price / av) - 1) * 100 if av and price else 0
            above = "above" if dist >= 0 else "BELOW"
            dist_color = "#22c55e" if dist >= 0 else "#ef4444"
            avwap_rows += f"""
            <tr>
              <td style="color:#999;">{label}</td>
              <td>{_fmt(av)}</td>
              <td style="color:#555;">{anchor}</td>
              <td style="color:{dist_color};">{_pct(dist)} {above}</td>
            </tr>"""
    else:
        avwap_rows = '<tr><td colspan="4" style="color:#555;">No AVWAPs configured</td></tr>'

    # --- Pivot rows ---
    pivot_html = ""
    if pivots:
        p = pivots
        pivot_html = f"""
        <table class="inner-table">
          <tr><th>S3</th><th>S2</th><th>S1</th><th style="color:#eab308;">Pivot</th><th>R1</th><th>R2</th><th>R3</th></tr>
          <tr>
            <td style="color:#ef4444;">{_fmt(p.get('s3'))}</td>
            <td style="color:#ef4444;">{_fmt(p.get('s2'))}</td>
            <td style="color:#ef4444;">{_fmt(p.get('s1'))}</td>
            <td style="color:#eab308;font-weight:700;">{_fmt(p.get('pivot'))}</td>
            <td style="color:#22c55e;">{_fmt(p.get('r1'))}</td>
            <td style="color:#22c55e;">{_fmt(p.get('r2'))}</td>
            <td style="color:#22c55e;">{_fmt(p.get('r3'))}</td>
          </tr>
        </table>"""

    # --- MA state table with DIRECTION ---
    def _dir_icon(direction):
        if direction == "rising":
            return '<span style="color:#22c55e;">RISING</span>'
        elif direction == "falling":
            return '<span style="color:#ef4444;">FALLING</span>'
        elif direction == "flat":
            return '<span style="color:#eab308;">FLAT</span>'
        return '<span style="color:#555;">--</span>'

    def _tmw_icon(bias):
        if bias == "will_rise":
            return '<span style="color:#22c55e;">RISE</span>'
        elif bias == "will_fall":
            return '<span style="color:#ef4444;">FALL</span>'
        elif bias == "flat":
            return '<span style="color:#eab308;">FLAT</span>'
        return '<span style="color:#555;">--</span>'

    def build_ma_table(label, state, periods):
        """Build a full MA table with value, above/below, direction, and tomorrow."""
        rows = ""
        for p in periods:
            col = f"sma_{p}"
            val = state.get(col)
            if val is None:
                continue
            above = state.get(f"close_above_{col}")
            above_c = "#22c55e" if above else "#ef4444"
            above_txt = "Above" if above else "Below"
            dist = state.get(f"dist_from_{col}_pct", 0)
            direction = state.get(f"{col}_direction", "unknown")
            change = state.get(f"{col}_change", 0)
            tmw_bias = state.get(f"{col}_tomorrow_bias", "--")
            need_tmw = state.get(f"{col}_need_tomorrow")
            dropping = state.get(f"{col}_dropping_off")

            rows += f"""
            <tr>
              <td style="color:#aaa;">{p}</td>
              <td style="color:#fff;">{_fmt(val)}</td>
              <td style="color:{above_c};">{above_txt}</td>
              <td>{_pct(dist)}</td>
              <td>{_dir_icon(direction)}</td>
              <td style="color:#888;">{_fmt(change)}</td>
              <td>{_tmw_icon(tmw_bias)}</td>
              <td style="color:#888;">{_fmt(need_tmw) if need_tmw is not None else '--'}</td>
            </tr>"""

        stack = state.get("ma_stack", "--")
        stack_c = "#22c55e" if stack == "bullish" else "#ef4444" if stack == "bearish" else "#eab308"

        return f"""
        <div style="margin-bottom:6px;">
          <span style="color:#aaa;font-weight:700;">{label}</span>
          <span style="color:#fff;margin-left:8px;">Price: {_fmt(state.get('last_close'))}</span>
          <span style="color:{stack_c};margin-left:8px;">Stack: {stack}</span>
        </div>
        <table class="inner-table">
          <tr>
            <th>MA</th><th>Value</th><th>Price</th><th>Dist</th>
            <th>Direction</th><th>Chg</th><th>Tomorrow</th><th>Need Flat</th>
          </tr>
          {rows}
        </table>"""

    daily_ma_html = build_ma_table("DAILY", d, [5, 10, 20, 50, 200])
    weekly_ma_html = build_ma_table("WEEKLY", w, [5, 10, 20])
    intra_ma_html = build_ma_table("INTRADAY", intra, [10, 20, 50])

    # --- SMA5 tomorrow ---
    sma5_current = d.get("sma_5")
    sma5_tmw_html = ""
    if sma5_current or sma5_tmw:
        dropping = d.get("sma_5_dropping_off")
        sma5_tmw_html = f"""
        <div class="detail-row" style="margin-top:6px;padding:6px;background:#0a0a0a;border-radius:3px;">
          <span class="detail-label" style="color:#eab308;">SMA5 KEY:</span>
          <span>Today: {_fmt(sma5_current)}</span>
          <span style="margin-left:8px;">Dropping off: {_fmt(dropping)}</span>
          <span style="margin-left:8px;">Need for flat: <span style="color:#eab308;font-weight:700;">{_fmt(sma5_tmw)}</span></span>
          <span style="margin-left:8px;color:#888;">{'Price > dropping off = SMA5 rises tomorrow' if dropping and price and price > dropping else 'Price < dropping off = SMA5 falls tomorrow' if dropping and price else ''}</span>
        </div>"""

    # --- Checklist ---
    checklist_html = ""
    if cl:
        for c in cl["checks"]:
            icon = "PASS" if c["passed"] else "FAIL"
            ic = "#22c55e" if c["passed"] else "#ef4444"
            checklist_html += f'<div class="check-item"><span style="color:{ic};width:35px;">[{icon}]</span><span style="color:#888;width:120px;">{c["item"]}</span><span>{c["value"]}</span></div>'
        verdict = cl.get("verdict", "")
        vc = "#22c55e" if "PROCEED" in verdict else "#eab308" if "CAUTION" in verdict else "#ef4444"
        checklist_html += f'<div class="verdict" style="color:{vc};">{verdict}</div>'

    # --- Build card ---
    card = f"""
    <div class="symbol-card">
      <div class="card-header" style="border-left:4px solid {color};">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
          <div>
            <span class="sym-name">{sym}</span>
            <span style="color:{color};font-weight:700;font-size:1.3em;margin-left:8px;">{score}</span>
            <span style="color:#888;">/ 100</span>
            <span style="color:{color};margin-left:8px;">{sc.get('quality', '')}</span>
          </div>
          <div style="text-align:right;">
            <span style="font-size:1.2em;color:#fff;">{_fmt(price)}</span>
            <span style="color:#888;margin-left:8px;">ATR: {_fmt(d.get('atr'))}</span>
            <span style="color:#888;margin-left:8px;">RVol: {_fmt(d.get('rvol'), 1)}x</span>
          </div>
        </div>
        <div style="color:#aaa;margin-top:4px;">
          Setup: <span style="color:#fff;font-weight:700;">{setup.get('type', '--')}</span> --
          {setup.get('description', '')}
        </div>
        <div style="margin-top:6px;padding:8px;background:#0d1117;border:1px solid #333;border-radius:4px;">
          <div style="color:#22c55e;font-size:0.9em;margin-bottom:4px;">
            TRIGGER: {setup.get('trigger') or 'None'}
          </div>
          <div style="color:#eab308;font-size:0.85em;margin-bottom:4px;">
            WATCH: {setup.get('watch_for') or 'None'}
          </div>
          <div style="color:#ef4444;font-size:0.85em;margin-bottom:4px;">
            INVALIDATION: {setup.get('invalidation') or 'None'}
          </div>
          <div style="border-top:1px solid #222;margin-top:4px;padding-top:4px;font-size:0.85em;">
            <div style="color:#ccc;margin-bottom:2px;">
              <span style="color:#666;">GAP UP:</span> {setup.get('gap_up_plan', 'N/A')}
            </div>
            <div style="color:#ccc;margin-bottom:2px;">
              <span style="color:#666;">GAP DOWN:</span> {setup.get('gap_down_plan', 'N/A')}
            </div>
            <div style="color:#ccc;margin-bottom:2px;">
              <span style="color:#666;">PARTIALS:</span> {setup.get('partial_take_plan', 'N/A')}
            </div>
            <div style="color:#ccc;margin-bottom:2px;">
              <span style="color:#666;">MAX CHASE:</span> {setup.get('max_chase_pct', 0)}% | 
              <span style="color:#666;">SIZE:</span> {setup.get('position_size_guidance', 'standard')} | 
              <span style="color:#666;">HORIZON:</span> {setup.get('time_horizon', 'N/A')}
            </div>
          </div>
          {_build_upgrade_html(setup.get('upgrade_conditions', []))}
        </div>
      </div>

      <div class="card-body">
        <!-- CHARTS -->
        <div class="card-section" style="padding:4px 0;">
          {f'<img src="data:image/png;base64,{chart_data["daily_b64"]}" style="width:100%;border-radius:4px;margin-bottom:4px;" />' if chart_data.get("daily_b64") else '<div style="color:#555;font-size:0.8em;">No daily chart</div>'}
          {f'<img src="data:image/png;base64,{chart_data["weekly_b64"]}" style="width:100%;border-radius:4px;" />' if chart_data.get("weekly_b64") else '<div style="color:#555;font-size:0.8em;">No weekly chart</div>'}
        </div>

        <!-- TRADE LEVELS -->
        <div class="card-section">
          <h4>TRADE LEVELS</h4>
          <div class="levels-grid">
            <div class="level-box" style="border-color:#22c55e;">
              <div class="level-label">Entry Zone</div>
              <div class="level-value">{_fmt(ez.get('entry_low'))} - {_fmt(ez.get('entry_high'))}</div>
              <div class="level-note">{'IN ZONE' if ez.get('in_zone') else 'Outside zone'}</div>
            </div>
            <div class="level-box" style="border-color:#ef4444;">
              <div class="level-label">Stop</div>
              <div class="level-value">{_fmt(ez.get('stop'))}</div>
              <div class="level-note">Risk/sh: {_fmt(ez.get('risk_per_share'))}</div>
            </div>
            <div class="level-box" style="border-color:#3b82f6;">
              <div class="level-label">Target 1</div>
              <div class="level-value">{_fmt(ez.get('target_1'))}</div>
              <div class="level-note">R:R {ez.get('rr_t1', '--')}:1</div>
            </div>
            <div class="level-box" style="border-color:#8b5cf6;">
              <div class="level-label">Target 2</div>
              <div class="level-value">{_fmt(ez.get('target_2'))}</div>
              <div class="level-note">R:R {ez.get('rr_t2', '--')}:1</div>
            </div>
          </div>

          <div class="detail-row" style="margin-top:6px;">
            <span class="detail-label">Position</span>
            <span>{ps.get('shares', '--')} shares | ${_fmt(ps.get('risk_dollars'))} risk ({ps.get('risk_pct', '--')}% acct) | Group: {ps.get('group', '--')}</span>
          </div>
        </div>

        <!-- PIVOTS -->
        <div class="card-section">
          <h4>DAILY PIVOTS</h4>
          {pivot_html if pivot_html else '<span style="color:#555;">No pivot data</span>'}
          <div style="color:#555;font-size:0.8em;margin-top:4px;">R levels = profit targets / resistance | S levels = support / stop references</div>
        </div>

        <!-- AVWAP MAP -->
        <div class="card-section">
          <h4>ANCHORED VWAP MAP</h4>
          <table class="inner-table">
            <tr><th>Anchor</th><th>AVWAP</th><th>Date</th><th>Distance</th></tr>
            {avwap_rows}
          </table>
          <div style="margin-top:8px;padding:6px;background:#0a0a0a;border-radius:3px;">
            <span style="color:#eab308;font-weight:700;">INTRADAY VWAP:</span>
            <span style="margin-left:8px;">Daily VWAP: <span style="color:#fff;">{_fmt(session_vwaps.get('daily_vwap'))}</span></span>
            <span style="margin-left:12px;">2-Day VWAP: <span style="color:#fff;">{_fmt(session_vwaps.get('two_day_vwap'))}</span></span>
            <span style="margin-left:12px;">YTD AVWAP: <span style="color:#fff;">{_fmt(avwaps.get('ytd', {}).get('avwap'))}</span></span>
            <span style="margin-left:12px;">WTD AVWAP: <span style="color:#fff;">{_fmt(avwaps.get('wtd', {}).get('avwap'))}</span></span>
            <span style="margin-left:12px;">MTD AVWAP: <span style="color:#fff;">{_fmt(avwaps.get('mtd', {}).get('avwap'))}</span></span>
          </div>
        </div>

        <!-- MA STATE -->
        <div class="card-section">
          <h4>MOVING AVERAGE STATE + DIRECTION</h4>
          {daily_ma_html}
          <div style="margin-top:8px;">{weekly_ma_html}</div>
          <div style="margin-top:8px;">{intra_ma_html}</div>
          {sma5_tmw_html}
        </div>

        <!-- KEY LEVELS -->
        <div class="card-section">
          <h4>KEY LEVELS</h4>
          <div class="detail-row">
            <span class="detail-label">Recent High</span>
            <span>{_fmt(rh.get('price'))} ({rh.get('date', '--')})</span>
            <span class="detail-label" style="margin-left:16px;">Recent Low</span>
            <span>{_fmt(rl.get('price'))} ({rl.get('date', '--')})</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Prior Day</span>
            <span>H {_fmt(refs.get('prior_day_high'))} | L {_fmt(refs.get('prior_day_low'))} | C {_fmt(refs.get('prior_day_close'))}</span>
            <span style="color:#666;margin-left:8px;">({refs.get('prior_day_date', '--')})</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">RS 20d</span>
            <span style="color:{'#22c55e' if (rs.get('rs_20d') or 0) > 0 else '#ef4444'};">{_pct(rs.get('rs_20d'))}</span>
            <span class="detail-label" style="margin-left:12px;">RS 60d</span>
            <span style="color:{'#22c55e' if (rs.get('rs_60d') or 0) > 0 else '#ef4444'};">{_pct(rs.get('rs_60d'))}</span>
            <span class="detail-label" style="margin-left:12px;">Confluence</span>
            <span>{conf.get('score', 0)} levels ({', '.join(conf.get('support_names', [])[:3])})</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Earnings</span>
            <span style="color:{'#ef4444' if earn.get('warning') else '#888'};">{earn.get('note', '--')}</span>
          </div>
        </div>

        <!-- SCORE BREAKDOWN -->
        <div class="card-section">
          <h4>SCORE BREAKDOWN</h4>
          <div style="color:#888;font-size:0.85em;">
            {'<br>'.join(sc.get('reasons', ['--']))}
          </div>
        </div>

        <!-- TRADE PLAN (AI narrative) -->
        {f'''<div class="card-section" style="background:#0d1117;border:1px solid #1f6feb;border-radius:4px;padding:10px;margin-top:6px;">
          <h4 style="color:#58a6ff;">TRADE PLAN (AI)</h4>
          <div style="color:#ccc;white-space:pre-wrap;font-size:0.9em;line-height:1.6;">{narrative_text}</div>
        </div>''' if narrative_text else ''}

        <!-- CHECKLIST -->
        <div class="card-section">
          <h4>CHECKLIST ({cl.get('passed', 0)}/{cl.get('total', 0)})</h4>
          {checklist_html if checklist_html else '<span style="color:#555;">No checklist</span>'}
        </div>
      </div>
    </div>"""

    return card


def generate_dashboard(regime: dict, packets: dict, checklists: dict,
                       soxx_decision: dict = None,
                       narratives: dict = None,
                       leveraged: dict = None,
                       chart_images: dict = None,
                       output_path: Optional[Path] = None) -> Path:
    """Generate a static HTML dashboard."""
    narratives = narratives or {}
    leveraged = leveraged or {}
    chart_images = chart_images or {}
    output_path = output_path or cfg.BASE_DIR / "dashboard.html"
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Sort: watchlist first (by score desc), then benchmarks
    wl_syms = sorted(
        [s for s in packets if s in cfg.WATCHLIST],
        key=lambda s: packets[s].get("score", {}).get("score", 0),
        reverse=True,
    )
    bm_syms = [s for s in cfg.BENCHMARKS if s in packets]

    # --- Summary table rows ---
    summary_rows = ""
    for sym in wl_syms:
        pkt = packets[sym]
        sc = pkt.get("score", {})
        ez = pkt.get("entry_zone", {})
        setup = pkt.get("setup", {})
        score = sc.get("score", 0)
        color, bg = _score_color(score)
        in_zone = "YES" if ez.get("in_zone") else "no"
        zone_style = f'color:#22c55e;font-weight:700;' if ez.get("in_zone") else ""

        summary_rows += f"""
        <tr style="border-bottom:1px solid #222;">
          <td style="color:{color};font-weight:700;">{sym}</td>
          <td style="background:{bg};color:{color};text-align:center;font-weight:700;">{score}</td>
          <td>{sc.get('action_bias', '--')}</td>
          <td>{setup.get('type', '--')}</td>
          <td>{_fmt(pkt.get('daily',{}).get('last_close'))}</td>
          <td>{_fmt(ez.get('entry_low'))} - {_fmt(ez.get('entry_high'))}</td>
          <td style="{zone_style}">{in_zone}</td>
          <td style="color:#ef4444;">{_fmt(ez.get('stop'))}</td>
          <td style="color:#22c55e;">{_fmt(ez.get('target_1'))}</td>
          <td>{ez.get('rr_t1', '--')}:1</td>
          <td>{pkt.get('relative_strength',{}).get('rs_20d', '--')}</td>
        </tr>"""

    # --- Regime ---
    r = regime.get("regime", "?").upper().replace("_", " ")
    r_color = {"BULLISH": "#22c55e", "LEAN BULLISH": "#86efac", "NEUTRAL": "#eab308",
               "LEAN BEARISH": "#f97316", "BEARISH": "#ef4444"}.get(r, "#6b7280")

    flags_html = "".join(f'<span class="flag">{f}</span>' for f in regime.get("caution_flags", []))

    # --- Events ---
    from .events import get_event_context
    evt = get_event_context()
    events_html = ""
    for e in evt.get("upcoming_events", []):
        sev_color = {"high": "#ef4444", "medium": "#eab308", "low": "#6b7280"}
        events_html += f'<div class="event" style="border-left:3px solid {sev_color.get(e["severity"], "#666")};"><strong>{e["event"]}</strong> -- {e["date"]} ({e["days_away"]}d)</div>'
    if not events_html:
        events_html = '<div class="event" style="border-left:3px solid #22c55e;">No events next 7 days</div>'

    # --- SOXX tactical ---
    soxx_html = ""
    if soxx_decision:
        sig = soxx_decision.get("signal", "?")
        sig_color = {"GREEN": "#22c55e", "YELLOW": "#eab308", "RED": "#ef4444"}.get(sig, "#666")
        soxx_detail = ""
        if sig == "GREEN":
            ex = soxx_decision.get("execution", {})
            soxx_detail = f"""
            <div style="margin-top:8px;color:#ccc;font-size:0.9em;">
              Size: {ex.get('tactical_size','--')} ({ex.get('shares','--')} sh) |
              Risk: ${ex.get('risk_dollars','--')} |
              Stop SOXX: {ex.get('stop_soxx_level', soxx_decision.get('soxx_stop','--'))} |
              Max hold: {ex.get('max_hold_days','--')}d
            </div>"""
        elif sig == "YELLOW":
            wf = soxx_decision.get("watch_for", {})
            soxx_detail = f'<div style="margin-top:8px;color:#ccc;font-size:0.9em;">Watch: {wf.get("entry_zone","--")} | Trigger: {wf.get("would_turn_green","--")}</div>'

        soxx_html = f"""
        <div class="section">
          <h2>SOXX / SOXL TACTICAL</h2>
          <div style="display:flex;align-items:center;gap:12px;">
            <div style="width:20px;height:20px;border-radius:50%;background:{sig_color};"></div>
            <span style="font-size:1.2em;font-weight:700;color:{sig_color};">{sig}</span>
            <span style="color:#aaa;">-- {soxx_decision.get('action', '?')}</span>
            <span style="color:#888;margin-left:8px;">SOXX: {_fmt(soxx_decision.get('soxx_price'))}</span>
          </div>
          <p style="color:#999;margin-top:4px;">{soxx_decision.get('reason', '')}</p>
          {soxx_detail}
        </div>"""

    # --- Symbol detail cards ---
    cards_html = ""
    for sym in wl_syms:
        cl = checklists.get(sym, {})
        cards_html += _build_symbol_card(sym, packets[sym], cl, narratives.get(sym), chart_images.get(sym))

    # --- Benchmark summary ---
    bench_html = ""
    for sym in bm_syms:
        pkt = packets[sym]
        d = pkt.get("daily", {})
        sc = pkt.get("score", {})
        score = sc.get("score", 0)
        color, _ = _score_color(score)
        bench_html += f"""
        <div style="display:inline-block;margin-right:16px;margin-bottom:8px;">
          <span style="color:{color};font-weight:700;">{sym}</span>
          <span style="color:#ccc;">{_fmt(d.get('last_close'))}</span>
          <span style="color:#888;">({d.get('ma_stack', '--')})</span>
          <span style="color:{color};">{score}</span>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swing Engine -- {now}</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    background: #0a0a0a; color: #e0e0e0;
    padding: 16px; max-width: 1400px; margin: 0 auto;
    font-size: 12px; line-height: 1.5;
  }}
  h1 {{ font-size: 1.4em; color: #fff; margin-bottom: 4px; }}
  h2 {{ font-size: 1.05em; color: #aaa; margin-bottom: 8px; border-bottom: 1px solid #333; padding-bottom: 4px; }}
  h3 {{ font-size: 0.95em; color: #fff; margin-bottom: 6px; }}
  h4 {{ font-size: 0.85em; color: #666; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .timestamp {{ color: #555; font-size: 0.85em; margin-bottom: 14px; }}
  .section {{ background: #111; border: 1px solid #222; border-radius: 6px; padding: 12px; margin-bottom: 12px; }}
  .regime-bar {{
    background: #111; border: 1px solid #333; border-radius: 6px;
    padding: 10px 14px; margin-bottom: 12px;
    display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
  }}
  .regime-label {{ font-size: 1.2em; font-weight: 700; }}
  .regime-detail {{ color: #aaa; font-size: 0.9em; }}
  .flag {{ background: #1a0000; border: 1px solid #ef4444; border-radius: 3px; padding: 2px 6px; font-size: 0.8em; color: #ef4444; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
  th {{ text-align: left; color: #555; font-weight: 400; padding: 4px 6px; border-bottom: 1px solid #333; }}
  td {{ padding: 4px 6px; }}
  .inner-table {{ font-size: 0.9em; }}
  .inner-table th {{ font-size: 0.85em; color: #555; padding: 2px 5px; }}
  .inner-table td {{ padding: 2px 5px; }}
  .event {{ padding: 5px 10px; margin-bottom: 4px; font-size: 0.9em; }}
  .symbol-card {{ background: #111; border: 1px solid #222; border-radius: 6px; margin-bottom: 12px; overflow: hidden; }}
  .card-header {{ padding: 10px 12px; background: #0d0d0d; }}
  .card-body {{ padding: 0 12px 10px; }}
  .card-section {{ padding: 8px 0; border-bottom: 1px solid #1a1a1a; }}
  .card-section:last-child {{ border-bottom: none; }}
  .sym-name {{ font-size: 1.3em; font-weight: 700; color: #fff; }}
  .levels-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; }}
  .level-box {{ border: 1px solid #333; border-radius: 4px; padding: 6px 8px; text-align: center; border-top-width: 2px; }}
  .level-label {{ font-size: 0.75em; color: #888; text-transform: uppercase; }}
  .level-value {{ font-size: 1.1em; color: #fff; font-weight: 700; margin: 2px 0; }}
  .level-note {{ font-size: 0.8em; color: #666; }}
  .detail-row {{ display: flex; flex-wrap: wrap; gap: 4px; align-items: center; padding: 2px 0; font-size: 0.9em; }}
  .detail-label {{ color: #666; min-width: 80px; }}
  .check-item {{ display: flex; gap: 6px; padding: 2px 0; font-size: 0.85em; }}
  .verdict {{ margin-top: 6px; font-weight: 700; font-size: 0.9em; }}
  @media (max-width: 768px) {{
    body {{ font-size: 10px; padding: 8px; }}
    .levels-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .detail-row {{ font-size: 0.85em; }}
  }}
</style>
</head>
<body>

<h1>SWING ENGINE</h1>
<div class="timestamp">{now}</div>

<div class="regime-bar">
  <span class="regime-label" style="color:{r_color};">{r}</span>
  <span class="regime-detail">
    Bias: {regime.get('swing_bias', '?')} |
    Risk: {regime.get('risk_appetite', '?')} |
    VIX: {regime.get('vix_context', '?')} ({regime.get('vix_level', '?')}) |
    Signals: {regime.get('bull_signals', '?')}/{regime.get('total_signals', '?')}
  </span>
  {flags_html}
</div>

<div class="section">
  <h2>BENCHMARKS</h2>
  {bench_html}
</div>

<div class="section">
  <h2>EVENTS (NEXT 7 DAYS)</h2>
  {events_html}
</div>

{soxx_html}

{_build_leveraged_html(leveraged)}

<div class="section">
  <h2>WATCHLIST SUMMARY</h2>
  <table>
    <tr>
      <th>Sym</th><th>Score</th><th>Bias</th><th>Setup</th><th>Price</th>
      <th>Entry Zone</th><th>Zone?</th><th>Stop</th><th>Target 1</th><th>R:R</th><th>RS20</th>
    </tr>
    {summary_rows}
  </table>
</div>

<h2 style="color:#aaa;margin:16px 0 8px;border-bottom:1px solid #333;padding-bottom:4px;">SYMBOL DETAIL CARDS</h2>
{cards_html}

</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    print(f"  Dashboard written: {output_path}")
    return output_path
