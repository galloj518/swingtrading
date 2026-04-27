"""
Expansion stability analysis across time, regimes, and symbols.

Research-only analysis that widens the walk-forward sample by combining all
saved walk-forward JSON reports, deduping by symbol/date, and then validating
expansion-zone behavior across regimes, symbols, and time periods.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg
from . import data as mdata


OUTPUT_STEM = f"expansion_stability_analysis_{date.today().isoformat()}"
LOW_SAMPLE_THRESHOLD = max(8, int(cfg.RESEARCH_MIN_GROUP_SIZE) // 2)
STABLE_SAMPLE_THRESHOLD = int(cfg.RESEARCH_MIN_GROUP_SIZE)


def _walkforward_paths() -> list[Path]:
    paths = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    if not paths:
        raise FileNotFoundError("No walk-forward backtest reports found.")
    return paths


def _load_latest_rows(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows", []) if isinstance(data, dict) else data
    return pd.DataFrame(rows)


def _load_expanded_rows() -> tuple[pd.DataFrame, dict]:
    raw_rows: list[dict] = []
    for path in _walkforward_paths():
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if symbol not in set(cfg.BACKTEST_SYMBOLS):
                continue
            raw_rows.append({**row, "_source_file": path.name})
    if not raw_rows:
        return pd.DataFrame(), {"before_rows": 0, "after_rows": 0, "source_files": []}

    raw_rows.sort(key=lambda row: (str(row.get("symbol")), str(row.get("date")), str(row.get("_source_file"))))
    deduped: dict[tuple[str, str], dict] = {}
    for row in raw_rows:
        key = (str(row.get("symbol")), str(row.get("date")))
        deduped[key] = row

    latest_path = _walkforward_paths()[-1]
    latest_rows = _load_latest_rows(latest_path)
    latest_rows = latest_rows[latest_rows.get("symbol", pd.Series(dtype="object")).isin(cfg.BACKTEST_SYMBOLS)]

    frame = pd.DataFrame(deduped.values())
    meta = {
        "before_rows": int(len(latest_rows)),
        "after_rows": int(len(frame)),
        "source_files": sorted({str(row["_source_file"]) for row in deduped.values()}),
        "before_path": str(latest_path),
    }
    return frame, meta


def _expansion_zone(value: float) -> str:
    if pd.isna(value) or value <= 0:
        return "no_expansion"
    if value == 1:
        return "early_expansion"
    return "overextended_expansion"


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for column in [
        "realized_r",
        "return_5d",
        "return_10d",
        "range_ratio",
        "volume_ratio",
        "expansion_score",
    ]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])
    out["symbol"] = out["symbol"].astype(str)
    out["setup_state"] = out.get("setup_state", pd.Series(dtype="object")).fillna("unknown").astype(str)
    out["avwap_location_quality"] = out.get("avwap_location_quality", pd.Series(dtype="object")).fillna("unknown").astype(str)
    out["expansion_zone"] = out["expansion_score"].apply(_expansion_zone)
    out["year"] = out["date"].dt.year.astype(str)
    out["quarter"] = out["date"].dt.to_period("Q").astype(str)
    out["simple_regime"] = out["date"].dt.strftime("%Y-%m-%d").map(_simple_regime_map()).fillna(
        out.get("regime", pd.Series(dtype="object")).apply(_fallback_regime_label)
    )
    return out


def _fallback_regime_label(value) -> str:
    text = str(value or "").lower()
    if "bear" in text:
        return "bear_trend"
    if "bull" in text:
        return "bull_trend"
    return "sideways"


def _simple_regime_map() -> dict[str, str]:
    spy = mdata.load_daily("SPY")
    if spy.empty or "date" not in spy.columns:
        return {}
    frame = spy.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    frame["close"] = pd.to_numeric(frame.get("close"), errors="coerce")
    frame["sma50"] = frame["close"].rolling(50).mean()
    frame["sma200"] = frame["close"].rolling(200).mean()
    frame["sma50_slope20"] = frame["sma50"] - frame["sma50"].shift(20)

    def classify(row) -> str:
        if pd.isna(row["sma50"]) or pd.isna(row["sma200"]) or pd.isna(row["sma50_slope20"]) or pd.isna(row["close"]):
            return "sideways"
        if row["close"] > row["sma200"] and row["sma50"] > row["sma200"] and row["sma50_slope20"] > 0:
            return "bull_trend"
        if row["close"] < row["sma200"] and row["sma50"] < row["sma200"] and row["sma50_slope20"] < 0:
            return "bear_trend"
        return "sideways"

    frame["simple_regime"] = frame.apply(classify, axis=1)
    return {row["date"].strftime("%Y-%m-%d"): row["simple_regime"] for _, row in frame.iterrows()}


def _summary_metrics(df: pd.DataFrame) -> dict:
    realized = pd.to_numeric(df.get("realized_r", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret5 = pd.to_numeric(df.get("return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret10 = pd.to_numeric(df.get("return_10d", pd.Series(dtype="float64")), errors="coerce").dropna()

    def winsorized_mean(series: pd.Series) -> float | None:
        if series.empty:
            return None
        low = series.quantile(0.05)
        high = series.quantile(0.95)
        return float(series.clip(lower=low, upper=high).mean())

    win_mean = winsorized_mean(realized)
    mean_r = float(realized.mean()) if not realized.empty else None
    return {
        "sample_size": int(len(df)),
        "avg_realized_R": round(mean_r, 4) if mean_r is not None else None,
        "median_realized_R": round(float(realized.median()), 4) if not realized.empty else None,
        "win_rate": round(float((realized > 0).mean()), 4) if not realized.empty else None,
        "avg_return_5d": round(float(ret5.mean()), 4) if not ret5.empty else None,
        "avg_return_10d": round(float(ret10.mean()), 4) if not ret10.empty else None,
        "realized_R_stddev": round(float(realized.std(ddof=0)), 4) if not realized.empty else None,
        "realized_R_q25": round(float(realized.quantile(0.25)), 4) if not realized.empty else None,
        "realized_R_q75": round(float(realized.quantile(0.75)), 4) if not realized.empty else None,
        "winsorized_avg_realized_R": round(win_mean, 4) if win_mean is not None else None,
        "outlier_impact_R": round(float(mean_r - win_mean), 4) if mean_r is not None and win_mean is not None else None,
    }


def _group_metrics(frame: pd.DataFrame, fields: list[str]) -> dict:
    if frame.empty:
        return {}
    out = {}
    for keys, subset in frame.groupby(fields, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        name = " | ".join(f"{field}={value}" for field, value in zip(fields, keys))
        out[name] = _summary_metrics(subset)
    return out


def _zone_period_positive_share(frame: pd.DataFrame, period_field: str) -> dict:
    out = {}
    for zone, subset in frame.groupby("expansion_zone"):
        grouped = subset.groupby(period_field)
        period_means = []
        for _, period_df in grouped:
            stats = _summary_metrics(period_df)
            if stats["avg_realized_R"] is not None:
                period_means.append(stats["avg_realized_R"])
        positive_share = None
        if period_means:
            positive_share = round(sum(value > 0 for value in period_means) / len(period_means), 4)
        out[str(zone)] = {
            "period_count": len(period_means),
            "positive_period_share": positive_share,
        }
    return out


def _stability_assessment(frame: pd.DataFrame) -> dict:
    assessment = {}
    for zone in ["no_expansion", "early_expansion", "overextended_expansion"]:
        subset = frame[frame["expansion_zone"] == zone]
        regime_groups = {k: _summary_metrics(v) for k, v in subset.groupby("simple_regime")}
        symbol_groups = {k: _summary_metrics(v) for k, v in subset.groupby("symbol")}
        quarter_groups = {k: _summary_metrics(v) for k, v in subset.groupby("quarter")}

        valid_regime_values = [row["avg_realized_R"] for row in regime_groups.values() if row["avg_realized_R"] is not None and row["sample_size"] >= LOW_SAMPLE_THRESHOLD]
        valid_quarter_values = [row["avg_realized_R"] for row in quarter_groups.values() if row["avg_realized_R"] is not None and row["sample_size"] >= LOW_SAMPLE_THRESHOLD]

        label = "unstable / regime-dependent"
        if len(valid_regime_values) >= 2 and len(valid_quarter_values) >= 2:
            same_regime_sign = all(value > 0 for value in valid_regime_values) or all(value <= 0 for value in valid_regime_values)
            same_quarter_sign = all(value > 0 for value in valid_quarter_values) or all(value <= 0 for value in valid_quarter_values)
            if same_regime_sign and same_quarter_sign:
                label = "stable signal"

        assessment[zone] = {
            "label": label,
            "regime_positive_period_share": _zone_period_positive_share(subset, "simple_regime").get(zone, {}),
            "quarter_positive_period_share": _zone_period_positive_share(subset, "quarter").get(zone, {}),
            "symbol_count": int(subset["symbol"].nunique()),
            "low_sample_symbols": sorted(
                symbol for symbol, row in symbol_groups.items() if int(row.get("sample_size") or 0) < LOW_SAMPLE_THRESHOLD
            ),
        }
    return assessment


def analyze_expansion_stability() -> dict:
    raw_frame, dataset_meta = _load_expanded_rows()
    frame = _prepare_frame(raw_frame)

    results_by_zone = {
        zone: _summary_metrics(frame[frame["expansion_zone"] == zone])
        for zone in ["no_expansion", "early_expansion", "overextended_expansion"]
    }
    results_by_regime = _group_metrics(frame, ["expansion_zone", "simple_regime"])
    results_by_symbol = _group_metrics(frame, ["expansion_zone", "symbol"])
    results_by_time = {
        "year": _group_metrics(frame, ["expansion_zone", "year"]),
        "quarter": _group_metrics(frame, ["expansion_zone", "quarter"]),
    }

    top_bottom_symbols = {}
    for zone in ["no_expansion", "early_expansion", "overextended_expansion"]:
        rows = []
        for symbol, subset in frame[frame["expansion_zone"] == zone].groupby("symbol"):
            stats = _summary_metrics(subset)
            rows.append({"symbol": symbol, **stats})
        usable = [row for row in rows if row["avg_realized_R"] is not None and row["sample_size"] >= LOW_SAMPLE_THRESHOLD]
        usable.sort(key=lambda row: row["avg_realized_R"], reverse=True)
        top_bottom_symbols[zone] = {
            "top": usable[:5],
            "bottom": usable[-5:] if usable else [],
        }

    return {
        "status": "ok",
        "dataset_size": dataset_meta,
        "date_range": {
            "min_date": frame["date"].min().strftime("%Y-%m-%d") if not frame.empty else None,
            "max_date": frame["date"].max().strftime("%Y-%m-%d") if not frame.empty else None,
        },
        "results_by_expansion_zone": results_by_zone,
        "results_by_regime": results_by_regime,
        "results_by_symbol": results_by_symbol,
        "results_by_time_period": results_by_time,
        "top_bottom_symbols": top_bottom_symbols,
        "stability_assessment": _stability_assessment(frame),
    }


def render_expansion_stability_analysis(result: dict) -> str:
    dataset = result.get("dataset_size", {})
    lines = [
        "=" * 60,
        "EXPANSION STABILITY ANALYSIS",
        "=" * 60,
        f"BEFORE ROWS: {dataset.get('before_rows')}",
        f"AFTER ROWS: {dataset.get('after_rows')}",
        f"DATE RANGE: {result.get('date_range', {}).get('min_date')} -> {result.get('date_range', {}).get('max_date')}",
        f"SOURCE FILES: {', '.join(dataset.get('source_files', []))}",
        "",
        "RESULTS BY EXPANSION ZONE",
    ]
    for zone, stats in result.get("results_by_expansion_zone", {}).items():
        lines.append(
            f"{zone}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, medR={stats['median_realized_R']}, "
            f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}, "
            f"std={stats['realized_R_stddev']}, outlier_impact={stats['outlier_impact_R']}"
        )

    lines.append("")
    lines.append("RESULTS BY REGIME")
    for name, stats in sorted(result.get("results_by_regime", {}).items()):
        lines.append(
            f"{name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
            f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
        )

    lines.append("")
    lines.append("TOP / BOTTOM SYMBOLS")
    for zone, payload in result.get("top_bottom_symbols", {}).items():
        lines.append(f"{zone}:")
        lines.append("  top:")
        for row in payload.get("top", []):
            lines.append(f"    {row['symbol']}: n={row['sample_size']}, avgR={row['avg_realized_R']}, win={row['win_rate']}")
        lines.append("  bottom:")
        for row in payload.get("bottom", []):
            lines.append(f"    {row['symbol']}: n={row['sample_size']}, avgR={row['avg_realized_R']}, win={row['win_rate']}")

    lines.append("")
    lines.append("STABILITY ASSESSMENT")
    for zone, payload in result.get("stability_assessment", {}).items():
        lines.append(
            f"{zone}: {payload['label']}, symbol_count={payload['symbol_count']}, "
            f"regime_positive_share={payload['regime_positive_period_share'].get('positive_period_share')}, "
            f"quarter_positive_share={payload['quarter_positive_period_share'].get('positive_period_share')}"
        )
        if payload.get("low_sample_symbols"):
            lines.append(f"  low_sample_symbols: {', '.join(payload['low_sample_symbols'])}")
    return "\n".join(lines)


def run_expansion_stability_analysis(save: bool = True) -> dict:
    result = analyze_expansion_stability()
    text = render_expansion_stability_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
