"""
Dynamic correlation matrix for live group-risk calculation.

Replaces the static CORRELATION_GROUPS dict with rolling pairwise
correlations computed from actual recent returns. This catches:
  - Semis/mega-tech coupling during macro selloffs
  - Sector rotations where group membership diverges from reality
  - Names that were in different groups but are now co-moving

Falls back to the static group definition when insufficient data exists.

No new dependencies: uses pandas/numpy (already present).
"""
from __future__ import annotations

import pickle
from datetime import date
from pathlib import Path
from typing import Optional, List, Dict

import numpy as np
import pandas as pd

from . import config as cfg
from .constants import ThresholdRegistry as TR


_CORR_CACHE_PATH = cfg.CACHE_DIR / f"correlation_matrix_{date.today().isoformat()}.pkl"


def build_dynamic_correlation_matrix(
    data_store: dict,
    lookback: int = None,
) -> Optional[pd.DataFrame]:
    """
    Build a rolling pairwise return-correlation matrix from the data store.

    Args:
        data_store: Dict of {symbol: {"daily": pd.DataFrame, ...}}
                    as produced by data.load_all().
        lookback: Number of trading days. Defaults to TR.CORR_LOOKBACK_DAYS.

    Returns:
        Square DataFrame indexed/columned by symbol, or None if insufficient data.
    """
    lookback = lookback or getattr(cfg, "DYNAMIC_CORR_LOOKBACK", TR.CORR_LOOKBACK_DAYS)

    # Try cached version first
    if _CORR_CACHE_PATH.exists():
        try:
            with open(_CORR_CACHE_PATH, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass

    closes = {}
    for symbol, data in data_store.items():
        daily = data.get("daily")
        if daily is None or daily.empty:
            continue
        if "close" not in daily.columns:
            continue
        series = daily.set_index("date")["close"].tail(lookback + 1)
        if len(series) >= max(20, lookback // 3):
            closes[symbol] = series

    if len(closes) < 3:
        return None

    price_df = pd.DataFrame(closes)
    ret_df = price_df.pct_change(fill_method=None).dropna(how="all")

    # Require at least 20 non-NaN observations per pair
    corr_matrix = ret_df.corr(min_periods=20)

    # Cache it
    try:
        with open(_CORR_CACHE_PATH, "wb") as f:
            pickle.dump(corr_matrix, f)
    except Exception:
        pass

    return corr_matrix


def get_dynamic_peers(
    symbol: str,
    corr_matrix: pd.DataFrame,
    threshold: float = None,
    max_peers: int = 8,
) -> List[str]:
    """
    Return symbols highly correlated with `symbol` over the rolling window.

    Args:
        symbol: The symbol to find peers for.
        corr_matrix: Output of build_dynamic_correlation_matrix().
        threshold: Correlation above which a symbol is considered a peer.
        max_peers: Cap the peer list to avoid noisy outliers.

    Returns:
        List of peer symbols (excluding self), sorted by correlation descending.
    """
    threshold = threshold if threshold is not None else getattr(cfg, "DYNAMIC_CORR_THRESHOLD", TR.CORR_HIGH_THRESHOLD)

    if corr_matrix is None or symbol not in corr_matrix.columns:
        return _static_peers(symbol)

    row = corr_matrix[symbol].drop(labels=[symbol], errors="ignore")
    peers = row[row >= threshold].sort_values(ascending=False)
    return peers.head(max_peers).index.tolist()


def calc_dynamic_group_risk(
    symbol: str,
    open_positions: Dict[str, float],
    corr_matrix: Optional[pd.DataFrame],
) -> float:
    """
    Calculate the correlation-adjusted group risk for a symbol.

    Instead of a binary "same group = counts" rule, each open position
    contributes to the group risk weighted by its correlation to `symbol`.

    Corr = 1.0: counts fully (same risk bucket).
    Corr = 0.5: counts at 50%.
    Corr <= 0:  not counted.

    Args:
        symbol: Symbol being evaluated for a new position.
        open_positions: Dict of {other_symbol: risk_dollars_at_risk}.
        corr_matrix: Rolling correlation matrix (may be None).

    Returns:
        Total correlated risk in dollars already committed.
    """
    if not open_positions:
        return 0.0

    # If we have a live corr matrix, use it
    if corr_matrix is not None and symbol in corr_matrix.columns:
        total = 0.0
        for other, risk_dollars in open_positions.items():
            if other == symbol:
                total += risk_dollars
                continue
            if other not in corr_matrix.columns:
                # Unknown correlation — use static group membership as fallback
                if _same_static_group(symbol, other):
                    total += risk_dollars * 0.7  # conservative partial credit
                continue
            corr = float(corr_matrix.loc[symbol, other])
            if not np.isfinite(corr):
                corr = 0.0
            weighted = risk_dollars * max(0.0, corr)
            total += weighted
        return round(total, 2)

    # Fallback: static group membership (binary)
    return _static_group_risk(symbol, open_positions)


# ---------------------------------------------------------------------------
# Static group fallbacks
# ---------------------------------------------------------------------------

def _static_peers(symbol: str) -> List[str]:
    """Return static group members when no correlation data is available."""
    for members in cfg.CORRELATION_GROUPS.values():
        if symbol.upper() in [m.upper() for m in members]:
            return [m for m in members if m.upper() != symbol.upper()]
    return []


def _same_static_group(symbol: str, other: str) -> bool:
    for members in cfg.CORRELATION_GROUPS.values():
        upper_members = [m.upper() for m in members]
        if symbol.upper() in upper_members and other.upper() in upper_members:
            return True
    return False


def _static_group_risk(symbol: str, open_positions: Dict[str, float]) -> float:
    """Sum risk dollars for all positions in the same static group."""
    total = 0.0
    for other, risk_dollars in open_positions.items():
        if _same_static_group(symbol, other) or other == symbol:
            total += risk_dollars
    return round(total, 2)


def correlation_summary(corr_matrix: Optional[pd.DataFrame], symbols: List[str]) -> dict:
    """
    Produce a summary of the highest-correlation pairs for the dashboard.

    Returns top pairs above the high-correlation threshold, sorted by
    absolute correlation descending. Useful for identifying concentration risk.
    """
    if corr_matrix is None:
        return {"available": False, "pairs": []}

    pairs = []
    cols = [s for s in symbols if s in corr_matrix.columns]
    threshold = getattr(cfg, "DYNAMIC_CORR_THRESHOLD", TR.CORR_HIGH_THRESHOLD)

    for i, sym_a in enumerate(cols):
        for sym_b in cols[i + 1:]:
            val = corr_matrix.loc[sym_a, sym_b]
            if np.isfinite(val) and abs(val) >= threshold:
                pairs.append({
                    "symbol_a": sym_a,
                    "symbol_b": sym_b,
                    "correlation": round(float(val), 3),
                })

    pairs.sort(key=lambda p: abs(p["correlation"]), reverse=True)

    return {
        "available": True,
        "lookback_days": getattr(cfg, "DYNAMIC_CORR_LOOKBACK", TR.CORR_LOOKBACK_DAYS),
        "threshold": threshold,
        "high_correlation_pairs": len(pairs),
        "pairs": pairs[:20],  # top 20 for display
    }
