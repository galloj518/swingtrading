"""
Shared math utilities used across features and scoring modules.

Single source of truth for band/linear ratio helpers — avoids duplication
between features.py and scoring.py.
"""


def _linear_ratio(value: float, low: float, high: float) -> float:
    """Clamp-normalise value into [0, 1] across [low, high]."""
    if high == low:
        return 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


def _band_ratio(value: float, outer_low: float, ideal_low: float,
                ideal_high: float, outer_high: float) -> float:
    """
    Score how well a value falls inside an ideal band.

    Returns 1.0 inside [ideal_low, ideal_high].
    Returns 0.0 outside [outer_low, outer_high].
    Linear ramp between outer and ideal edges.

    Example — distance from 20 SMA (%):
        outer_low=-8, ideal_low=-2, ideal_high=2.5, outer_high=8
        value=-1.5 -> 1.0  (in the ideal pullback zone)
        value=-5.0 -> 0.5  (mid-ramp below ideal)
        value=-9.0 -> 0.0  (outside outer bound)
    """
    if value <= outer_low or value >= outer_high:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < ideal_low:
        return _linear_ratio(value, outer_low, ideal_low)
    return _linear_ratio(outer_high - value, 0.0, outer_high - ideal_high)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))
