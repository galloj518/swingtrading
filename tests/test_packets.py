from __future__ import annotations

import pandas as pd

from swing_engine import packets
from tests.helpers import make_bundle


def test_packet_build_survives_missing_intraday_data(synthetic_daily_frame):
    bundle = make_bundle(synthetic_daily_frame)
    packet = packets.build_packet("TEST", bundle, synthetic_daily_frame)
    assert packet["intraday_trigger"]["trigger_state"] == "data_unavailable"
    assert packet["score"]["setup_state"] in {"FORMING", "BREAKOUT_WATCH", "TRIGGER_WATCH", "DATA_UNAVAILABLE", "ACTIONABLE_BREAKOUT", "ACTIONABLE_RETEST", "ACTIONABLE_RECLAIM", "EXTENDED_WAIT", "FAILED", "BLOCKED"}


def test_packet_build_marks_missing_benchmark_context(synthetic_daily_frame, synthetic_intraday_frame):
    bundle = make_bundle(synthetic_daily_frame, synthetic_intraday_frame)
    empty_spy = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    packet = packets.build_packet("TEST", bundle, empty_spy, regime={"quality": "degraded"})
    assert packet["context_quality"]["benchmark_status"] == "unavailable"
