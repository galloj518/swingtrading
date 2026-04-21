from __future__ import annotations

from pathlib import Path

from swing_engine import scan_modes
from tests.helpers import make_bundle


def test_breakout_watch_run_survives_symbol_failures(monkeypatch, workspace_tmp_path, synthetic_daily_frame, synthetic_intraday_frame):
    monkeypatch.setattr(scan_modes.db, "initialize", lambda: None)
    monkeypatch.setattr(scan_modes.mdata, "clean_old_cache", lambda: None)
    monkeypatch.setattr(scan_modes.cfg, "WATCHLIST", ["GOOD", "BAD"])
    monkeypatch.setattr(scan_modes.cfg, "BENCHMARKS", ["SPY", "QQQ", "SOXX", "DIA"])
    monkeypatch.setattr(scan_modes.cfg, "REPORTS_DIR", workspace_tmp_path)
    monkeypatch.setattr(scan_modes.cfg, "RUN_HEALTH_OUTPUT_DIR", workspace_tmp_path)

    def fake_load_daily(symbol: str, force: bool = False):
        return synthetic_daily_frame.copy()

    def fake_load_all(symbol: str, force: bool = False):
        if symbol in {"SPY", "QQQ", "SOXX", "DIA"}:
            return make_bundle(synthetic_daily_frame, synthetic_intraday_frame)
        if symbol == "BAD":
            return make_bundle(synthetic_daily_frame)
        return make_bundle(synthetic_daily_frame, synthetic_intraday_frame)

    monkeypatch.setattr(scan_modes.mdata, "load_daily", fake_load_daily)
    monkeypatch.setattr(scan_modes.mdata, "load_all", fake_load_all)
    monkeypatch.setattr(scan_modes.mdata, "load_macro_signals", lambda force=False: {})
    monkeypatch.setattr(scan_modes.mdata, "load_vix", lambda force=False: synthetic_daily_frame.copy())
    monkeypatch.setattr(scan_modes.calibration, "build_calibration_profile", lambda: {"available": False, "global": {"sample_size": 0, "score": 50.0, "success_rate": 0.55, "avg_outcome": 0.0}})
    monkeypatch.setattr(scan_modes.packets, "enrich_calibration", lambda packets_map, calibration_profile, regime=None: None)
    monkeypatch.setattr(scan_modes.packets, "save_packet", lambda symbol, packet: Path(workspace_tmp_path / f"{symbol}.json"))
    monkeypatch.setattr(scan_modes.dashboard, "generate_dashboard", lambda *args, **kwargs: workspace_tmp_path / "dashboard.html")

    original_build_packet = scan_modes.packets.build_packet

    def fake_build_packet(symbol, data, spy_daily, **kwargs):
        if symbol == "BAD":
            raise RuntimeError("boom")
        return original_build_packet(symbol, data, spy_daily, **kwargs)

    monkeypatch.setattr(scan_modes.packets, "build_packet", fake_build_packet)

    context = scan_modes.run_breakout_watch(include_dashboard=False)
    assert context["packets"]["BAD"]["score"]["setup_state"] == "DATA_UNAVAILABLE"
    assert context["run_health"]["packet_build_failures"] == 1
    assert context["run_health"]["overall_status"] == "degraded"
