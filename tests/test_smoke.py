from __future__ import annotations

from swing_engine import smoke


def test_offline_smoke_runs(workspace_tmp_path, monkeypatch):
    monkeypatch.setattr(smoke.cfg, "OFFLINE_SMOKE_OUTPUT_DIR", workspace_tmp_path)
    monkeypatch.setattr(smoke.cfg, "DASHBOARD_OUTPUT_PATH", workspace_tmp_path / "dashboard.html")
    result = smoke.run_offline_smoke(include_dashboard=True)
    assert result["run_health"]["overall_status"] in {"healthy", "degraded"}
    assert (workspace_tmp_path / "offline_smoke_report.json").exists()
    assert (workspace_tmp_path / "offline_smoke_dashboard.html").exists()
