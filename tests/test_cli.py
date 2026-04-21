from __future__ import annotations

import sys

from swing_engine import __main__ as cli


def test_top_level_help_does_not_run_scans(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["python", "--help"])
    monkeypatch.setattr(cli.scan_modes, "run_structural", lambda **_: (_ for _ in ()).throw(AssertionError("scan should not run")))
    cli.main()
    output = capsys.readouterr().out
    assert "python -m swing_engine run-structural" in output


def test_mode_help_does_not_run_scan(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["python", "run-breakout-watch", "--help"])
    monkeypatch.setattr(cli.scan_modes, "run_breakout_watch", lambda **_: (_ for _ in ()).throw(AssertionError("scan should not run")))
    cli.main()
    output = capsys.readouterr().out
    assert "Usage: python -m swing_engine run-breakout-watch [--force]" in output
