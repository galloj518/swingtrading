# Swing Engine

Deterministic breakout-first swing scanning built around yfinance, cached market data, structured run health, and optional narrative post-processing.

## Entry Point

The authoritative CLI entrypoint is `swing_engine/__main__.py`.

Run commands with:

```bash
python -m swing_engine --help
```

## Scan Modes

```bash
python -m swing_engine run
python -m swing_engine run --with-narratives
python -m swing_engine run-structural
python -m swing_engine run-breakout-watch
python -m swing_engine run-triggers
python -m swing_engine run-narratives
python -m swing_engine smoke
```

Mode behavior:

- `run` is deterministic by default.
- `run --with-narratives` explicitly opts into LLM post-processing.
- `run-structural` is structural quality only and does not invoke narratives.
- `run-breakout-watch` is deterministic and does not invoke narratives.
- `run-triggers` is deterministic and does not invoke narratives.
- `run-narratives` is the only frequent-scan-adjacent mode that may call the narrative layer.
- `smoke` is a fully offline deterministic smoke run with synthetic market data.

## Architecture

The repo preserves the three-layer scan stack:

1. Structural scan
2. Breakout-readiness scan
3. Deterministic intraday trigger monitor

Frequent runs remain deterministic and do not require `OPENAI_API_KEY`.

## Degraded Runs

A run is marked degraded when the engine sees meaningful operational weakness, including:

- benchmark data unavailable
- cache fallback replacing live data
- symbol-level unavailable packets
- packet build fallback
- trigger logic degraded by missing or stale intraday data

Every scan run now emits machine-readable run health JSON under `reports/` with:

- run mode
- timestamp
- duration
- live / cache-fallback / unavailable symbol counts
- packet failure counts
- benchmark availability
- regime degradation flag
- trigger degradation counts
- setup-state and actionability counts
- overall status (`healthy`, `degraded`, `failed`)

## Data and Failure Behavior

The market-data layer is defensive against broken yfinance responses:

- `None`, empty, malformed, and partial responses normalize to empty frames
- symbols may fall back to cache
- fully unavailable symbols degrade to structured unavailable packets
- benchmark failures degrade regime and relative-strength quality instead of crashing the run
- repeated provider failures trip a short cooldown to avoid hammering broken downloads

## Outputs

Source files:

- `templates/dashboard.html`
- `swing_engine/*.py`
- `.github/workflows/swing-engine.yml`

Generated/runtime artifacts:

- `dashboard.html`
- `reports/*.json`
- `reports/offline_smoke_dashboard.html`
- `reports/offline_smoke_report.json`
- `data/cache/*`
- `data/signals.csv`
- `data/swing_engine.sqlite3`

The generated root `dashboard.html` is intentionally not treated as source.

## Local Verification

Install dependencies:

```bash
pip install -r requirements.txt
```

Run compile validation:

```bash
python -m compileall swing_engine
```

Run tests:

```bash
python -m pytest -q
```

Run the deterministic offline smoke path:

```bash
python -m swing_engine smoke
```

The smoke path verifies:

- scan orchestration
- packet / scoring / checklist flow
- dashboard rendering
- run-health reporting
- deterministic no-LLM execution

## CI / Scheduled Operation

The GitHub Actions workflow now performs:

- dependency install
- compile validation
- pytest
- offline smoke verification
- scheduled scan execution

Pages publishing only proceeds when the workflow remains successful.

## Environment Variables

```bash
OPENAI_API_KEY=...   # only used by run-narratives or run --with-narratives
SWING_ENGINE_LOG_LEVEL=INFO
```

Legacy Schwab env vars may still exist in local environments, but market data remains yfinance-only.
