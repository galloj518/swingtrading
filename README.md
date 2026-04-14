# Swing Engine

A deterministic, price-action-first swing trading tool for personal use.

## Philosophy

- **Price action dominates** — Brian Shannon / Alphtrends methodology
- **Gated scoring** — weekly trend must pass before daily matters
- **No LLM in the critical path** — all decisions are deterministic and repeatable
- **Behavioral edge** — checklists and tracking enforce discipline
- **Simplicity over cleverness** — every component must earn its place

## Architecture

```
yfinance (market data)
    ↓
pandas-ta (standard indicators) + custom (AVWAP, gated scoring, confluence)
    ↓
Gated Scoring Engine (weekly gate → daily gate → entry quality)
    ↓
Deterministic Regime Model (SPY/QQQ/SOXX/DIA/VIX)
    ↓
Pre-Trade Checklists + Signal/Trade Store (SQLite + CSV)
    ↓
Static HTML Dashboard → Vercel (mobile access)
```

## Setup

```bash
pip install -r requirements.txt
```

Set environment variables (optional, for Schwab account data):
```
SCHWAB_APP_KEY=...
SCHWAB_APP_SECRET=...
SCHWAB_REFRESH_TOKEN=...
```

## Usage

```bash
# Morning pre-market run (full analysis)
python -m swing_engine run

# Single symbol check
python -m swing_engine check NVDA

# SOXX→SOXL tactical
python -m swing_engine soxx

# Weekly review of trade journal
python -m swing_engine review

# Sync legacy CSV history into the SQLite store
python -m swing_engine db-sync

# Generate HTML dashboard
python -m swing_engine dashboard
```

## GitHub Travel Setup

This project can run on GitHub Actions while your home computer is off.

- The workflow lives at `.github/workflows/swing-engine.yml`
- It runs on weekdays and can also be started manually
- It updates `data/swing_engine.sqlite3`, `data/signals.csv`, `reports/`, and `dashboard.html`
- It publishes the latest dashboard to GitHub Pages for phone access

Recommended repository settings:

- Enable GitHub Pages and set the source to `GitHub Actions`
- Add `OPENAI_API_KEY` only if you want AI narratives
- Add `SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`, and `SCHWAB_REFRESH_TOKEN` only if you want Schwab-linked features

Phone access pattern:

- Open the GitHub Pages URL for the latest dashboard
- Use the repository itself as the durable store for the SQLite database and reports

## File Outputs

- `data/signals.csv` — daily signal log with outcome backfill
- `data/journal.csv` — trade journal (manual entry + helpers)
- `data/swing_engine.sqlite3` — durable signal/trade database
- `data/cache/` — cached market data CSVs
- `reports/` — daily JSON reports
- `dashboard.html` — static dashboard for Vercel deployment

## Project Structure

```
swing_engine/
├── __init__.py
├── __main__.py          # CLI entry points
├── config.py            # All configuration
├── data.py              # Market data loading + caching
├── features.py          # SMAs, ATR, pivots, AVWAP, RS
├── scoring.py           # Gated scoring engine
├── regime.py            # Deterministic regime model
├── sizing.py            # Position sizing + correlation groups
├── packets.py           # Packet builder
├── signals.py           # Signal logging + outcome tracking
├── soxx_tactical.py     # SOXX→SOXL dedicated module
├── checklist.py         # Pre-trade checklist generator
├── dashboard.py         # Static HTML generator
├── review.py            # Weekly review script
templates/
├── dashboard.html       # Jinja2 template for dashboard
sandbox.ipynb            # Ad-hoc exploration notebook
requirements.txt
```
