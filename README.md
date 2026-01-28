# Kolkata FF Scraper

A Railway-ready scraper and API for Kolkata FF results. The system stores results in SQLite, keeps 60 days of data, posts new updates to Telegram, and exposes a FastAPI backend for WordPress consumption.

## Features
- Scrapes https://kolkataff.tv/ and detects new results via a stable signature.
- Persists data to SQLite on a Railway Volume (`/data/results.db`).
- FastAPI endpoints for latest, past, and by-date results.
- Telegram notifications on newly inserted results only.
- Cron-friendly fetcher with retries and backoff.

## Environment Variables
- `SITE_URL` (optional: force a single source URL; otherwise the scraper tries `https://kolkataff.tv/`, then `https://kolkataff.in/`, then `https://kolkataff.net/`)
- `TELEGRAM_BOT_TOKEN` (required for Telegram posting)
- `TELEGRAM_CHAT_ID` (required for Telegram posting)
- `DATA_DIR` (default: `/data`)
- `RETENTION_DAYS` (default: `60`)

## Railway Deployment
1. Create a new Railway project.
2. Add a **Volume** mounted at `/data`.
3. Create two services from this repo:

### Service 1: Web API (FastAPI)
- Start command:
  ```bash
  uvicorn api:app --host 0.0.0.0 --port $PORT
  ```
- Always on.

### Service 2: Cron (Fetcher)
- Start command:
  ```bash
  python fetcher.py
  ```
- Cron schedule:
  ```
  */5 * * * *
  ```

### Environment Variables (both services)
- `SITE_URL`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DATA_DIR=/data`
- `RETENTION_DAYS=60`

## API Endpoints
- `GET /health` -> `{ "ok": true }`
- `GET /api/latest` -> latest saved result
- `GET /api/past?days=60` -> results from last N days (default 60)
- `GET /api/by-date?date=YYYY-MM-DD` -> results for specific day

Response JSON fields:
`id`, `source`, `draw_date`, `draw_time`, `result_text`, `signature`, `created_at`.

## How to test
### Local run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run API
uvicorn api:app --host 0.0.0.0 --port 8000

# Run fetcher manually
python fetcher.py
```

### Test API endpoints
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/latest
curl "http://localhost:8000/api/past?days=60"
curl "http://localhost:8000/api/by-date?date=2024-01-01"
```

### Verify Telegram posting
- Ensure `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set.
- Run the fetcher and confirm a message is posted when a new result is inserted.

### Inspect SQLite DB
```bash
sqlite3 /data/results.db "SELECT * FROM results ORDER BY created_at DESC LIMIT 5;"
```
