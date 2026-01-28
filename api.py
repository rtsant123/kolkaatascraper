from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query

import db
import scraper

app = FastAPI()


@app.on_event("startup")
def init_db() -> None:
    """Ensure the SQLite schema exists when the API starts."""
    db.init_db()


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/debug/db")
def debug_db() -> Dict[str, Any]:
    """Lightweight debug endpoint: shows DB path and row count."""
    return {
        "data_dir": db.get_data_dir().as_posix(),
        "db_path": db.get_db_path().as_posix(),
        "rows": db.get_row_count(),
    }


@app.get("/api/latest")
def latest() -> Optional[Dict[str, Any]]:
    result = db.get_latest_result()
    if result:
        return result
    # Bootstrap: if DB is empty, scrape the newest result once and persist
    try:
        scraped = scraper.fetch_latest_result()
        db.insert_result(
            source=os.getenv("SITE_URL", "https://kolkataff.tv/"),
            draw_date=scraped["draw_date"],
            draw_time=scraped.get("draw_time") or None,
            result_text=scraped["result_text"],
            signature=scraped["signature"],
        )
    except Exception:
        return None
    return db.get_latest_result()


@app.get("/api/past")
def past(days: int = Query(60, ge=1, le=365)) -> List[Dict[str, Any]]:
    return db.get_past_results(days=days)


@app.get("/api/by-date")
def by_date(date: str = Query(..., regex=r"\d{4}-\d{2}-\d{2}")) -> List[Dict[str, Any]]:
    return db.get_results_by_date(date)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
