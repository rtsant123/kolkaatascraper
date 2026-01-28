from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import db
import scraper
import telegram

LOGGER = logging.getLogger("kolkataff.fetcher")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    LOGGER.log(level, json.dumps(payload, ensure_ascii=False))


def format_message(draw_date: str, draw_time: str | None, result_text: str) -> str:
    lines = ["Kolkata FF Update", f"Date: {draw_date}"]
    if draw_time:
        lines.append(f"Time: {draw_time}")
    lines.append(f"Result: {result_text}")
    return "\n".join(lines)


def main() -> int:
    db.init_db()
    retention_days = int(os.getenv("RETENTION_DAYS", "60"))
    site_url = os.getenv("SITE_URL", "https://kolkataff.tv/")
    try:
        html = scraper.fetch_html(site_url)
        if os.getenv("SAVE_HTML", "0") == "1":
            data_dir = Path(os.getenv("DATA_DIR", "/data"))
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "last_fetch.html").write_text(html, encoding="utf-8")
        parsed = scraper.parse_latest_result(html)
    except Exception as exc:  # noqa: BLE001
        log_event(logging.ERROR, "parse_failed", error=str(exc))
        return 0  # don't fail the container; try again next run

    draw_time = parsed.get("draw_time") or None
    inserted = db.insert_result(
        source=site_url,
        draw_date=parsed["draw_date"],
        draw_time=draw_time,
        result_text=parsed["result_text"],
        signature=parsed["signature"],
    )
    db.cleanup_old(retention_days)

    if inserted:
        message = format_message(parsed["draw_date"], draw_time, parsed["result_text"])
        telegram.send_message(message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
