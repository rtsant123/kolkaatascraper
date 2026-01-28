from __future__ import annotations

import json
import logging
import os
import sys
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
    try:
        parsed = scraper.fetch_latest_result()
    except Exception as exc:  # noqa: BLE001
        log_event(logging.ERROR, "parse_failed", error=str(exc))
        return 1

    draw_time = parsed.get("draw_time") or None
    inserted = db.insert_result(
        source=os.getenv("SITE_URL", "https://kolkataff.tv/"),
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
