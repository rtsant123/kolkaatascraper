from __future__ import annotations

import json
import logging
import os
import sys
import datetime
import time
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


def scrape_with_fallback() -> tuple[str, list[dict[str, str]]]:
    """
    Try multiple sources so a single 502/host outage doesn't stop inserts.
    Priority:
      1) Explicit SITE_URL if provided
      2) Known mirrors (tv -> in -> net)
    """
    env_site = os.getenv("SITE_URL")
    candidates = (
        [env_site]
        if env_site
        else [
            "https://kolkataff.tv/",
            "https://kolkataff.in/",
            "https://kolkataff.net/",
        ]
    )

    last_error: str | None = None
    for url in candidates:
        try:
            html = scraper.fetch_html(url)
            if os.getenv("SAVE_HTML", "0") == "1":
                data_dir = Path(os.getenv("DATA_DIR", "/data"))
                data_dir.mkdir(parents=True, exist_ok=True)
                (data_dir / "last_fetch.html").write_text(html, encoding="utf-8")
            parsed_list = scraper.parse_results(html)
            return url, parsed_list
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            log_event(logging.WARNING, "scrape_source_failed", source=url, error=last_error)

    raise RuntimeError(f"all sources failed; last_error={last_error}")


def _within_backfill(draw_date: str, backfill_days: int) -> bool:
    """Return True if draw_date is within backfill_days of today. If parse fails, allow insert."""
    if backfill_days <= 0:
        return True
    try:
        dt = datetime.datetime.strptime(draw_date, "%Y-%m-%d").date()
    except ValueError:
        return True
    return dt >= (datetime.date.today() - datetime.timedelta(days=backfill_days))


def main() -> int:
    # Optional: wipe DB on startup to re-play all results (controlled via env)
    if os.getenv("RESET_DB_ON_START") == "1":
        db_path = db.get_db_path()
        try:
            db_path.unlink()
            log_event(logging.WARNING, "db_reset", path=str(db_path))
        except FileNotFoundError:
            pass
    db.init_db()
    retention_days = int(os.getenv("RETENTION_DAYS", "60"))
    backfill_days = int(os.getenv("BACKFILL_DAYS", "0"))
    try:
        source_url, results = scrape_with_fallback()
    except Exception as exc:  # noqa: BLE001
        log_event(logging.ERROR, "parse_failed", error=str(exc))
        return 0  # don't fail the container; try again next run

    # Filter out old entries if BACKFILL_DAYS is set
    if backfill_days > 0:
        results = [r for r in results if _within_backfill(r["draw_date"], backfill_days)]

    # By default send only the newest entry on the page; set SEND_ALL_RESULTS=1 to send all
    if os.getenv("SEND_ALL_RESULTS", "0") != "1" and results:
        results = results[:1]

    # Send newest-first (page order is already newest first for KolkataFF)
    new_count = 0
    for parsed in results:
        draw_time = parsed.get("draw_time") or None
        inserted = db.insert_result(
            source=source_url,
            draw_date=parsed["draw_date"],
            draw_time=draw_time,
            result_text=parsed["result_text"],
            signature=parsed["signature"],
        )
        if inserted:
            new_count += 1
            message = format_message(parsed["draw_date"], draw_time, parsed["result_text"])
            result = telegram.send_message(message)
            if result.get("ok") is not True:
                log_event(logging.WARNING, "telegram_not_sent", details=result)
            # Avoid hitting Telegram flood limits on first-run bursts
            time.sleep(1)

    db.cleanup_old(retention_days)

    if new_count:
        log_event(logging.INFO, "results_inserted", count=new_count)
    else:
        log_event(logging.INFO, "no_new_results")

    return 0


if __name__ == "__main__":
    sys.exit(main())
