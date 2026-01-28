from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger("kolkataff.scraper")
logging.basicConfig(level=logging.INFO, format="%(message)s")

USER_AGENT = "KolkataFFScraper/1.0 (+https://railway.app)"
DATE_PATTERNS = [
    re.compile(r"(\d{4}-\d{2}-\d{2})"),
    re.compile(r"(\d{2}-\d{2}-\d{4})"),
    re.compile(r"(\d{2}/\d{2}/\d{4})"),
]
TIME_PATTERN = re.compile(r"(\d{1,2}:\d{2})")
RESULT_PATTERN = re.compile(
    r"(?i)result\s*[:\-]?\s*([A-Za-z0-9\- ]{2,})"
)


def log_event(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    LOGGER.log(level, json.dumps(payload, ensure_ascii=False))


def fetch_html(url: str, timeout_s: int = 15, max_retries: int = 3) -> str:
    session = requests.Session()
    headers = {"User-Agent": USER_AGENT}
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=timeout_s)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            backoff = 2 ** (attempt - 1)
            log_event(
                logging.WARNING,
                "fetch_failed",
                attempt=attempt,
                backoff_s=backoff,
                error=str(exc),
            )
            time.sleep(backoff)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _normalize_date(raw: str) -> str:
    if "/" in raw:
        day, month, year = raw.split("/")
        return f"{year}-{month}-{day}"
    if raw.count("-") == 2:
        parts = raw.split("-")
        if len(parts[0]) == 4:
            return raw
        day, month, year = parts
        return f"{year}-{month}-{day}"
    return raw


def _extract_date_time(text: str) -> Tuple[Optional[str], Optional[str]]:
    date_value = None
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            date_value = _normalize_date(match.group(1))
            break
    time_match = TIME_PATTERN.search(text)
    time_value = time_match.group(1) if time_match else None
    return date_value, time_value


def _extract_result_text(text: str) -> Optional[str]:
    match = RESULT_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines:
        if re.search(r"\d", line) and len(line) <= 50:
            lowered = line.lower()
            if "date" in lowered or "time" in lowered:
                continue
            return line
    return None


def parse_latest_result(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        ".latest-result",
        ".latest",
        ".result",
        ".results",
        "#latest-result",
        "#result",
        "#results",
        "main",
        "article",
        ".entry-content",
    ]
    candidates = []
    for selector in selectors:
        candidates.extend(soup.select(selector))
    if not candidates:
        candidates = [soup.body] if soup.body else []

    for candidate in candidates:
        text = candidate.get_text("\n", strip=True)
        if not text:
            continue
        draw_date, draw_time = _extract_date_time(text)
        result_text = _extract_result_text(text)
        if draw_date and result_text:
            signature = compute_signature(draw_date, draw_time, result_text)
            return {
                "draw_date": draw_date,
                "draw_time": draw_time or "",
                "result_text": result_text,
                "signature": signature,
            }

    raise ValueError("Unable to parse latest result")


def compute_signature(draw_date: str, draw_time: Optional[str], result_text: str) -> str:
    value = f"{draw_date}|{draw_time or ''}|{result_text}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def fetch_latest_result(site_url: Optional[str] = None) -> Dict[str, str]:
    url = site_url or os.getenv("SITE_URL", "https://kolkataff.tv/")
    html = fetch_html(url)
    return parse_latest_result(html)
