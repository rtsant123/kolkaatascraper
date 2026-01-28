from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import datetime
from typing import Any, Dict, Optional, Tuple, List

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger("kolkataff.scraper")
logging.basicConfig(level=logging.INFO, format="%(message)s")

USER_AGENT = "KolkataFFScraper/1.0 (+https://railway.app)"
DATE_PATTERNS = [
    re.compile(r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})", re.IGNORECASE),  # e.g. 21 January 2026
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
    cleaned = raw.strip()
    # Handle month names (e.g. "28 January 2026" or uppercase)
    for candidate in (cleaned, cleaned.title()):
        try:
            return datetime.datetime.strptime(candidate, "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            try:
                return datetime.datetime.strptime(candidate, "%d %b %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
    # Handle slash-separated
    try:
        if "/" in cleaned:
            day, month, year = cleaned.split("/")
            return f"{year}-{month}-{day}"
        if cleaned.count("-") == 2:
            parts = cleaned.split("-")
            if len(parts[0]) == 4:
                return cleaned
            day, month, year = parts
            return f"{year}-{month}-{day}"
    except Exception:
        pass
    return cleaned


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


def _extract_result_pairs(lines: List[str]) -> Optional[str]:
    """
    KolkataFF.tv lists dates followed by alternating 3-digit numbers and single-digit values.
    Build paired strings like '120-3 140-5 ...'. If no single digits, just join 3-digit numbers.
    """
    joined = " ".join(lines)
    numbers = re.findall(r"\b\d{3}\b", joined)
    singles = re.findall(r"\b\d\b", joined)
    if not numbers:
        return None
    pairs: List[str] = []
    for idx, num in enumerate(numbers):
        if idx < len(singles):
            pairs.append(f"{num}-{singles[idx]}")
        else:
            pairs.append(num)
    return " ".join(pairs)


def _extract_kolkataff_in_section(lines: List[str]) -> Optional[str]:
    """
    kolkataff.in structure:
      Date
      <maybe header line>
      line with 3-digit numbers and dashes
      line with single digits and dashes
    """
    row1 = None
    row2 = None
    for line in lines:
        if row1 is None and re.search(r"\b\d{3}\b", line):
            row1 = line
            continue
        if row1 is not None and row2 is None and re.search(r"\b\d\b", line):
            row2 = line
            break

    if row1 is None:
        return None

    tokens1 = re.findall(r"\b\d{3}\b|[-–]", row1)
    tokens2 = re.findall(r"\b\d\b|[-–]", row2) if row2 else []

    pairs: List[str] = []
    for idx, tok in enumerate(tokens1):
        if tok in ("-", "–"):
            continue
        suffix = tokens2[idx] if idx < len(tokens2) else None
        if suffix and suffix not in ("-", "–"):
            pairs.append(f"{tok}-{suffix}")
        else:
            pairs.append(tok)

    return " ".join(pairs) if pairs else None


def _make_soup(html: str) -> BeautifulSoup:
    """Try lxml first for speed; fall back to built-in parser if unavailable."""
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(html, parser)
        except Exception:
            continue
    # Final fallback (should rarely hit)
    return BeautifulSoup(html, "html.parser")


def parse_latest_result(html: str) -> Dict[str, str]:
    """
    Extract the most recent date block with numbers from KolkataFF.tv.
    Falls back to generic parsing if no date blocks are found.
    """
    soup = _make_soup(html)
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Find date headings with their index positions
    date_positions: List[Tuple[int, str]] = []
    for idx, line in enumerate(lines):
        date_value = None
        for pattern in DATE_PATTERNS:
            match = pattern.search(line)
            if match:
                date_value = _normalize_date(match.group(1))
                break
        if date_value:
            date_positions.append((idx, date_value))

    # Walk date sections in order and pick the first that contains numbers
    for pos, date_value in date_positions:
        next_pos = next((p for p, _ in date_positions if p > pos), len(lines))
        section = lines[pos + 1 : next_pos]

        # kolkataff.in specific pairing
        result_text = _extract_kolkataff_in_section(section)
        if not result_text:
            result_text = _extract_result_pairs(section)

        if result_text:
            signature = compute_signature(date_value, None, result_text)
            return {
                "draw_date": date_value,
                "draw_time": "",
                "result_text": result_text,
                "signature": signature,
            }

    # Fallback: use legacy selector-based parsing
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
        c_text = candidate.get_text("\n", strip=True)
        if not c_text:
            continue
        draw_date, draw_time = _extract_date_time(c_text)
        result_text = _extract_result_pairs([c_text])
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
