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
RESULT_PATTERN = re.compile(r"(?i)result\s*[:\-]?\s*([A-Za-z0-9\- ]{2,})")
# Schedule defaults: first draw 10:20, then every 90 minutes, 8 draws/day.
DEFAULT_FIRST_DRAW = "10:20"
DEFAULT_DRAW_INTERVAL_MIN = 90
DEFAULT_DRAWS_PER_DAY = 8


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


def _extract_result_pairs(lines: List[str]) -> Optional[List[str]]:
    """
    KolkataFF.tv lists dates followed by alternating 3-digit numbers and single-digit values.
    Build list like ['120-3', '140-5', ...]. If no single digits, just the 3-digit numbers.
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
    return pairs


def _extract_kolkataff_in_section(lines: List[str]) -> Optional[List[str]]:
    """
    kolkataff.in structure:
      Date
      <maybe header line>
      line with 3-digit numbers and dashes
      line with single digits and dashes
    """
    joined = " ".join(lines)
    numbers = re.findall(r"\b\d{3}\b", joined)
    singles = re.findall(r"\b\d\b", joined)
    if not numbers:
        return None
    pairs: List[str] = []
    for idx, num in enumerate(numbers):
        suffix = singles[idx] if idx < len(singles) else None
        if suffix and suffix not in ("-", "–"):
            pairs.append(f"{num}-{suffix}")
        else:
            pairs.append(num)
    return pairs if pairs else None


def _make_soup(html: str) -> BeautifulSoup:
    """Try lxml first for speed; fall back to built-in parser if unavailable."""
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(html, parser)
        except Exception:
            continue
    # Final fallback (should rarely hit)
    return BeautifulSoup(html, "html.parser")


def _build_draw_times(count: int) -> List[str]:
    """Generate draw times for the day based on env or defaults."""
    first_str = os.getenv("FIRST_DRAW_TIME", DEFAULT_FIRST_DRAW)
    interval_min = int(os.getenv("DRAW_INTERVAL_MIN", str(DEFAULT_DRAW_INTERVAL_MIN)))
    times: List[str] = []
    try:
        base = datetime.datetime.strptime(first_str, "%H:%M")
    except ValueError:
        base = datetime.datetime.strptime(DEFAULT_FIRST_DRAW, "%H:%M")
    for idx in range(count):
        t = base + datetime.timedelta(minutes=interval_min * idx)
        times.append(t.strftime("%H:%M"))
    return times


def parse_results(html: str) -> List[Dict[str, str]]:
    """
    Extract all date blocks with numbers from KolkataFF-style pages.
    Returns a list ordered as they appear on the page (typically newest first).
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

    # Merge consecutive duplicate dates so sections aren't zero-length
    merged_positions: List[Tuple[int, str]] = []
    for idx, date_value in date_positions:
        if merged_positions and merged_positions[-1][1] == date_value:
            # keep the later index for the same date block
            merged_positions[-1] = (idx, date_value)
        else:
            merged_positions.append((idx, date_value))

    results: List[Dict[str, str]] = []
    seen_signatures: set[str] = set()

    # Walk date sections in order and collect ones that contain numbers
    for pos, date_value in merged_positions:
        next_pos = next((p for p, _ in merged_positions if p > pos), len(lines))
        section = lines[pos + 1 : next_pos]

        # kolkataff.in specific pairing
        result_pairs = _extract_kolkataff_in_section(section)
        if not result_pairs:
            result_pairs = _extract_result_pairs(section)

        if result_pairs:
            times = _build_draw_times(len(result_pairs))
            for pair, draw_time in zip(result_pairs, times):
                signature = compute_signature(date_value, draw_time, pair)
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                results.append(
                    {
                        "draw_date": date_value,
                        "draw_time": draw_time,
                        "result_text": pair,
                        "signature": signature,
                    }
                )

    if results:
        return results

    # Fallback: use legacy selector-based parsing and return a single result if found
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
        result_pairs = _extract_result_pairs([c_text])
        if draw_date and result_pairs:
            times = _build_draw_times(len(result_pairs))
            fallback_results: List[Dict[str, str]] = []
            for pair, dt in zip(result_pairs, times):
                signature = compute_signature(draw_date, dt, pair)
                fallback_results.append(
                    {
                        "draw_date": draw_date,
                        "draw_time": dt,
                        "result_text": pair,
                        "signature": signature,
                    }
                )
            return fallback_results

    raise ValueError("Unable to parse latest result")


def parse_latest_result(html: str) -> Dict[str, str]:
    """Compatibility helper: return just the first parsed result."""
    results = parse_results(html)
    if not results:
        raise ValueError("Unable to parse latest result")
    return results[0]


def fetch_results(site_url: Optional[str] = None) -> List[Dict[str, str]]:
    """Fetch HTML from a site and return all parsed results (page order)."""
    url = site_url or os.getenv("SITE_URL", "https://kolkataff.tv/")
    html = fetch_html(url)
    return parse_results(html)


def fetch_latest_result(site_url: Optional[str] = None) -> Dict[str, str]:
    """Fetch HTML from a site and return only the newest parsed result."""
    url = site_url or os.getenv("SITE_URL", "https://kolkataff.tv/")
    html = fetch_html(url)
    results = parse_results(html)
    if not results:
        raise ValueError("Unable to parse latest result")
    return results[0]

def compute_signature(draw_date: str, draw_time: Optional[str], result_text: str) -> str:
    value = f"{draw_date}|{draw_time or ''}|{result_text}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
