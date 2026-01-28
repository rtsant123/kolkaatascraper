from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import requests
from requests import HTTPError

LOGGER = logging.getLogger("kolkataff.telegram")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    LOGGER.log(level, json.dumps(payload, ensure_ascii=False))


def send_message(message: str) -> Dict[str, Any]:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    response = requests.post(url, json=payload, timeout=15)
    try:
        response.raise_for_status()
    except HTTPError as exc:  # noqa: BLE001
        log_event(
            logging.ERROR,
            "telegram_failed",
            status=response.status_code,
            error=str(exc),
            response_text=response.text[:500],
        )
        raise

    log_event(logging.INFO, "telegram_sent", status=response.status_code)
    return response.json()
