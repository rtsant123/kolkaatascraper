from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger("kolkataff.db")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    LOGGER.log(level, json.dumps(payload, ensure_ascii=False))


def get_data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "/data"))


def get_db_path() -> Path:
    return get_data_dir() / "results.db"


def get_connection() -> sqlite3.Connection:
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                draw_date TEXT NOT NULL,
                draw_time TEXT,
                result_text TEXT NOT NULL,
                signature TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_draw_date ON results(draw_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_created_at ON results(created_at)"
        )
    conn.close()


def insert_result(
    source: str,
    draw_date: str,
    draw_time: Optional[str],
    result_text: str,
    signature: str,
    created_at: Optional[int] = None,
) -> bool:
    created_at = created_at or int(time.time())
    conn = get_connection()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO results (source, draw_date, draw_time, result_text, signature, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source, draw_date, draw_time, result_text, signature, created_at),
            )
        log_event(
            logging.INFO,
            "result_inserted",
            source=source,
            draw_date=draw_date,
            draw_time=draw_time,
            signature=signature,
        )
        return True
    except sqlite3.IntegrityError:
        log_event(logging.INFO, "result_duplicate", signature=signature)
        return False
    finally:
        conn.close()


def cleanup_old(retention_days: int) -> int:
    cutoff = int(time.time()) - retention_days * 86400
    conn = get_connection()
    with conn:
        cursor = conn.execute("DELETE FROM results WHERE created_at < ?", (cutoff,))
    conn.close()
    deleted = cursor.rowcount if cursor else 0
    if deleted:
        log_event(logging.INFO, "cleanup_deleted", deleted=deleted)
    return deleted


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "source": row["source"],
        "draw_date": row["draw_date"],
        "draw_time": row["draw_time"],
        "result_text": row["result_text"],
        "signature": row["signature"],
        "created_at": row["created_at"],
    }


def get_latest_result() -> Optional[Dict[str, Any]]:
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM results ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return _row_to_dict(row) if row else None


def get_past_results(days: int) -> List[Dict[str, Any]]:
    cutoff = int(time.time()) - days * 86400
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM results WHERE created_at >= ? ORDER BY created_at DESC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [_row_to_dict(row) for row in rows]


def get_results_by_date(date: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM results WHERE draw_date = ? ORDER BY created_at DESC",
        (date,),
    ).fetchall()
    conn.close()
    return [_row_to_dict(row) for row in rows]
