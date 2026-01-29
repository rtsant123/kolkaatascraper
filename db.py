from __future__ import annotations


import mysql.connector
import time
import os
from typing import Any, Dict, List, Optional

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
    )

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(255) NOT NULL,
            draw_date VARCHAR(20) NOT NULL,
            draw_time VARCHAR(20),
            result_text VARCHAR(255) NOT NULL,
            signature VARCHAR(255) NOT NULL UNIQUE,
            created_at BIGINT NOT NULL
        )
        """
    )
    try:
        cursor.execute("CREATE INDEX idx_results_draw_date ON results(draw_date)")
    except Exception:
        pass
    try:
        cursor.execute("CREATE INDEX idx_results_created_at ON results(created_at)")
    except Exception:
        pass
    conn.commit()
    cursor.close()
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
    print(f"[DEBUG] Attempting to insert result: source={source}, draw_date={draw_date}, draw_time={draw_time}, result_text={result_text}, signature={signature}, created_at={created_at}")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO results (source, draw_date, draw_time, result_text, signature, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (source, draw_date, draw_time, result_text, signature, created_at),
        )
        conn.commit()
        print(f"[DEBUG] Inserted result successfully: signature={signature}")
        return True
    except mysql.connector.IntegrityError:
        print(f"[DEBUG] Duplicate result detected, not inserted: signature={signature}")
        return False
    finally:
        cursor.close()
        conn.close()

def get_latest_result() -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM results ORDER BY created_at DESC LIMIT 1")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row if row else None

def get_row_count() -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM results")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count

def get_past_results(days: int) -> List[Dict[str, Any]]:
    cutoff = int(time.time()) - days * 86400
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM results WHERE created_at >= %s ORDER BY created_at DESC", (cutoff,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_results_by_date(date: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM results WHERE draw_date = %s ORDER BY created_at DESC", (date,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def init_db() -> None:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(255) NOT NULL,
            draw_date VARCHAR(20) NOT NULL,
            draw_time VARCHAR(20),
            result_text VARCHAR(255) NOT NULL,
            signature VARCHAR(255) NOT NULL UNIQUE,
            created_at BIGINT NOT NULL
        )
        """
    )
    try:
        cursor.execute("CREATE INDEX idx_results_draw_date ON results(draw_date)")
    except Exception:
        pass
    try:
        cursor.execute("CREATE INDEX idx_results_created_at ON results(created_at)")
    except Exception:
        pass
    conn.commit()
    cursor.close()
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
    print(f"[DEBUG] Attempting to insert result: source={source}, draw_date={draw_date}, draw_time={draw_time}, result_text={result_text}, signature={signature}, created_at={created_at}")
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
        print(f"[DEBUG] Inserted result successfully: signature={signature}")
        return True
    except sqlite3.IntegrityError:
        print(f"[DEBUG] Duplicate result detected, not inserted: signature={signature}")
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


def get_row_count() -> int:
    conn = get_connection()
    try:
        count = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    finally:
        conn.close()
    return count


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
