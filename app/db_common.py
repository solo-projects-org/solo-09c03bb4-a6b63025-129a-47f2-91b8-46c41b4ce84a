"""Shared SQLite connection helpers (WAL, foreign keys, Row factory)."""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

_INIT_LOCK = threading.Lock()
_INITIALIZED: set = set()


def get_conn(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str, ddl: str) -> None:
    """Idempotent one-time schema initialisation per process."""
    with _INIT_LOCK:
        if db_path in _INITIALIZED:
            return
        conn = get_conn(db_path)
        try:
            conn.executescript(ddl)
            conn.commit()
        finally:
            conn.close()
        _INITIALIZED.add(db_path)
