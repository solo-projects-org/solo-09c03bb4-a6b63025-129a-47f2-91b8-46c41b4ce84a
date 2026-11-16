"""Shared SQLite connection helpers (WAL, foreign keys, Row factory)."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

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


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """Explicit transaction with optional ``BEGIN IMMEDIATE`` write locking.

    Compare-and-set fences (e.g. "only move version if it is still the one I
    observed") must be evaluated inside the *same* write transaction that
    performs the mutation; the legacy implicit-transaction mode can interleave
    a concurrent reader between the ``SELECT`` and the ``UPDATE``.
    """
    previous = conn.isolation_level
    conn.isolation_level = None  # take manual control of BEGIN/COMMIT
    try:
        conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
    finally:
        conn.isolation_level = previous


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
