"""Persistence layer for one analyzer service (its own SQLite database)."""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict

from ..db_common import get_conn
from ..avro_codec import DEFAULT_SCHEMA_V1

_DDL = """
CREATE TABLE IF NOT EXISTS reader_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    version             INTEGER NOT NULL,
    last_switch_id      TEXT,
    reader_schema_json  TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS switch_log (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    reader_schema_json  TEXT NOT NULL,
    writer_schema_json  TEXT NOT NULL,
    applied_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sim_config (
    id                     INTEGER PRIMARY KEY CHECK (id = 1),
    delay_ms               INTEGER NOT NULL DEFAULT 0,
    drop_switch_response   INTEGER NOT NULL DEFAULT 0,
    drop_decode_response   INTEGER NOT NULL DEFAULT 0
);
"""


def init(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(_DDL)
        now = _now()
        conn.execute(
            "INSERT OR IGNORE INTO reader_state(id, version, last_switch_id, "
            "reader_schema_json, updated_at) VALUES (1, 1, NULL, ?, ?)",
            (json.dumps(DEFAULT_SCHEMA_V1), now),
        )
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_switch_response, "
            "drop_decode_response) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def get_state(db_path: str) -> sqlite3.Row:
    conn = get_conn(db_path)
    try:
        return conn.execute("SELECT * FROM reader_state WHERE id = 1").fetchone()
    finally:
        conn.close()


def apply_switch(db_path: str, *, switch_id: str, version: int, base_version: int,
                 reader_schema: Dict[str, Any], writer_schema: Dict[str, Any]) -> None:
    """Durably install a new reader schema atomically with its audit row."""
    conn = get_conn(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO switch_log(switch_id, version, base_version, "
                "reader_schema_json, writer_schema_json, applied_at) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(switch_id) DO NOTHING",
                (switch_id, version, base_version,
                 json.dumps(reader_schema), json.dumps(writer_schema), _now()),
            )
            conn.execute(
                "UPDATE reader_state SET version = ?, last_switch_id = ?, "
                "reader_schema_json = ?, updated_at = ? WHERE id = 1",
                (version, switch_id, json.dumps(reader_schema), _now()),
            )
    finally:
        conn.close()


def get_sim(db_path: str) -> Dict[str, Any]:
    conn = get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM sim_config WHERE id = 1").fetchone()
        return {
            "delay_ms": row["delay_ms"],
            "drop_switch_response": bool(row["drop_switch_response"]),
            "drop_decode_response": bool(row["drop_decode_response"]),
        }
    finally:
        conn.close()


def set_sim(db_path: str, delay_ms: int, drop_switch: bool, drop_decode: bool) -> None:
    conn = get_conn(db_path)
    try:
        with conn:
            conn.execute(
                "UPDATE sim_config SET delay_ms = ?, drop_switch_response = ?, "
                "drop_decode_response = ? WHERE id = 1",
                (max(0, int(delay_ms)), int(bool(drop_switch)), int(bool(drop_decode))),
            )
    finally:
        conn.close()


def reset_drop_flag(db_path: str, flag: str) -> None:
    assert flag in {"drop_switch_response", "drop_decode_response"}
    conn = get_conn(db_path)
    try:
        with conn:
            conn.execute(f"UPDATE sim_config SET {flag} = 0 WHERE id = 1")
    finally:
        conn.close()
