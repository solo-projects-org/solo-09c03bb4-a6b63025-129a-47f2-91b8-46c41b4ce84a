"""Persistence layer for one analyzer service (its own SQLite database).

Every reader switch is applied inside a single ``BEGIN IMMEDIATE`` transaction
that re-reads the effective state, checks the idempotency log, applies the
fence and performs the mutation atomically.  A late/duplicated request can
therefore never overwrite a newer effective reader, and a replayed request can
never be confused with a *different* switch that happens to reuse its id.
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional

from ..avro_codec import DEFAULT_SCHEMA_V1, fingerprint
from ..db_common import get_conn, transaction

_DDL = """
CREATE TABLE IF NOT EXISTS reader_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    version             INTEGER NOT NULL,
    last_switch_id      TEXT,
    reader_schema_json  TEXT NOT NULL,
    reader_fp           TEXT,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS switch_log (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    reader_schema_json  TEXT NOT NULL,
    reader_fp           TEXT,
    writer_schema_json  TEXT NOT NULL,
    writer_schemas_json TEXT,
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
        _migrate(conn)
        now = _now()
        fp = fingerprint(DEFAULT_SCHEMA_V1)
        conn.execute(
            "INSERT OR IGNORE INTO reader_state(id, version, last_switch_id, "
            "reader_schema_json, reader_fp, updated_at) VALUES (1, 1, NULL, ?, ?, ?)",
            (json.dumps(DEFAULT_SCHEMA_V1), fp, now),
        )
        # Backfill the fingerprint for databases created before it existed,
        # deriving it from the *stored* reader schema (which may not be v1).
        row = conn.execute("SELECT reader_schema_json FROM reader_state "
                           "WHERE id = 1 AND (reader_fp IS NULL OR reader_fp = '')"
                           ).fetchone()
        if row is not None:
            try:
                existing_fp = fingerprint(json.loads(row["reader_schema_json"]))
            except Exception:
                existing_fp = fp
            conn.execute("UPDATE reader_state SET reader_fp = ? WHERE id = 1",
                         (existing_fp,))
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_switch_response, "
            "drop_decode_response) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


def _columns(conn: sqlite3.Connection, table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _migrate(conn: sqlite3.Connection) -> None:
    state_cols = _columns(conn, "reader_state")
    if "reader_fp" not in state_cols:
        conn.execute("ALTER TABLE reader_state ADD COLUMN reader_fp TEXT")
    log_cols = _columns(conn, "switch_log")
    if "reader_fp" not in log_cols:
        conn.execute("ALTER TABLE switch_log ADD COLUMN reader_fp TEXT")
    if "writer_schemas_json" not in log_cols:
        conn.execute("ALTER TABLE switch_log ADD COLUMN writer_schemas_json TEXT")
    # Backfill fingerprints for legacy switch-log rows.
    for row in conn.execute("SELECT switch_id, reader_schema_json FROM switch_log "
                            "WHERE reader_fp IS NULL OR reader_fp = ''").fetchall():
        try:
            schema = json.loads(row["reader_schema_json"])
        except Exception:
            continue
        conn.execute("UPDATE switch_log SET reader_fp = ? WHERE switch_id = ?",
                     (fingerprint(schema), row["switch_id"]))


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def get_state(db_path: str) -> sqlite3.Row:
    conn = get_conn(db_path)
    try:
        return conn.execute("SELECT * FROM reader_state WHERE id = 1").fetchone()
    finally:
        conn.close()


def get_switch_log(db_path: str, switch_id: str) -> Optional[sqlite3.Row]:
    conn = get_conn(db_path)
    try:
        return conn.execute("SELECT * FROM switch_log WHERE switch_id = ?",
                            (switch_id,)).fetchone()
    finally:
        conn.close()


def apply_switch(db_path: str, *, switch_id: str, version: int, base_version: int,
                 reader_schema: Dict[str, Any],
                 writer_schemas: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Atomically apply a fenced, idempotent reader switch.

    Returns ``{"status": applied|already_applied|stale|conflict, ...}``.
    A request whose ``switch_id`` is already logged with a different payload is
    a conflict; a request whose version is not newer than the effective one is
    stale, so an old switch can never roll the reader back.
    """
    reader_fp = fingerprint(reader_schema)
    writer_schemas_json = json.dumps(writer_schemas)
    conn = get_conn(db_path)
    try:
        with transaction(conn, immediate=True):
            state = conn.execute(
                "SELECT * FROM reader_state WHERE id = 1").fetchone()
            current_version = state["version"]
            logged = conn.execute(
                "SELECT * FROM switch_log WHERE switch_id = ?", (switch_id,)
            ).fetchone()

            if logged is not None:
                logged_fp = logged["reader_fp"] or fingerprint(
                    json.loads(logged["reader_schema_json"]))
                if logged["version"] != version or logged_fp != reader_fp:
                    return {
                        "status": "conflict",
                        "effective_version": current_version,
                        "reason": (f"switch_id {switch_id} was already logged "
                                   f"with a different version/schema"),
                    }
                return {
                    "status": "already_applied",
                    "effective_version": current_version,
                    "switch_version": version,
                    "reader_fp": logged_fp,
                }

            # Late or duplicated switch: never overwrite a newer reader.
            if version <= current_version:
                return {
                    "status": "stale",
                    "effective_version": current_version,
                    "requested_version": version,
                    "reason": (f"requested version {version} is not newer than "
                               f"effective version {current_version}; late "
                               "switch rejected"),
                }
            if current_version != base_version:
                return {
                    "status": "stale",
                    "effective_version": current_version,
                    "requested_version": version,
                    "reason": (f"base_version {base_version} does not match "
                               f"effective version {current_version}; late "
                               "switch rejected"),
                }

            conn.execute(
                "INSERT INTO switch_log(switch_id, version, base_version, "
                "reader_schema_json, reader_fp, writer_schema_json, "
                "writer_schemas_json, applied_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (switch_id, version, base_version, json.dumps(reader_schema),
                 reader_fp,
                 json.dumps(writer_schemas[0]) if writer_schemas else "null",
                 writer_schemas_json, _now()),
            )
            conn.execute(
                "UPDATE reader_state SET version = ?, last_switch_id = ?, "
                "reader_schema_json = ?, reader_fp = ?, updated_at = ? WHERE id = 1",
                (version, switch_id, json.dumps(reader_schema), reader_fp, _now()),
            )
            return {"status": "applied", "effective_version": version,
                    "reader_fp": reader_fp}
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
