"""Persistence layer for the FastAPI web platform.

Beyond the original tables this module now records the *possible* formats that
can still be observed by an upgrade:

* ``format_versions`` is the canonical, per-kind monotonic registry.  A writer
  and a reader may legitimately use the same schema, and every switch must get
  a fresh version, so versions are keyed by ``(kind, version)`` for both kinds
  (the legacy ``schema_versions`` table is migrated and kept for compatibility).
* ``ingest_leases`` records an event that has selected a writer format but has
  not committed yet.  Reader upgrades must be able to read those formats even
  though the event is not in ``events`` yet.
* ``writer_switch_log`` is written in the *same transaction* as the writer
  pointer so reconciliation can tell "our switch applied" from "somebody else
  advanced the pointer".
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

from ..avro_codec import DEFAULT_SCHEMA_V1, fingerprint
from ..db_common import get_conn

_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    fp                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('writer', 'reader')),
    version             INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    UNIQUE(kind, version)
);
CREATE TABLE IF NOT EXISTS format_versions (
    kind                TEXT NOT NULL CHECK (kind IN ('writer', 'reader')),
    version             INTEGER NOT NULL,
    fp                  TEXT NOT NULL,
    schema_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    PRIMARY KEY (kind, version)
);
CREATE INDEX IF NOT EXISTS idx_format_versions_fp ON format_versions(fp);
CREATE TABLE IF NOT EXISTS writer_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    effective_fp        TEXT NOT NULL,
    effective_version   INTEGER NOT NULL,
    last_switch_id      TEXT,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS writer_switch_log (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    fp                  TEXT NOT NULL,
    applied_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    payload_json        TEXT NOT NULL,
    blob_b64            TEXT NOT NULL,
    ingested_at         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_leases (
    lease_id            TEXT PRIMARY KEY,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    expires_at          REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS writer_switches (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    new_fp              TEXT NOT NULL,
    schema_json         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    target              TEXT NOT NULL DEFAULT 'local',
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reader_switches (
    switch_id           TEXT PRIMARY KEY,
    analyzer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    writer_fp           TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decode_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL,
    analyzer_id         TEXT NOT NULL,
    reader_version      INTEGER,
    decoded_ok          INTEGER NOT NULL,
    datum_json          TEXT,
    error               TEXT,
    decoded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sim_config (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    delay_ms            INTEGER NOT NULL DEFAULT 0,
    drop_writer_switch  INTEGER NOT NULL DEFAULT 0,
    drop_reader_switch  INTEGER NOT NULL DEFAULT 0
);
"""

# How long an in-flight ingestion stays visible to upgrade guards.  It only
# needs to outlive the encoding call; a crashed process leaves a harmless
# residue that ages out.
INGEST_LEASE_SECONDS = 3600


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _columns(conn: sqlite3.Connection, table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent in-place migration; never drops existing data."""
    if "last_switch_id" not in _columns(conn, "writer_state"):
        conn.execute("ALTER TABLE writer_state ADD COLUMN last_switch_id TEXT")
    # Bring legacy registry rows forward (they predate the per-kind registry).
    if _columns(conn, "schema_versions"):
        conn.execute(
            "INSERT OR IGNORE INTO format_versions(kind, version, fp, schema_json, created_at) "
            "SELECT kind, version, fp, schema_json, created_at FROM schema_versions"
        )


def init(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(_DDL)
        _migrate(conn)
        fp = fingerprint(DEFAULT_SCHEMA_V1)
        conn.execute(
            "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
            "VALUES (?, 'writer', 1, ?, ?)",
            (fp, json.dumps(DEFAULT_SCHEMA_V1), _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO format_versions(kind, version, fp, schema_json, created_at) "
            "VALUES ('writer', 1, ?, ?, ?)",
            (fp, json.dumps(DEFAULT_SCHEMA_V1), _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO writer_state(id, effective_fp, effective_version, "
            "last_switch_id, updated_at) VALUES (1, ?, 1, NULL, ?)", (fp, _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_writer_switch, "
            "drop_reader_switch) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


# ----- schemas --------------------------------------------------------------

def register_schema(conn: sqlite3.Connection, kind: str, version: int,
                    schema: Dict[str, Any]) -> str:
    """Reserve a format version.  Idempotent for the same (kind, version, fp).

    Registering a *different* schema for an existing version is a programming
    error and fails loudly instead of silently corrupting the version chain.
    """
    fp = fingerprint(schema)
    existing = conn.execute(
        "SELECT fp FROM format_versions WHERE kind = ? AND version = ?",
        (kind, version),
    ).fetchone()
    if existing is not None:
        if existing["fp"] != fp:
            raise ValueError(
                f"{kind} format v{version} is already registered with a "
                f"different schema (fp {existing['fp'][:8]} != {fp[:8]})")
        return fp
    conn.execute(
        "INSERT INTO format_versions(kind, version, fp, schema_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (kind, version, fp, json.dumps(schema), _now()),
    )
    conn.execute(
        "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (fp, kind, version, json.dumps(schema), _now()),
    )
    return fp


def next_version(conn: sqlite3.Connection, kind: str) -> int:
    """Strictly monotonic per kind, including switches already proposed."""
    switch_table = "writer_switches" if kind == "writer" else "reader_switches"
    candidates = [
        conn.execute("SELECT COALESCE(MAX(version), 0) AS m FROM format_versions "
                     "WHERE kind = ?", (kind,)).fetchone()["m"],
        conn.execute("SELECT COALESCE(MAX(version), 0) AS m FROM schema_versions "
                     "WHERE kind = ?", (kind,)).fetchone()["m"],
        conn.execute(f"SELECT COALESCE(MAX(version), 0) AS m FROM {switch_table}"
                     ).fetchone()["m"],
    ]
    # Both formats start at v1 (the default schema shipped in every service),
    # so the first *switch* must propose v2.
    return max(2, max(candidates) + 1)


def get_schema_by_fp(conn: sqlite3.Connection, fp: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT schema_json FROM format_versions WHERE fp = ? ORDER BY version DESC LIMIT 1",
        (fp,),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT schema_json FROM schema_versions WHERE fp = ?", (fp,)
        ).fetchone()
    return json.loads(row["schema_json"]) if row else None


def get_writer_schema_by_version(conn: sqlite3.Connection,
                                 version: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT schema_json FROM format_versions WHERE kind = 'writer' AND version = ?",
        (version,),
    ).fetchone()
    return json.loads(row["schema_json"]) if row else None


def all_writer_schemas(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT fp, version, schema_json, created_at FROM format_versions "
        "WHERE kind = 'writer' ORDER BY version"
    ).fetchall()
    return [dict(fp=r["fp"], version=r["version"],
                 schema=json.loads(r["schema_json"]), created_at=r["created_at"])
            for r in rows]


# ----- writer state ---------------------------------------------------------

def get_writer_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM writer_state WHERE id = 1").fetchone()


def set_writer_state(conn: sqlite3.Connection, fp: str, version: int,
                     last_switch_id: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE writer_state SET effective_fp = ?, effective_version = ?, "
        "last_switch_id = ?, updated_at = ? WHERE id = 1",
        (fp, version, last_switch_id, _now()),
    )


def apply_writer_switch_fenced(conn: sqlite3.Connection, *, switch_id: str,
                               version: int, base_version: int, fp: str) -> Dict[str, Any]:
    """Durably move the writer pointer iff this switch is still applicable.

    MUST be called inside ``transaction(conn, immediate=True)`` so the
    read-check-write is atomic against concurrent ingestion and reconciliation.
    Returns ``{"status": applied|already_applied|stale|conflict}``.
    """
    state = get_writer_state(conn)
    current_version = state["effective_version"]
    current_fp = state["effective_fp"]
    existing = conn.execute(
        "SELECT * FROM writer_switch_log WHERE switch_id = ?", (switch_id,)
    ).fetchone()
    if existing is not None:
        if existing["version"] != version or existing["fp"] != fp:
            return {"status": "conflict",
                    "reason": f"switch {switch_id} already logged with different payload"}
        return {"status": "already_applied", "effective_version": current_version}
    if current_version > version:
        return {"status": "stale",
                "reason": f"late switch v{version} cannot overwrite effective "
                          f"v{current_version}"}
    if current_version == version:
        if current_fp == fp:
            return {"status": "already_applied", "effective_version": current_version}
        return {"status": "conflict",
                "reason": f"effective v{version} has different schema fp"}
    if current_version != base_version:
        return {"status": "stale",
                "reason": f"base v{base_version} does not match effective "
                          f"v{current_version}"}
    conn.execute(
        "INSERT INTO writer_switch_log(switch_id, version, base_version, fp, applied_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (switch_id, version, base_version, fp, _now()),
    )
    set_writer_state(conn, fp, version, switch_id)
    return {"status": "applied", "effective_version": version}


def count_events_for_writer(conn: sqlite3.Connection, fp: str, version: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE writer_fp = ? OR writer_version = ?",
        (fp, version),
    ).fetchone()["n"]


# ----- ingest leases --------------------------------------------------------

def begin_ingest_lease(conn: sqlite3.Connection, *, writer_fp: str,
                       writer_version: int, ttl: float = INGEST_LEASE_SECONDS) -> str:
    """Register an in-flight ingestion. Call inside an immediate transaction."""
    lease_id = "lease_" + uuid.uuid4().hex[:12]
    now = time.time()
    conn.execute(
        "INSERT INTO ingest_leases(lease_id, writer_fp, writer_version, created_at, "
        "expires_at) VALUES (?, ?, ?, ?, ?)",
        (lease_id, writer_fp, writer_version, _now(), now + ttl),
    )
    return lease_id


def finish_ingest_lease(conn: sqlite3.Connection, lease_id: str) -> bool:
    """Remove a lease (caller inserts the event in the same transaction)."""
    cur = conn.execute("DELETE FROM ingest_leases WHERE lease_id = ?", (lease_id,))
    return cur.rowcount > 0


def refresh_ingest_lease(conn: sqlite3.Connection, lease_id: str,
                         ttl: float = INGEST_LEASE_SECONDS) -> bool:
    """Extend a lease after a slow encode; False means it is no longer ours.

    Guards never delete active leases, so a missing row means the lease was
    explicitly cleaned up (e.g. by an operator/repair path) and the ingestion
    must not commit: its format was no longer covered by the upgrade guard.
    """
    cur = conn.execute(
        "UPDATE ingest_leases SET expires_at = ? WHERE lease_id = ?",
        (time.time() + ttl, lease_id))
    return cur.rowcount > 0


def active_ingest_leases(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Leases still protecting an in-flight ingestion (expired ones ignored)."""
    return conn.execute(
        "SELECT * FROM ingest_leases WHERE expires_at > ? ORDER BY created_at",
        (time.time(),),
    ).fetchall()


def purge_expired_ingest_leases(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM ingest_leases WHERE expires_at <= ?",
                       (time.time(),))
    return cur.rowcount


# ----- events ---------------------------------------------------------------

def insert_event(conn: sqlite3.Connection, *, writer_fp: str, writer_version: int,
                 payload: Dict[str, Any], blob_b64: str) -> str:
    event_id = "evt_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO events(id, writer_fp, writer_version, payload_json, blob_b64, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, writer_fp, writer_version, json.dumps(payload, ensure_ascii=False),
         blob_b64, _now()),
    )
    return event_id


def list_events(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events ORDER BY ingested_at DESC, rowid DESC"
    ).fetchall()


def get_event(conn: sqlite3.Connection, event_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()


# ----- switches -------------------------------------------------------------

def insert_writer_switch(conn: sqlite3.Connection, *, version: int, base_version: int,
                         new_fp: str, schema: Dict[str, Any], target: str) -> str:
    switch_id = "wsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO writer_switches(switch_id, version, base_version, new_fp, schema_json, "
        "status, target, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', "
        "?, NULL, ?, ?)",
        (switch_id, version, base_version, new_fp, json.dumps(schema), target, _now(), _now()),
    )
    return switch_id


def update_writer_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE writer_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_writer_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_writer_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE status = 'pending' ORDER BY version"
    ).fetchall()


def insert_reader_switch(conn: sqlite3.Connection, *, analyzer_id: str, version: int,
                         base_version: int, schema: Dict[str, Any], writer_fp: str) -> str:
    switch_id = "rsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO reader_switches(switch_id, analyzer_id, version, base_version, schema_json, "
        "writer_fp, status, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
        "'pending', NULL, ?, ?)",
        (switch_id, analyzer_id, version, base_version, json.dumps(schema),
         writer_fp, _now(), _now()),
    )
    return switch_id


def update_reader_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE reader_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_reader_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_reader_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE status = 'pending' ORDER BY created_at"
    ).fetchall()


def pending_reader_switches_for(conn: sqlite3.Connection,
                                analyzer_id: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE status = 'pending' AND analyzer_id = ? "
        "ORDER BY version", (analyzer_id,),
    ).fetchall()


# ----- decode results -------------------------------------------------------

def insert_decode_result(conn: sqlite3.Connection, *, event_id: str, analyzer_id: str,
                         reader_version: Optional[int], decoded_ok: bool,
                         datum: Any, error: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO decode_results(event_id, analyzer_id, reader_version, decoded_ok, "
        "datum_json, error, decoded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, analyzer_id, reader_version, int(decoded_ok),
         json.dumps(datum, ensure_ascii=False) if datum is not None else None, error, _now()),
    )


def latest_decode_results(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM decode_results ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


# ----- simulation -----------------------------------------------------------

def get_sim(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM sim_config WHERE id = 1").fetchone()
    return {
        "delay_ms": row["delay_ms"],
        "drop_writer_switch": bool(row["drop_writer_switch"]),
        "drop_reader_switch": bool(row["drop_reader_switch"]),
    }


def set_sim(conn: sqlite3.Connection, delay_ms: int, drop_writer: bool,
            drop_reader: bool) -> None:
    conn.execute(
        "UPDATE sim_config SET delay_ms = ?, drop_writer_switch = ?, "
        "drop_reader_switch = ? WHERE id = 1",
        (max(0, int(delay_ms)), int(bool(drop_writer)), int(bool(drop_reader))),
    )


def drop_armed(db_path: str, flag: str) -> bool:
    """Read a one-shot drop flag without consuming it.

    The flag must only be consumed by the operation that actually *dispatches*
    the request, otherwise an earlier validation/short-circuit would swallow
    the fault injection and the next switch would lose its receipt at an
    unexpected moment."""
    assert flag in {"drop_writer_switch", "drop_reader_switch"}
    conn = get_conn(db_path)
    try:
        row = conn.execute(f"SELECT {flag} AS v FROM sim_config WHERE id = 1"
                           ).fetchone()
        return bool(row["v"])
    finally:
        conn.close()


def clear_drop_flag(db_path: str, flag: str) -> None:
    """Consume a one-shot drop flag after the request was actually withheld."""
    assert flag in {"drop_writer_switch", "drop_reader_switch"}
    conn = get_conn(db_path)
    try:
        with conn:
            conn.execute(f"UPDATE sim_config SET {flag} = 0 WHERE id = 1")
    finally:
        conn.close()
