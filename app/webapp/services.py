"""Switch orchestration: compatibility guards, durable two-phase protocol and
post-restart reconciliation.

Protocol summary
================

Writer (write-format) switch
----------------------------
1. Reject if another writer switch is still pending (serial, monotonic versions).
2. Guard locally (static + round-trip) AND ask *both* analyzers to prove their
   effective readers can read the proposed writer schema (``/writer/check``).
   The current writer pointer never moves before this passes, so an
   incompatible format can never be released.
3. Persist a ``pending`` switch row *before* any mutation.
4. Apply the local writer switch as a durable, fenced, idempotent operation
   (a simulated lost receipt leaves the row pending and success unreported).
5. Re-verify both analyzers post-switch; mark ``confirmed`` only when every
   check is durable-observable. A crash/lost receipt triggers reconciliation:
   real state is inspected, the mutation is retried only if it never happened,
   and a late switch is fenced so it can never overwrite a newer version.

Reader (read-format) switch (per analyzer)
------------------------------------------
1. Guard locally: the new reader must read the *current writer* schema AND the
   original writer schema of *every retained event*. On failure return the
   field paths and do not touch any format.
2. Persist ``pending`` row, then call the analyzer's durable fenced switch.
3. Confirm on success; on a lost receipt/crash keep ``pending`` and reconcile
   against the analyzer's reported effective version.

Interleaving
------------
Event ingestion only ever reads the *confirmed* effective writer pointer and is
not blocked by pending switches, so compatible events keep landing in the
warehouse while verification is in flight.
"""
from __future__ import annotations

import base64
import json
import threading
from typing import Any, Dict, List

from ..avro_codec import (CodecError, SchemaError, encode, fingerprint,
                          parse_schema, roundtrip_check)
from ..avro_compat import explain
from . import db
from .analyzer_client import ANALYZER_ORDER, AnalyzerClient, RemoteError


class GuardError(Exception):
    """A precondition/compatibility guard rejected the request."""


class SwitchService:
    def __init__(self, db_path: str, clients: Dict[str, AnalyzerClient]):
        self.db_path = db_path
        self.clients = clients
        # Serialize switch mutations; SQLite serializes durability anyway.
        self._lock = threading.RLock()

    def _conn(self):
        from ..db_common import get_conn
        return get_conn(self.db_path)

    # ======================================================================
    # Compatibility guards
    # ======================================================================

    def guard_writer_switch(self, new_writer: Dict[str, Any]) -> List[str]:
        """Local static + round-trip guard against every effective reader.

        Returns a list of field-path errors (empty == compatible)."""
        errors: List[str] = []
        try:
            parse_schema(new_writer)
        except SchemaError as exc:
            return [str(exc)]
        for analyzer_id in ANALYZER_ORDER:
            try:
                reader = self._effective_reader(analyzer_id)
            except RemoteError as exc:
                # Fail closed: do not release a format an unreachable service
                # cannot be proven to read.
                errors.append(f"[{analyzer_id}] reader state unreachable: {exc}")
                continue
            err = roundtrip_check(reader, new_writer)
            if err:
                errors.append(f"[{analyzer_id}] {err}")
        return errors

    def remote_writer_checks(self, new_writer: Dict[str, Any]) -> Dict[str, Any]:
        """Ask both analyzers to prove readability (read-only)."""
        report = {}
        for analyzer_id in ANALYZER_ORDER:
            try:
                report[analyzer_id] = self.clients[analyzer_id].writer_check(new_writer)
            except RemoteError as exc:
                report[analyzer_id] = {
                    "compatible": False,
                    "error": f"unconfirmed (transport failure): {exc}",
                }
        return report

    def guard_reader_switch(self, new_reader: Dict[str, Any]) -> List[str]:
        """Reader must read current writer + every retained event's original."""
        errors: List[str] = []
        try:
            parse_schema(new_reader)
        except SchemaError as exc:
            return [str(exc)]
        with self._conn() as conn:
            state = db.get_writer_state(conn)
            current_writer = db.get_schema_by_fp(conn, state["effective_fp"])
            err = roundtrip_check(new_reader, current_writer)
            if err:
                errors.append(f"[current writer v{state['effective_version']}] {err}")
            for ev in conn.execute(
                "SELECT DISTINCT writer_fp, writer_version FROM events"
            ).fetchall():
                wschema = db.get_schema_by_fp(conn, ev["writer_fp"])
                err = explain(parse_schema(new_reader), parse_schema(wschema))
                if err:
                    errors.append(
                        f"[retained event writer v{ev['writer_version']} "
                        f"fp={ev['writer_fp'][:8]}] {err}"
                    )
        return errors

    # ======================================================================
    # Writer switch
    # ======================================================================

    def create_writer_switch(self, new_schema: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            with self._conn() as conn:
                pending = db.pending_writer_switches(conn)
                if pending:
                    raise GuardError(
                        "another writer switch is still unconfirmed "
                        f"({pending[0]['switch_id']} v{pending[0]['version']}); "
                        "reconcile it before proposing a new write format"
                    )
            # Guard 1: local static analysis against both effective readers.
            errors = self.guard_writer_switch(new_schema)
            if errors:
                raise GuardError("write format blocked by reader compatibility: "
                                 + " | ".join(errors))
            # Guard 2: both analyzers prove readability themselves.
            checks = self.remote_writer_checks(new_schema)
            bad = {k: v for k, v in checks.items() if not v.get("compatible")}
            if bad:
                raise GuardError(
                    "write format blocked: analyzer check failed: "
                    + json.dumps(bad, ensure_ascii=False))

            with self._conn() as conn:
                state = db.get_writer_state(conn)
                version = db.next_version(conn, "writer")
                fp = db.register_schema(conn, "writer", version, new_schema)
                switch_id = db.insert_writer_switch(
                    conn, version=version, base_version=state["effective_version"],
                    new_fp=fp, schema=new_schema, target="local",
                )
                conn.commit()

            self._drive_writer_switch(switch_id)
            with self._conn() as conn:
                return self._writer_switch_view(db.get_writer_switch(conn, switch_id))

    def _drive_writer_switch(self, switch_id: str) -> None:
        """Attempt the durable local mutation; transport trouble is recorded,
        never raised, and retried via reconciliation."""
        with self._lock:
            with self._conn() as conn:
                row = db.get_writer_switch(conn, switch_id)
                if row is None or row["status"] != "pending":
                    return
                new_schema = json.loads(row["schema_json"])
                version = row["version"]
                state = db.get_writer_state(conn)
                drop_armed = db.consume_drop_flag(conn, "drop_writer_switch")
                conn.commit()

            if state["effective_version"] >= version:
                # Idempotent replay: mutation already durable.
                self._post_switch_verify(switch_id, version, new_schema)
                return

            if drop_armed:
                # The receipt vanishes; whether the mutation landed is unknown.
                # Never report success - reconciliation will establish truth.
                self._record_writer_error(
                    switch_id,
                    "local writer switch receipt lost (simulated); effective "
                    "state unverified - reconciliation required",
                )
                return

            try:
                self._apply_local_writer(version, new_schema)
            except GuardError as exc:
                with self._conn() as conn:
                    db.update_writer_switch(conn, switch_id, "rejected", str(exc))
                    conn.commit()
                return

            self._post_switch_verify(switch_id, version, new_schema)

    def _post_switch_verify(self, switch_id: str, version: int,
                            new_schema: Dict[str, Any]) -> None:
        """Re-prove both analyzers can read the now-effective writer."""
        checks = self.remote_writer_checks(new_schema)
        bad = {k: v for k, v in checks.items() if not v.get("compatible")}
        if bad:
            self._record_writer_error(
                switch_id,
                "post-switch verification unconfirmed: "
                + json.dumps(bad, ensure_ascii=False),
            )
            return
        with self._conn() as conn:
            db.update_writer_switch(conn, switch_id, "confirmed")
            conn.commit()

    def _apply_local_writer(self, version: int, new_schema: Dict[str, Any]) -> None:
        """Fenced durable local writer mutation."""
        with self._conn() as conn:
            state = db.get_writer_state(conn)
            if state["effective_version"] > version:
                raise GuardError(
                    f"late switch v{version} cannot overwrite effective "
                    f"v{state['effective_version']}")
            if state["effective_version"] == version:
                return
            fp = fingerprint(new_schema)
            db.set_writer_state(conn, fp, version)
            conn.commit()

    def _record_writer_error(self, switch_id: str, message: str) -> None:
        with self._conn() as conn:
            db.update_writer_switch(conn, switch_id, "pending", message)
            conn.commit()

    # ======================================================================
    # Reader switch
    # ======================================================================

    def create_reader_switch(self, analyzer_id: str,
                             new_schema: Dict[str, Any]) -> Dict[str, Any]:
        if analyzer_id not in self.clients:
            raise GuardError(f"unknown analyzer '{analyzer_id}'")
        with self._lock:
            errors = self.guard_reader_switch(new_schema)
            if errors:
                raise GuardError("read format blocked: " + " | ".join(errors))

            with self._conn() as conn:
                state = db.get_writer_state(conn)
                version = db.next_version(conn, "reader")
                remote = self.clients[analyzer_id].reader_state()
                base_version = remote["effective_version"]
                db.register_schema(conn, "reader", version, new_schema)
                switch_id = db.insert_reader_switch(
                    conn, analyzer_id=analyzer_id, version=version,
                    base_version=base_version, schema=new_schema,
                    writer_fp=state["effective_fp"],
                )
                conn.commit()

            self._drive_reader_switch(switch_id)
            with self._conn() as conn:
                return self._reader_switch_view(
                    db.get_reader_switch(conn, switch_id))

    def _drive_reader_switch(self, switch_id: str) -> None:
        with self._lock:
            with self._conn() as conn:
                row = db.get_reader_switch(conn, switch_id)
                if row is None or row["status"] != "pending":
                    return
                analyzer_id = row["analyzer_id"]
                new_schema = json.loads(row["schema_json"])
                version, base = row["version"], row["base_version"]
                state = db.get_writer_state(conn)
                writer_schema = db.get_schema_by_fp(conn, state["effective_fp"])
                drop_armed = db.consume_drop_flag(conn, "drop_reader_switch")
                conn.commit()

            if drop_armed:
                self._record_reader_error(
                    switch_id,
                    "reader switch receipt lost (simulated); remote state "
                    "unverified - reconciliation required",
                )
                return
            try:
                result = self.clients[analyzer_id].reader_switch(
                    switch_id=switch_id, version=version, base_version=base,
                    reader_schema=new_schema, writer_schema=writer_schema,
                )
            except RemoteError as exc:
                self._record_reader_error(
                    switch_id, f"remote call unconfirmed: {exc}")
                return
            if result.get("status") == "stale":
                with self._conn() as conn:
                    db.update_reader_switch(conn, switch_id, "rejected",
                                            result.get("reason"))
                    conn.commit()
                return
            with self._conn() as conn:
                db.update_reader_switch(conn, switch_id, "confirmed")
                conn.commit()

    def _record_reader_error(self, switch_id: str, message: str) -> None:
        with self._conn() as conn:
            db.update_reader_switch(conn, switch_id, "pending", message)
            conn.commit()

    # ======================================================================
    # Reconciliation (restart / lost receipt / crash recovery)
    # ======================================================================

    def reconcile_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Verify real remote/local state and resume every pending switch."""
        result = {"writer": [], "reader": []}
        with self._lock:
            with self._conn() as conn:
                writer_rows = db.pending_writer_switches(conn)
                reader_rows = db.pending_reader_switches(conn)
            for row in writer_rows:
                result["writer"].append(
                    self._reconcile_writer(row["switch_id"]))
            for row in reader_rows:
                result["reader"].append(
                    self._reconcile_reader(row["switch_id"]))
        return result

    def _reconcile_writer(self, switch_id: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = db.get_writer_switch(conn, switch_id)
            version = row["version"]
            new_schema = json.loads(row["schema_json"])
            state = db.get_writer_state(conn)
            local_effective = state["effective_version"]

        # Fence: a newer version already went effective -> this one is late.
        if local_effective > version:
            with self._conn() as conn:
                db.update_writer_switch(
                    conn, switch_id, "rejected",
                    f"late switch v{version} superseded by effective "
                    f"v{local_effective}")
                conn.commit()
            return {"switch_id": switch_id, "outcome": "rejected_late"}

        # Establish whether the local mutation actually landed before any
        # receipt was lost, instead of blindly re-applying it.
        if local_effective < version:
            self._drive_writer_switch(switch_id)
            with self._conn() as conn:
                row2 = db.get_writer_switch(conn, switch_id)
            return {"switch_id": switch_id, "outcome": row2["status"]}

        # Local writer already at the requested version: verify analyzers.
        checks = self.remote_writer_checks(new_schema)
        bad = {k: v for k, v in checks.items() if not v.get("compatible")}
        if bad:
            self._record_writer_error(
                switch_id,
                "reconciliation: local writer applied, analyzer verification "
                "unconfirmed: " + json.dumps(bad, ensure_ascii=False))
            return {"switch_id": switch_id, "outcome": "pending_waiting_analyzers",
                    "checks": checks}
        with self._conn() as conn:
            db.update_writer_switch(conn, switch_id, "confirmed")
            conn.commit()
        return {"switch_id": switch_id, "outcome": "confirmed_from_observed_state",
                "checks": checks}

    def _reconcile_reader(self, switch_id: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = db.get_reader_switch(conn, switch_id)
            analyzer_id = row["analyzer_id"]
            version = row["version"]
            base = row["base_version"]
            new_schema = json.loads(row["schema_json"])
            state = db.get_writer_state(conn)
            writer_schema = db.get_schema_by_fp(conn, state["effective_fp"])

        try:
            remote = self.clients[analyzer_id].reader_state()
        except RemoteError as exc:
            self._record_reader_error(
                switch_id, f"reconciliation unreachable: {exc}")
            return {"switch_id": switch_id, "outcome": "pending_unreachable"}

        # Durably applied even though the receipt was lost: confirm from the
        # analyzer's own reported state.
        if remote["effective_version"] >= version:
            with self._conn() as conn:
                db.update_reader_switch(conn, switch_id, "confirmed")
                conn.commit()
            return {"switch_id": switch_id,
                    "outcome": "confirmed_from_remote_state",
                    "remote_version": remote["effective_version"]}

        # The analyzer moved to some other version not matching our base.
        if remote["effective_version"] != base:
            with self._conn() as conn:
                db.update_reader_switch(
                    conn, switch_id, "rejected",
                    f"base v{base} stale vs remote v{remote['effective_version']}")
                conn.commit()
            return {"switch_id": switch_id, "outcome": "rejected_late"}

        # Remote never persisted it: safely retry the idempotent fenced call.
        try:
            result = self.clients[analyzer_id].reader_switch(
                switch_id=switch_id, version=version, base_version=base,
                reader_schema=new_schema, writer_schema=writer_schema)
        except RemoteError as exc:
            self._record_reader_error(
                switch_id, f"retry unconfirmed: {exc}")
            return {"switch_id": switch_id, "outcome": "pending_retry_failed"}

        if result.get("status") == "stale":
            with self._conn() as conn:
                db.update_reader_switch(conn, switch_id, "rejected",
                                        result.get("reason"))
                conn.commit()
            return {"switch_id": switch_id, "outcome": "rejected_late"}
        with self._conn() as conn:
            db.update_reader_switch(conn, switch_id, "confirmed")
            conn.commit()
        return {"switch_id": switch_id, "outcome": "confirmed_on_retry"}

    # ======================================================================
    # Event ingestion / decoding
    # ======================================================================

    def ingest_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encode with the *currently effective* write format and retain it.

        Works regardless of any pending switch: only the confirmed effective
        writer pointer is ever used, so interleaved compatible events are never
        blocked or missed."""
        with self._conn() as conn:
            state = db.get_writer_state(conn)
            writer_schema = db.get_schema_by_fp(conn, state["effective_fp"])
        try:
            blob = encode(writer_schema, payload)
        except CodecError as exc:
            raise GuardError(str(exc))
        blob_b64 = base64.b64encode(blob).decode("ascii")
        with self._conn() as conn:
            event_id = db.insert_event(
                conn, writer_fp=state["effective_fp"],
                writer_version=state["effective_version"],
                payload=payload, blob_b64=blob_b64)
            conn.commit()
        return {"event_id": event_id, "writer_version": state["effective_version"],
                "writer_fp": state["effective_fp"], "size_bytes": len(blob)}

    def decode_event(self, event_id: str, analyzer_id: str) -> Dict[str, Any]:
        if analyzer_id not in self.clients:
            raise GuardError(f"unknown analyzer '{analyzer_id}'")
        with self._conn() as conn:
            row = db.get_event(conn, event_id)
            if row is None:
                raise GuardError(f"event '{event_id}' not found")
            writer_schema = db.get_schema_by_fp(conn, row["writer_fp"])
            blob = base64.b64decode(row["blob_b64"])
        result = self.clients[analyzer_id].decode(
            event_id=event_id, blob=blob, writer_schema=writer_schema)
        with self._conn() as conn:
            db.insert_decode_result(
                conn, event_id=event_id, analyzer_id=analyzer_id,
                reader_version=result.get("reader_version"),
                decoded_ok=bool(result.get("decoded_ok")),
                datum=result.get("datum"), error=result.get("error"),
            )
            conn.commit()
        return result

    # ======================================================================
    # Views
    # ======================================================================

    def _effective_reader(self, analyzer_id: str) -> Dict[str, Any]:
        remote = self.clients[analyzer_id].reader_state()
        return remote["reader_schema"]

    def status(self) -> Dict[str, Any]:
        with self._conn() as conn:
            state = db.get_writer_state(conn)
            writer_schema = db.get_schema_by_fp(conn, state["effective_fp"])
            events = [dict(e) for e in db.list_events(conn)]
            for e in events:
                e["payload"] = json.loads(e.pop("payload_json"))
                e.pop("blob_b64", None)
            wsw = [self._writer_switch_view(r)
                   for r in conn.execute(
                       "SELECT * FROM writer_switches ORDER BY rowid DESC LIMIT 10")]
            rsw = [self._reader_switch_view(r)
                   for r in conn.execute(
                       "SELECT * FROM reader_switches ORDER BY rowid DESC LIMIT 10")]
            decodes = [dict(r) for r in db.latest_decode_results(conn)]
            for d in decodes:
                d["datum"] = json.loads(d["datum_json"]) if d["datum_json"] else None
                d.pop("datum_json", None)
            sim = db.get_sim(conn)
            schemas = db.all_writer_schemas(conn)

        analyzers = {}
        for analyzer_id in ANALYZER_ORDER:
            try:
                remote = self.clients[analyzer_id].reader_state()
                remote["reachable"] = True
                try:
                    remote["simulation"] = self.clients[analyzer_id].get_simulate()
                except RemoteError:
                    remote["simulation"] = None
            except RemoteError as exc:
                remote = {"reachable": False, "error": str(exc),
                          "effective_version": None}
            analyzers[analyzer_id] = remote

        return {
            "effective_writer": {
                "version": state["effective_version"],
                "fingerprint": state["effective_fp"],
                "schema": writer_schema,
            },
            "analyzers": analyzers,
            "events": events,
            "writer_switches": wsw,
            "reader_switches": rsw,
            "decode_results": decodes,
            "simulation": sim,
            "writer_schemas": schemas,
        }

    @staticmethod
    def _writer_switch_view(row) -> Dict[str, Any]:
        return {
            "switch_id": row["switch_id"],
            "kind": "writer",
            "version": row["version"],
            "base_version": row["base_version"],
            "status": row["status"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "schema": json.loads(row["schema_json"]),
        }

    @staticmethod
    def _reader_switch_view(row) -> Dict[str, Any]:
        return {
            "switch_id": row["switch_id"],
            "kind": "reader",
            "analyzer_id": row["analyzer_id"],
            "version": row["version"],
            "base_version": row["base_version"],
            "status": row["status"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "schema": json.loads(row["schema_json"]),
        }
