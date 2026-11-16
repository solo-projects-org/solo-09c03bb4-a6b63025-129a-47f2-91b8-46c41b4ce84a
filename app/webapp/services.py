"""Switch orchestration: compatibility guards, durable two-phase protocol and
post-restart reconciliation.

Protocol summary
================

Writer (write-format) switch
----------------------------
1. Reject if another writer switch is still pending (serial, monotonic versions).
2. Guard locally (static + round-trip) against **every reader format that can
   still take effect**: the effective reader of both analyzers *and* the reader
   schemas of reader switches that are still pending (a lost receipt may have
   landed remotely or may still land later).
3. Ask *both* analyzers to prove their effective readers can read the proposed
   writer schema (``/writer/check``).
4. Persist a ``pending`` switch row *before* any mutation.
5. Only after the guard succeeded is the writer pointer released, inside one
   ``BEGIN IMMEDIATE`` transaction with a base-version fence and a per-switch
   idempotency log.  A simulated lost receipt leaves the row pending **without
   releasing** the format; reconciliation re-verifies before deciding.
6. Reconciliation never applies a pending switch blindly: it inspects the real
   pointer, distinguishes "our switch applied" (switch log) from "a newer
   switch advanced it", re-runs the full guard, and rejects conflicting
   switches without touching the pointer.  If a released format turns out to
   be unreadable and no event used it, the pointer is rolled back.

Reader (read-format) switch (per analyzer)
------------------------------------------
1. Guard locally against **every writer format that can still become an
   event**: the effective writer, formats of all retained events, formats held
   by in-flight ingestions (leases) and formats of pending writer switches.
   Retained events are additionally decoded for real with the new reader.
2. Persist ``pending`` row, then call the analyzer's atomic fenced switch with
   the full set of possible writer schemas; the analyzer re-validates, fences
   by ``base_version``, rejects version regressions and detects idempotency
   conflicts (same switch id, different payload).
3. Confirm only after observing the analyzer's real effective state.  On a
   lost receipt/crash keep ``pending`` and reconcile against that state,
   retrying the idempotent call only when the remote never changed.

Interleaving
------------
Ingestion takes a short-lived durable *lease* on the writer format inside a
``BEGIN IMMEDIATE`` transaction, encodes, then inserts the event and drops the
lease in one transaction.  Reader guards therefore see in-flight events too.
Ingestion and status queries never wait on the switch lock, so compatible
events keep landing while a slow verification is in progress.
"""
from __future__ import annotations

import base64
import json
import threading
from typing import Any, Dict, List, Optional

from ..avro_codec import (CodecError, SchemaError, decode as avro_decode,
                          encode, fingerprint, parse_schema, roundtrip_check)
from ..db_common import transaction
from . import db
from .analyzer_client import ANALYZER_ORDER, AnalyzerClient, RemoteError


class GuardError(Exception):
    """A precondition/compatibility guard rejected the request."""


class SwitchService:
    def __init__(self, db_path: str, clients: Dict[str, AnalyzerClient]):
        self.db_path = db_path
        self.clients = clients
        # Serialize switch mutations so a guard/verify/release decision cannot
        # interleave with another switch.  Ingestion and status never take it.
        self._lock = threading.RLock()

    def _conn(self):
        from ..db_common import get_conn
        return get_conn(self.db_path)

    # ======================================================================
    # Possible formats
    # ======================================================================

    def any_pending_switch(self) -> bool:
        with self._conn() as conn:
            return bool(db.pending_writer_switches(conn)
                        or db.pending_reader_switches(conn))

    def _possible_writer_formats(self, conn) -> List[Dict[str, Any]]:
        """Every writer schema an event can still be observed with.

        Includes: the effective pointer, retained events, in-flight ingestion
        leases, and pending writer switches (which may still become effective
        after a lost receipt / late execution).  Deduplicated by fingerprint.
        """
        formats: Dict[str, Dict[str, Any]] = {}

        def add(fp: str, version: int, schema: Optional[Dict[str, Any]],
                source: str) -> None:
            if schema is None:
                return
            fp = fp or fingerprint(schema)
            item = formats.get(fp)
            if item is None:
                formats[fp] = {"fp": fp, "version": version, "schema": schema,
                               "sources": [source]}
            else:
                item["version"] = max(item["version"], version)
                if source not in item["sources"]:
                    item["sources"].append(source)

        state = db.get_writer_state(conn)
        add(state["effective_fp"], state["effective_version"],
            db.get_schema_by_fp(conn, state["effective_fp"]), "current writer")
        for ev in conn.execute(
                "SELECT DISTINCT writer_fp, writer_version FROM events").fetchall():
            add(ev["writer_fp"], ev["writer_version"],
                db.get_schema_by_fp(conn, ev["writer_fp"]), "retained event")
        for lease in db.active_ingest_leases(conn):
            add(lease["writer_fp"], lease["writer_version"],
                db.get_schema_by_fp(conn, lease["writer_fp"]), "in-flight ingest")
        for row in db.pending_writer_switches(conn):
            add(row["new_fp"], row["version"],
                json.loads(row["schema_json"]), "pending writer switch")
        return list(formats.values())

    def _possible_reader_formats(self, conn,
                                 analyzer_id: str) -> List[Dict[str, Any]]:
        """Effective remote reader plus reader switches that may still land."""
        formats: Dict[str, Dict[str, Any]] = {}
        remote = self.clients[analyzer_id].reader_state()  # raises RemoteError
        fp = remote.get("reader_fp") or fingerprint(remote["reader_schema"])
        formats[fp] = {"fp": fp, "version": remote["effective_version"],
                       "schema": remote["reader_schema"], "sources": ["effective"]}
        for row in db.pending_reader_switches_for(conn, analyzer_id):
            schema = json.loads(row["schema_json"])
            rfp = fingerprint(schema)
            item = formats.get(rfp)
            if item is None:
                formats[rfp] = {"fp": rfp, "version": row["version"],
                                "schema": schema,
                                "sources": ["pending reader switch"]}
            else:
                item["version"] = max(item["version"], row["version"])
                if "pending reader switch" not in item["sources"]:
                    item["sources"].append("pending reader switch")
        return list(formats.values())

    # ======================================================================
    # Compatibility guards
    # ======================================================================

    def guard_writer_switch(self, new_writer: Dict[str, Any]) -> List[str]:
        """Local static + round-trip guard against every possible reader.

        Returns a list of field-path errors (empty == compatible)."""
        errors: List[str] = []
        try:
            parse_schema(new_writer)
        except SchemaError as exc:
            return [str(exc)]
        with self._conn() as conn:
            for analyzer_id in ANALYZER_ORDER:
                try:
                    readers = self._possible_reader_formats(conn, analyzer_id)
                except RemoteError as exc:
                    # Fail closed: an unreachable service cannot prove anything.
                    errors.append(f"[{analyzer_id}] reader state unreachable: {exc}")
                    continue
                for fmt in readers:
                    err = roundtrip_check(fmt["schema"], new_writer)
                    if err:
                        errors.append(
                            f"[{analyzer_id} reader v{fmt['version']} "
                            f"({'/'.join(fmt['sources'])}; fp {fmt['fp'][:8]})] {err}")
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
        """Reader must read every writer format that may still be observed:
        current, retained, in-flight, and pending-writer formats."""
        errors: List[str] = []
        seen: set = set()

        def add_error(message: str) -> None:
            if message not in seen:
                seen.add(message)
                errors.append(message)

        try:
            parse_schema(new_reader)
        except SchemaError as exc:
            return [str(exc)]
        with self._conn() as conn:
            writers = self._possible_writer_formats(conn)
            for fmt in writers:
                err = roundtrip_check(new_reader, fmt["schema"])
                if err:
                    add_error(
                        f"[possible writer v{fmt['version']} "
                        f"({'/'.join(fmt['sources'])}; fp {fmt['fp'][:8]})] {err}")
            # Real data check: every retained event must actually decode.
            for ev in conn.execute("SELECT * FROM events ORDER BY rowid").fetchall():
                wschema = db.get_schema_by_fp(conn, ev["writer_fp"])
                blob = base64.b64decode(ev["blob_b64"])
                datum, derr = avro_decode(new_reader, blob, wschema)
                if derr:
                    add_error(
                        f"[retained event {ev['id']} writer v{ev['writer_version']} "
                        f"fp={ev['writer_fp'][:8]}] {derr}")
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
            # Guard 1: local static/round-trip analysis against every possible
            # reader (effective + still-pending reader switches).
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

            with self._conn() as conn, transaction(conn, immediate=True):
                if db.pending_writer_switches(conn):
                    raise GuardError("another writer switch became pending concurrently")
                state = db.get_writer_state(conn)
                version = db.next_version(conn, "writer")
                fp = db.register_schema(conn, "writer", version, new_schema)
                switch_id = db.insert_writer_switch(
                    conn, version=version, base_version=state["effective_version"],
                    new_fp=fp, schema=new_schema, target="local",
                )

            self._drive_writer_switch(switch_id)
            with self._conn() as conn:
                return self._writer_switch_view(db.get_writer_switch(conn, switch_id))

    def _drive_writer_switch(self, switch_id: str) -> None:
        """Verify first, release second.  Anything unconfirmed stays pending
        *without* the write format having been released."""
        with self._lock:
            with self._conn() as conn:
                row = db.get_writer_switch(conn, switch_id)
                if row is None or row["status"] != "pending":
                    return
                new_schema = json.loads(row["schema_json"])
                version, base, new_fp = (row["version"], row["base_version"],
                                         row["new_fp"])
                state = db.get_writer_state(conn)

            if state["effective_version"] > version:
                self._reject_writer(
                    switch_id,
                    f"late switch v{version} superseded by effective "
                    f"v{state['effective_version']}")
                return
            if state["effective_version"] == version:
                # Applied before a crash / receipt loss: verify real state.
                self._settle_applied_writer(switch_id, version, new_fp, new_schema)
                return
            if state["effective_version"] != base:
                self._reject_writer(
                    switch_id,
                    f"base v{base} no longer matches effective "
                    f"v{state['effective_version']}; switch not applied")
                return

            # Re-validate immediately before release (covers reader switches
            # that were confirmed while a receipt was lost).
            errors = self.guard_writer_switch(new_schema)
            if errors:
                self._reject_writer(
                    switch_id,
                    "switch blocked at release: a possible reader cannot read "
                    "it: " + " | ".join(errors))
                return
            checks = self.remote_writer_checks(new_schema)
            bad = {k: v for k, v in checks.items() if not v.get("compatible")}
            if bad:
                # Never release a format an analyzer cannot prove it reads.
                self._record_writer_error(
                    switch_id,
                    "release withheld: analyzer verification unconfirmed: "
                    + json.dumps(bad, ensure_ascii=False))
                return

            if db.drop_armed(self.db_path, "drop_writer_switch"):
                # Simulated lost receipt: the release is deliberately not
                # performed, so nothing was handed over silently.  State stays
                # pending; reconciliation verifies and decides.
                db.clear_drop_flag(self.db_path, "drop_writer_switch")
                self._record_writer_error(
                    switch_id,
                    "local writer switch receipt lost (simulated) before release; "
                    "effective state unverified - reconciliation required")
                return

            try:
                with self._conn() as conn, transaction(conn, immediate=True):
                    result = db.apply_writer_switch_fenced(
                        conn, switch_id=switch_id, version=version,
                        base_version=base, fp=new_fp)
            except Exception as exc:  # pragma: no cover - defensive
                self._record_writer_error(
                    switch_id, f"local writer switch unconfirmed: {exc}")
                return

            status = result.get("status")
            if status in ("applied", "already_applied"):
                self._settle_applied_writer(
                    switch_id, version, new_fp, new_schema)
                return
            self._reject_writer(
                switch_id,
                result.get("reason") or f"writer switch {status}")

    def _settle_applied_writer(self, switch_id: str, version: int, new_fp: str,
                               new_schema: Dict[str, Any]) -> None:
        """The pointer is at our version: verify real state, then confirm.

        If the released format can no longer be read by a possible reader and
        no event has used it, roll the pointer back and reject instead of
        leaving a conflicting release in place."""
        with self._conn() as conn:
            state = db.get_writer_state(conn)
            if state["effective_version"] != version:
                if state["effective_version"] > version:
                    self._reject_writer(
                        switch_id,
                        f"late switch v{version} superseded by effective "
                        f"v{state['effective_version']}")
                return
            if state["effective_fp"] != new_fp:
                self._reject_writer(
                    switch_id,
                    f"effective v{version} carries a different schema "
                    f"(fp {state['effective_fp'][:8]}) - switch not applied")
                return

        errors = self.guard_writer_switch(new_schema)
        if errors:
            with self._conn() as conn, transaction(conn, immediate=True):
                row = db.get_writer_switch(conn, switch_id)
                state = db.get_writer_state(conn)
                used = db.count_events_for_writer(conn, new_fp, version)
                leased = any(l["writer_fp"] == new_fp and
                             l["writer_version"] == version
                             for l in db.active_ingest_leases(conn))
                if (state["effective_version"] == version
                        and state["effective_fp"] == new_fp
                        and used == 0 and not leased):
                    base_fp = _base_fp_for_switch(conn, row["base_version"],
                                                  new_fp)
                    if base_fp is not None:
                        db.set_writer_state(conn, base_fp,
                                            row["base_version"], switch_id=None)
                        db.update_writer_switch(
                            conn, switch_id, "rejected",
                            "conflicting release rolled back: " + " | ".join(errors))
                        return
            self._record_writer_error(
                switch_id,
                "released format can no longer be confirmed: " + " | ".join(errors))
            return

        checks = self.remote_writer_checks(new_schema)
        bad = {k: v for k, v in checks.items() if not v.get("compatible")}
        if bad:
            # Transport/verification trouble after release: keep pending so the
            # reconciler keeps trying.  The format was guard-verified (locally
            # and against the remote readers) immediately before release, and
            # ingestion keeps using this already-guarded pointer.
            self._record_writer_error(
                switch_id,
                "post-release verification unconfirmed: "
                + json.dumps(bad, ensure_ascii=False))
            return
        with self._conn() as conn, transaction(conn, immediate=True):
            db.update_writer_switch(conn, switch_id, "confirmed", None)

    def _reject_writer(self, switch_id: str, reason: str) -> None:
        with self._conn() as conn, transaction(conn, immediate=True):
            db.update_writer_switch(conn, switch_id, "rejected", reason)

    def _record_writer_error(self, switch_id: str, message: str) -> None:
        with self._conn() as conn, transaction(conn, immediate=True):
            db.update_writer_switch(conn, switch_id, "pending", message)

    # ======================================================================
    # Reader switch
    # ======================================================================

    def create_reader_switch(self, analyzer_id: str,
                             new_schema: Dict[str, Any]) -> Dict[str, Any]:
        if analyzer_id not in self.clients:
            raise GuardError(f"unknown analyzer '{analyzer_id}'")
        with self._lock:
            # Note: a pending writer switch does NOT block a reader upgrade by
            # itself - only a format it cannot read does.  All switch mutations
            # take ``self._lock``, so the "possible formats" set used by the
            # guard is stable for the duration of this decision; a compatible
            # reader may therefore be upgraded while a write switch awaits
            # confirmation, and the writer's own release re-validates against
            # this new reader.
            with self._conn() as conn:
                pending = db.pending_reader_switches_for(conn, analyzer_id)
                if pending:
                    raise GuardError(
                        f"{analyzer_id} already has an unconfirmed reader switch "
                        f"({pending[0]['switch_id']} v{pending[0]['version']}); "
                        "reconcile it before proposing a new read format")
            errors = self.guard_reader_switch(new_schema)
            if errors:
                raise GuardError("read format blocked: " + " | ".join(errors))

            try:
                remote = self.clients[analyzer_id].reader_state()
            except RemoteError as exc:
                raise GuardError(f"{analyzer_id} reader state unreachable: {exc}")

            with self._conn() as conn, transaction(conn, immediate=True):
                if db.pending_reader_switches_for(conn, analyzer_id):
                    raise GuardError(
                        f"{analyzer_id} acquired a pending switch concurrently")
                state = db.get_writer_state(conn)
                version = db.next_version(conn, "reader")
                db.register_schema(conn, "reader", version, new_schema)
                switch_id = db.insert_reader_switch(
                    conn, analyzer_id=analyzer_id, version=version,
                    base_version=remote["effective_version"], schema=new_schema,
                    writer_fp=state["effective_fp"],
                )

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
                writer_schemas = [f["schema"] for f in
                                  self._possible_writer_formats(conn)]

            # Re-validate against the formats possible right now, then verify
            # the remote state is still the base we are fencing on.
            errors = self.guard_reader_switch(new_schema)
            if errors:
                self._reject_reader(
                    switch_id,
                    "switch blocked at dispatch: " + " | ".join(errors))
                return
            try:
                remote = self.clients[analyzer_id].reader_state()
            except RemoteError as exc:
                self._record_reader_error(
                    switch_id, f"remote state unconfirmed at dispatch: {exc}")
                return
            if remote["effective_version"] >= version:
                self._settle_reader_state(switch_id, analyzer_id, version,
                                          new_schema, remote)
                return
            if remote["effective_version"] != base:
                self._reject_reader(
                    switch_id,
                    f"base v{base} stale vs remote v{remote['effective_version']}; "
                    "switch not dispatched")
                return

            if db.drop_armed(self.db_path, "drop_reader_switch"):
                db.clear_drop_flag(self.db_path, "drop_reader_switch")
                self._record_reader_error(
                    switch_id,
                    "reader switch receipt lost (simulated) before dispatch; "
                    "remote state unverified - reconciliation required")
                return

            try:
                result = self.clients[analyzer_id].reader_switch(
                    switch_id=switch_id, version=version, base_version=base,
                    reader_schema=new_schema, writer_schemas=writer_schemas,
                )
            except RemoteError as exc:
                if exc.status in (400, 422):
                    # The analyzer itself rejected the format: definitive.
                    self._reject_reader(
                        switch_id, f"analyzer rejected the switch: {exc}")
                else:
                    self._record_reader_error(
                        switch_id, f"remote call unconfirmed: {exc}")
                return

            status = result.get("status")
            if status in ("stale", "conflict"):
                self._reject_reader(
                    switch_id, result.get("reason") or f"remote switch {status}")
                return
            if status not in ("applied", "already_applied"):
                self._record_reader_error(
                    switch_id, f"unexpected remote status: {status}")
                return

            # Always verify the real effective state after the call.
            try:
                remote = self.clients[analyzer_id].reader_state()
            except RemoteError as exc:
                self._record_reader_error(
                    switch_id, f"post-switch state unconfirmed: {exc}")
                return
            self._settle_reader_state(switch_id, analyzer_id, version,
                                      new_schema, remote)

    def _settle_reader_state(self, switch_id: str, analyzer_id: str, version: int,
                             new_schema: Dict[str, Any], remote: Dict[str, Any]
                             ) -> str:
        """Decide a pending reader switch purely from observed remote state."""
        expected_fp = fingerprint(new_schema)
        remote_version = remote["effective_version"]
        remote_fp = remote.get("reader_fp") or fingerprint(remote["reader_schema"])
        if remote_version == version and remote_fp == expected_fp:
            with self._conn() as conn, transaction(conn, immediate=True):
                db.update_reader_switch(conn, switch_id, "confirmed")
            return "confirmed"
        if remote_version >= version:
            if remote_fp == expected_fp:
                with self._conn() as conn, transaction(conn, immediate=True):
                    db.update_reader_switch(conn, switch_id, "confirmed")
                return "confirmed"
            self._reject_reader(
                switch_id,
                f"switch v{version} superseded by remote v{remote_version} "
                f"(fp {remote_fp[:8]}); it did not overwrite the newer reader")
            return "rejected_late"
        # Remote is behind the requested version.  If it is no longer on our
        # base either, another switch moved it: ours is late.
        with self._conn() as conn:
            row = db.get_reader_switch(conn, switch_id)
            base = row["base_version"]
        if remote_version != base:
            self._reject_reader(
                switch_id,
                f"base v{base} stale vs remote v{remote_version}; switch is late")
            return "rejected_late"
        self._record_reader_error(
            switch_id,
            f"remote still at base v{base}; switch not observed yet - retry pending")
        return "pending"

    def _reject_reader(self, switch_id: str, reason: str) -> None:
        with self._conn() as conn, transaction(conn, immediate=True):
            db.update_reader_switch(conn, switch_id, "rejected", reason)

    def _record_reader_error(self, switch_id: str, message: str) -> None:
        with self._conn() as conn, transaction(conn, immediate=True):
            db.update_reader_switch(conn, switch_id, "pending", message)

    # ======================================================================
    # Reconciliation (restart / lost receipt / crash recovery)
    # ======================================================================

    def reconcile_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Verify real remote/local state and resume every pending switch."""
        result = {"writer": [], "reader": []}
        with self._lock:
            with self._conn() as conn, transaction(conn, immediate=True):
                db.purge_expired_ingest_leases(conn)
                writer_rows = db.pending_writer_switches(conn)
                reader_rows = db.pending_reader_switches(conn)
            for row in writer_rows:
                try:
                    result["writer"].append(self._reconcile_writer(row["switch_id"]))
                except Exception as exc:  # keep reconciling the rest
                    result["writer"].append(
                        {"switch_id": row["switch_id"], "outcome": "error",
                         "error": str(exc)})
            for row in reader_rows:
                try:
                    result["reader"].append(self._reconcile_reader(row["switch_id"]))
                except Exception as exc:
                    result["reader"].append(
                        {"switch_id": row["switch_id"], "outcome": "error",
                         "error": str(exc)})
        return result

    def _reconcile_writer(self, switch_id: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = db.get_writer_switch(conn, switch_id)
            if row is None or row["status"] != "pending":
                return {"switch_id": switch_id, "outcome": row["status"]
                        if row else "missing"}
            version = row["version"]
            base = row["base_version"]
            new_fp = row["new_fp"]
            new_schema = json.loads(row["schema_json"])
            state = db.get_writer_state(conn)

        local_effective = state["effective_version"]
        # Fence: a newer version already went effective -> this one is late.
        if local_effective > version:
            self._reject_writer(
                switch_id,
                f"late switch v{version} superseded by effective "
                f"v{local_effective}")
            return {"switch_id": switch_id, "outcome": "rejected_late"}
        if local_effective == version:
            if state["effective_fp"] != new_fp:
                self._reject_writer(
                    switch_id,
                    f"effective v{version} has fp "
                    f"{state['effective_fp'][:8]}, not this switch's "
                    f"{new_fp[:8]}; switch was not applied")
                return {"switch_id": switch_id, "outcome": "rejected_conflict"}
            self._settle_applied_writer(switch_id, version, new_fp, new_schema)
            with self._conn() as conn:
                row2 = db.get_writer_switch(conn, switch_id)
            return {"switch_id": switch_id, "outcome": row2["status"]}
        if local_effective != base:
            self._reject_writer(
                switch_id,
                f"base v{base} stale vs effective v{local_effective}; "
                "switch not applied")
            return {"switch_id": switch_id, "outcome": "rejected_late"}

        # Never applied: re-validate against current possible readers before
        # any release, then drive the same persisted row.
        self._drive_writer_switch(switch_id)
        with self._conn() as conn:
            row2 = db.get_writer_switch(conn, switch_id)
        return {"switch_id": switch_id, "outcome": row2["status"]}

    def _reconcile_reader(self, switch_id: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = db.get_reader_switch(conn, switch_id)
            if row is None or row["status"] != "pending":
                return {"switch_id": switch_id,
                        "outcome": row["status"] if row else "missing"}
            analyzer_id = row["analyzer_id"]
            version = row["version"]
            base = row["base_version"]
            new_schema = json.loads(row["schema_json"])

        try:
            remote = self.clients[analyzer_id].reader_state()
        except RemoteError as exc:
            self._record_reader_error(
                switch_id, f"reconciliation unreachable: {exc}")
            return {"switch_id": switch_id, "outcome": "pending_unreachable"}

        outcome = self._settle_reader_state(
            switch_id, analyzer_id, version, new_schema, remote)
        if outcome != "pending":
            named = ("confirmed_from_remote_state" if outcome == "confirmed"
                     else "rejected_late")
            return {"switch_id": switch_id, "outcome": named,
                    "remote_version": remote["effective_version"]}

        # Remote never moved.  Re-guard against the *current* possible writer
        # formats (a conflicting writer switch may have been released while
        # the receipt was lost) and only then retry the idempotent call.
        errors = self.guard_reader_switch(new_schema)
        if errors:
            self._reject_reader(
                switch_id,
                "retry blocked by current formats: " + " | ".join(errors))
            return {"switch_id": switch_id, "outcome": "rejected_conflict"}
        with self._conn() as conn:
            writer_schemas = [f["schema"] for f in
                              self._possible_writer_formats(conn)]
        try:
            result = self.clients[analyzer_id].reader_switch(
                switch_id=switch_id, version=version, base_version=base,
                reader_schema=new_schema, writer_schemas=writer_schemas)
        except RemoteError as exc:
            if exc.status in (400, 422):
                self._reject_reader(
                    switch_id, f"analyzer rejected the retry: {exc}")
                return {"switch_id": switch_id,
                        "outcome": "rejected_conflict"}
            self._record_reader_error(switch_id, f"retry unconfirmed: {exc}")
            return {"switch_id": switch_id, "outcome": "pending_retry_failed"}

        if result.get("status") in ("stale", "conflict"):
            self._reject_reader(
                switch_id, result.get("reason") or result.get("status"))
            return {"switch_id": switch_id, "outcome": "rejected_late"}
        try:
            remote2 = self.clients[analyzer_id].reader_state()
        except RemoteError as exc:
            self._record_reader_error(
                switch_id, f"retry applied but state unconfirmed: {exc}")
            return {"switch_id": switch_id, "outcome": "pending_verify"}
        outcome = self._settle_reader_state(
            switch_id, analyzer_id, version, new_schema, remote2)
        named = ("confirmed_on_retry" if outcome == "confirmed"
                 else outcome)
        return {"switch_id": switch_id, "outcome": named,
                "remote_version": remote2["effective_version"]}

    # ======================================================================
    # Event ingestion / decoding
    # ======================================================================

    def ingest_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encode with the *currently effective* write format and retain it.

        A short durable lease is taken before encoding and removed in the same
        transaction that inserts the event, so upgrade guards see in-flight
        events.  No switch lock is taken: ingestion proceeds while a slow
        verification is running, and it only ever uses the confirmed pointer.
        The lease is renewed (with a compare-and-set on the version it still
        holds) right after encoding, so a guard cannot mistake a slow encode
        for an abandoned lease and let a reader upgrade land in the gap.
        Ingestion is never paused for a pending switch (only the confirmed
        pointer is used), including while a switch is being verified slowly.
        """
        with self._conn() as conn, transaction(conn, immediate=True):
            state = db.get_writer_state(conn)
            writer_schema = db.get_schema_by_fp(conn, state["effective_fp"])
            lease_id = db.begin_ingest_lease(
                conn, writer_fp=state["effective_fp"],
                writer_version=state["effective_version"])
        try:
            blob = encode(writer_schema, payload)
        except CodecError as exc:
            with self._conn() as conn, transaction(conn, immediate=True):
                db.finish_ingest_lease(conn, lease_id)
            raise GuardError(str(exc))
        blob_b64 = base64.b64encode(blob).decode("ascii")
        with self._conn() as conn, transaction(conn, immediate=True):
            # The lease must still be ours (nobody declared it abandoned).
            if not db.refresh_ingest_lease(conn, lease_id):
                raise GuardError(
                    "in-flight ingestion lease was lost while encoding; the "
                    "event was not accepted - please retry")
            event_id = db.insert_event(
                conn, writer_fp=state["effective_fp"],
                writer_version=state["effective_version"],
                payload=payload, blob_b64=blob_b64)
            db.finish_ingest_lease(conn, lease_id)
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
        with self._conn() as conn, transaction(conn, immediate=True):
            db.insert_decode_result(
                conn, event_id=event_id, analyzer_id=analyzer_id,
                reader_version=result.get("reader_version"),
                decoded_ok=bool(result.get("decoded_ok")),
                datum=result.get("datum"), error=result.get("error"),
            )
        return result

    # ======================================================================
    # Views
    # ======================================================================

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
            leases = [dict(r) for r in db.active_ingest_leases(conn)]
            for lease in leases:
                lease.pop("expires_at", None)

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
            "in_flight_ingests": leases,
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


def _base_fp_for_switch(conn, base_version: int, exclude_fp: str) -> Optional[str]:
    """Exact fingerprint of the pre-switch (base) format, for rollback only.

    Never falls back to an unrelated version: rolling back to a schema that
    was not the base would itself be a conflicting release.
    """
    row = conn.execute(
        "SELECT fp FROM format_versions WHERE kind = 'writer' AND version = ?",
        (base_version,)).fetchone()
    if row is None or row["fp"] == exclude_fp:
        return None
    return row["fp"]
