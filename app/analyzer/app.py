"""Independent analyzer service.

Each instance (analyzer-a / analyzer-b) owns a *persistent reader schema* in
its own SQLite database. The web platform asks an analyzer to:

  * switch its reader schema (durable, idempotent, fenced by a version number),
  * decode a retained event using its *currently effective* reader schema,
  * report its actual state so the webapp can reconcile after a crash or a
    lost receipt.

An analyzer never trusts the webapp's notion of "current": every switch is
re-validated locally against the writer schema and fenced by ``base_version``
so a late, duplicated or replayed switch can never overwrite a newer one.

``create_app`` builds an isolated application bound to its own database, which
lets the test-suite run two fully independent analyzors in one process; the
module-level ``app`` is the uvicorn entrypoint configured from environment.
"""
from __future__ import annotations

import base64
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..avro_codec import decode, fingerprint, parse_schema, roundtrip_check
from ..avro_compat import explain
from . import db


# --- request models ---------------------------------------------------------

class ReaderSwitchReq(BaseModel):
    switch_id: str
    version: int = Field(..., gt=0)
    base_version: int = Field(..., ge=0)
    reader_schema: Dict[str, Any]
    # Every writer format that may still be observed: current, retained
    # events, in-flight ingestions and still-unconfirmed writer switches.
    writer_schemas: List[Dict[str, Any]] = Field(default_factory=list)
    # Retained for backwards compatibility with older platforms.
    writer_schema: Optional[Dict[str, Any]] = None


class WriterCheckReq(BaseModel):
    """Read-only proof that this analyzer's effective reader can read a
    proposed writer schema. Used by the platform before releasing a new write
    format; nothing is persisted here."""
    writer_schema: Dict[str, Any]


class DecodeReq(BaseModel):
    event_id: str
    blob_b64: str
    writer_schema: Dict[str, Any]


class SimReq(BaseModel):
    delay_ms: int = 0
    drop_switch_response: bool = False
    drop_decode_response: bool = False


def create_app(analyzer_id: str, db_path: str) -> FastAPI:
    """Factory: isolated FastAPI app bound to its own persistent database."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init(db_path)
        yield

    app = FastAPI(title=f"Analyzer {analyzer_id}", lifespan=lifespan)

    def _state() -> Dict[str, Any]:
        row = db.get_state(db_path)
        reader_schema = json.loads(row["reader_schema_json"])
        return {
            "analyzer_id": analyzer_id,
            "effective_version": row["version"],
            "reader_schema": reader_schema,
            "reader_fp": row["reader_fp"] or fingerprint(reader_schema),
            "updated_at": row["updated_at"],
            "last_switch_id": row["last_switch_id"],
        }

    def _maybe_drop(sim: Dict[str, Any], kind: str, payload: Dict[str, Any]):
        flag = ("drop_switch_response" if kind == "switch"
                else "drop_decode_response")
        if sim[flag]:
            db.reset_drop_flag(db_path, flag)  # one-shot: only next call lost
            raise HTTPException(503, "simulated lost response (receipt dropped)")
        return payload

    @app.get("/health")
    def health():
        return {"status": "ok", "analyzer_id": analyzer_id}

    @app.get("/reader/state")
    def reader_state():
        """Truth source for reconciliation after crash / lost receipt."""
        return _state()

    @app.post("/writer/check")
    def writer_check(req: WriterCheckReq):
        """Read-only: validate the proposed writer schema against the
        effective reader and prove a real binary round-trip."""
        sim = db.get_sim(db_path)
        if sim["delay_ms"]:
            time.sleep(min(sim["delay_ms"] / 1000.0, 5.0))
        row = db.get_state(db_path)
        reader_schema = json.loads(row["reader_schema_json"])
        try:
            err = roundtrip_check(reader_schema, req.writer_schema)
        except Exception as exc:  # malformed schema
            raise HTTPException(400, f"invalid writer schema: {exc}")
        payload = {
            "analyzer_id": analyzer_id,
            "reader_version": row["version"],
            "compatible": err is None,
            "error": err,
        }
        # Read-only proof: never consumes the one-shot "lost switch response"
        # fault, which must apply to the next actual reader switch.
        return payload

    @app.post("/reader/switch")
    def reader_switch(req: ReaderSwitchReq):
        sim = db.get_sim(db_path)
        if sim["delay_ms"]:
            time.sleep(min(sim["delay_ms"] / 1000.0, 5.0))

        row = db.get_state(db_path)
        current_version = row["version"]
        current_fp = row["reader_fp"] or fingerprint(
            json.loads(row["reader_schema_json"]))
        requested_fp = fingerprint(req.reader_schema)

        # Idempotent replay of the exact same switch: answer from state.
        if (row["last_switch_id"] == req.switch_id
                and current_version == req.version):
            if current_fp != requested_fp:
                return _maybe_drop(sim, "switch", {
                    "status": "conflict",
                    "analyzer_id": analyzer_id,
                    "switch_id": req.switch_id,
                    "effective_version": current_version,
                    "reason": (f"switch {req.switch_id} was already applied "
                               "with a different reader schema"),
                })
            result = {
                "status": "already_applied",
                "analyzer_id": analyzer_id,
                "switch_id": req.switch_id,
                "effective_version": current_version,
                "reader_version": current_version,
                "reader_fp": current_fp,
            }
            return _maybe_drop(sim, "switch", result)

        # Fence: a late / replayed / not-newer version must never clobber the
        # effective reader.  Version regression is rejected explicitly.
        if req.version <= current_version or current_version != req.base_version:
            reason = (
                f"requested version {req.version} is not newer than effective "
                f"version {current_version}; late switch rejected"
                if req.version <= current_version else
                f"base_version {req.base_version} does not match effective "
                f"version {current_version}; late switch rejected")
            result = {
                "status": "stale",
                "analyzer_id": analyzer_id,
                "effective_version": current_version,
                "requested_version": req.version,
                "reason": reason,
            }
            return _maybe_drop(sim, "switch", result)

        # Local guard: the analyzer validates schemas and compatibility itself,
        # against *every* writer format the platform may still hand over.
        writer_schemas = list(req.writer_schemas)
        if req.writer_schema is not None and req.writer_schema not in writer_schemas:
            writer_schemas.append(req.writer_schema)
        if not writer_schemas:
            raise HTTPException(400, "writer_schemas is required")
        try:
            parse_schema(req.reader_schema)
            for wschema in writer_schemas:
                parse_schema(wschema)
        except Exception as exc:
            raise HTTPException(400, f"invalid schema: {exc}")
        for index, wschema in enumerate(writer_schemas):
            err = explain(req.reader_schema, wschema)
            if err:
                raise HTTPException(422, {
                    "status": "incompatible", "error": err,
                    "writer_index": index,
                    "writer_fp": fingerprint(wschema),
                })
            # Prove a real binary round-trip, not just static reasoning.
            rt_err = roundtrip_check(req.reader_schema, wschema)
            if rt_err:
                raise HTTPException(422, {
                    "status": "incompatible", "error": rt_err,
                    "writer_index": index,
                    "writer_fp": fingerprint(wschema),
                })

        applied = db.apply_switch(
            db_path,
            switch_id=req.switch_id,
            version=req.version,
            base_version=req.base_version,
            reader_schema=req.reader_schema,
            writer_schemas=writer_schemas,
        )
        status = applied.get("status")
        if status not in ("applied", "already_applied"):
            result = {
                "status": status,
                "analyzer_id": analyzer_id,
                "switch_id": req.switch_id,
                "effective_version": applied.get("effective_version",
                                                 current_version),
                "requested_version": req.version,
                "reason": applied.get("reason"),
            }
            return _maybe_drop(sim, "switch", result)
        result = {
            "status": status,
            "analyzer_id": analyzer_id,
            "switch_id": req.switch_id,
            "effective_version": applied["effective_version"],
            "reader_version": applied["effective_version"],
            "reader_fp": applied.get("reader_fp", requested_fp),
        }
        return _maybe_drop(sim, "switch", result)

    @app.post("/events/decode")
    def events_decode(req: DecodeReq):
        sim = db.get_sim(db_path)
        if sim["delay_ms"]:
            time.sleep(min(sim["delay_ms"] / 1000.0, 5.0))

        state = db.get_state(db_path)
        reader_schema = json.loads(state["reader_schema_json"])
        try:
            blob = base64.b64decode(req.blob_b64)
        except Exception as exc:
            raise HTTPException(400, f"invalid base64 payload: {exc}")

        datum, err = decode(reader_schema, blob, req.writer_schema)
        result = {
            "analyzer_id": analyzer_id,
            "event_id": req.event_id,
            "reader_version": state["version"],
            "decoded_ok": err is None,
            "datum": datum,
            "error": err,
        }
        return _maybe_drop(sim, "decode", result)

    @app.post("/simulate")
    def simulate(req: SimReq):
        db.set_sim(db_path, req.delay_ms, req.drop_switch_response,
                   req.drop_decode_response)
        return {"status": "armed", **req.model_dump()}

    @app.get("/simulate")
    def get_simulate():
        return db.get_sim(db_path)

    return app


# Default uvicorn entrypoint, configured from environment.
ANALYZER_ID = os.environ.get("ANALYZER_ID", "analyzer")
DB_PATH = os.environ.get("ANALYZER_DB", f"/data/{ANALYZER_ID}.db")
app = create_app(ANALYZER_ID, DB_PATH)
