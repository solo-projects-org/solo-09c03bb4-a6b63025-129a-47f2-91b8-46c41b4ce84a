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
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..avro_codec import decode, parse_schema, roundtrip_check
from ..avro_compat import explain
from . import db


# --- request models ---------------------------------------------------------

class ReaderSwitchReq(BaseModel):
    switch_id: str
    version: int = Field(..., gt=0)
    base_version: int = Field(..., ge=0)
    reader_schema: Dict[str, Any]
    writer_schema: Dict[str, Any]


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
        return {
            "analyzer_id": analyzer_id,
            "effective_version": row["version"],
            "reader_schema": json.loads(row["reader_schema_json"]),
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
        return _maybe_drop(sim, "switch", payload)

    @app.post("/reader/switch")
    def reader_switch(req: ReaderSwitchReq):
        sim = db.get_sim(db_path)
        if sim["delay_ms"]:
            time.sleep(min(sim["delay_ms"] / 1000.0, 5.0))

        row = db.get_state(db_path)
        current_version = row["version"]

        # Idempotent replay of the exact same switch: answer from state.
        if (row["last_switch_id"] == req.switch_id
                and current_version == req.version):
            result = {
                "status": "already_applied",
                "analyzer_id": analyzer_id,
                "switch_id": req.switch_id,
                "effective_version": current_version,
                "reader_version": current_version,
            }
            return _maybe_drop(sim, "switch", result)

        # Fence: a switch based on an outdated base must never clobber a newer.
        if current_version != req.base_version:
            result = {
                "status": "stale",
                "analyzer_id": analyzer_id,
                "effective_version": current_version,
                "requested_version": req.version,
                "reason": (f"base_version {req.base_version} does not match "
                           f"effective version {current_version}; late switch "
                           "rejected"),
            }
            return _maybe_drop(sim, "switch", result)

        # Local guard: the analyzer validates schemas and compatibility itself.
        try:
            parse_schema(req.reader_schema)
            parse_schema(req.writer_schema)
        except Exception as exc:
            raise HTTPException(400, f"invalid schema: {exc}")
        err = explain(req.reader_schema, req.writer_schema)
        if err:
            raise HTTPException(422, {"status": "incompatible", "error": err})

        db.apply_switch(
            db_path,
            switch_id=req.switch_id,
            version=req.version,
            base_version=req.base_version,
            reader_schema=req.reader_schema,
            writer_schema=req.writer_schema,
        )
        result = {
            "status": "applied",
            "analyzer_id": analyzer_id,
            "switch_id": req.switch_id,
            "effective_version": req.version,
            "reader_version": req.version,
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
