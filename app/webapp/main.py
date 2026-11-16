"""FastAPI web platform for Avro instrument-event format upgrades."""
from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ..avro_codec import DEFAULT_SCHEMA_V1
from . import db
from .analyzer_client import ANALYZER_ORDER, build_clients
from .services import GuardError, SwitchService

DB_PATH = os.environ.get("WEBAPP_DB", "/data/webapp.db")
WEB_DIR = Path(__file__).resolve().parent.parent.parent / "web"

RECONCILE_INTERVAL = float(os.environ.get("RECONCILE_INTERVAL", "5"))

service: Optional[SwitchService] = None
_bg_stop = threading.Event()


def _db_path() -> str:
    # Read at call time so tests can point WEBAPP_DB at a temp database after
    # import; production behaviour is identical.
    return os.environ.get("WEBAPP_DB", DB_PATH)


def _analyzer_urls() -> Dict[str, str]:
    return {
        "analyzer-a": os.environ.get("ANALYZER_A_URL",
                                     "http://analyzer-a:8001"),
        "analyzer-b": os.environ.get("ANALYZER_B_URL",
                                     "http://analyzer-b:8002"),
    }


def _bg_reconcile() -> None:
    """Periodically verify pending switches so a lost receipt after a crash is
    eventually resolved even without a manual click."""
    while not _bg_stop.wait(RECONCILE_INTERVAL):
        try:
            service.reconcile_all()
        except Exception as exc:  # never let the loop die
            print(f"[reconcile] background pass failed: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global service
    if service is None:  # tests may inject an in-process SwitchService
        db.init(_db_path())
        clients = build_clients(_analyzer_urls())
        service = SwitchService(_db_path(), clients)
    else:
        db.init(_db_path())
    # Startup reconciliation: verify every durable pending switch and resume.
    try:
        service.reconcile_all()
    except Exception as exc:
        print(f"[startup] initial reconciliation failed: {exc}", flush=True)
    t = threading.Thread(target=_bg_reconcile, name="reconciler", daemon=True)
    t.start()
    yield
    _bg_stop.set()


app = FastAPI(title="Avro 仪器事件格式升级平台", lifespan=lifespan)


# --- request models ---------------------------------------------------------

class EventIn(BaseModel):
    sample_id: Any = Field(None, description="样品号（也可用 payload 自由提交）")
    project: Any = None
    measurement: Any = Field(None, alias="measurement_value")
    payload: Optional[Dict[str, Any]] = Field(
        None, description="完整 Avro record JSON；提供时优先于平铺字段")
    decode_on: Optional[List[str]] = Field(
        None, description="可选：提交后让指定分析服务实际解码")

    model_config = {"populate_by_name": True}


class WriterSwitchIn(BaseModel):
    schema_: Dict[str, Any] = Field(..., alias="schema")

    model_config = {"populate_by_name": True}


class ReaderSwitchIn(BaseModel):
    analyzer_id: str
    schema_: Dict[str, Any] = Field(..., alias="schema")

    model_config = {"populate_by_name": True}


class SimIn(BaseModel):
    delay_ms: int = 0
    drop_writer_switch: bool = False
    drop_reader_switch: bool = False
    analyzer_id: Optional[str] = None
    drop_analyzer_switch: bool = False
    drop_analyzer_decode: bool = False


class DecodeIn(BaseModel):
    event_id: str
    analyzer_id: str


# --- helpers ----------------------------------------------------------------

def _guard_response(exc: GuardError, status: int = 409) -> JSONResponse:
    return JSONResponse(status_code=status,
                        content={"ok": False, "error": str(exc)})


# --- UI ---------------------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(WEB_DIR / "index.html")


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


# --- status -----------------------------------------------------------------

@app.get("/api/status")
def get_status():
    return service.status()


@app.get("/api/default-schema")
def default_schema():
    return DEFAULT_SCHEMA_V1


# --- events -----------------------------------------------------------------

@app.post("/api/events")
def ingest_event(event: EventIn):
    payload = event.payload
    if payload is None:
        payload = {}
        if event.sample_id is not None:
            payload["sample_id"] = event.sample_id
        if event.project is not None:
            payload["project"] = event.project
        if event.measurement is not None:
            payload["measurement"] = event.measurement
    if not payload:
        raise HTTPException(400, "payload 为空：请提交 Avro record JSON "
                                "（至少包含样品号、项目、测值字段）")
    try:
        result = service.ingest_event(payload)
    except GuardError as exc:
        return _guard_response(exc, 422)

    decodes = []
    for analyzer_id in (event.decode_on or []):
        if analyzer_id not in _analyzer_urls():
            decodes.append({"analyzer_id": analyzer_id,
                            "error": "unknown analyzer"})
            continue
        try:
            decodes.append(service.decode_event(result["event_id"], analyzer_id))
        except GuardError as exc:
            decodes.append({"analyzer_id": analyzer_id, "error": str(exc)})
        except Exception as exc:
            decodes.append({"analyzer_id": analyzer_id,
                            "error": f"decode call unconfirmed: {exc}"})
    result["decodes"] = decodes
    return {"ok": True, **result}


@app.post("/api/events/decode")
def decode_existing(req: DecodeIn):
    try:
        return {"ok": True, "result": service.decode_event(
            req.event_id, req.analyzer_id)}
    except GuardError as exc:
        return _guard_response(exc, 409)
    except Exception as exc:
        # A lost/unconfirmed analyzer response must surface as a structured
        # 502, never an uncaught 500.
        return JSONResponse(
            status_code=502,
            content={"ok": False,
                     "error": f"decode call unconfirmed: {exc}"})


# --- switches ---------------------------------------------------------------

@app.post("/api/switches/writer")
def switch_writer(req: WriterSwitchIn):
    try:
        view = service.create_writer_switch(req.schema_)
    except GuardError as exc:
        return _guard_response(exc)
    return {"ok": True, "switch": view}


@app.post("/api/switches/reader")
def switch_reader(req: ReaderSwitchIn):
    try:
        view = service.create_reader_switch(req.analyzer_id, req.schema_)
    except GuardError as exc:
        return _guard_response(exc)
    return {"ok": True, "switch": view}


@app.post("/api/reconcile")
def reconcile():
    return {"ok": True, "result": service.reconcile_all()}


# --- simulation -------------------------------------------------------------

@app.post("/api/simulate")
def simulate(req: SimIn):
    from ..db_common import get_conn
    out = {}
    conn = get_conn(_db_path())
    try:
        with conn:
            db.set_sim(conn, req.delay_ms, req.drop_writer_switch,
                       req.drop_reader_switch)
        out["webapp"] = db.get_sim(conn)
    finally:
        conn.close()

    target_ids = [req.analyzer_id] if req.analyzer_id else ANALYZER_ORDER
    for analyzer_id in target_ids:
        if analyzer_id not in service.clients:
            out[analyzer_id] = {"error": "unknown analyzer"}
            continue
        try:
            out[analyzer_id] = service.clients[analyzer_id].simulate(
                delay_ms=req.delay_ms,
                drop_switch_response=req.drop_analyzer_switch,
                drop_decode_response=req.drop_analyzer_decode,
            )
        except Exception as exc:
            out[analyzer_id] = {"error": str(exc)}
    return {"ok": True, "simulation": out}
