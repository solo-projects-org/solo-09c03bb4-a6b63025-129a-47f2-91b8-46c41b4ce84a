"""End-to-end tests with real FastAPI apps (in-process ASGI transport) and
separate SQLite databases - no network sockets or Docker required.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_integration -v
"""
import copy
import os
import tempfile
import unittest
from unittest import mock

import httpx
from fastapi.testclient import TestClient

from app.analyzer.app import create_app
from app.analyzer import db as analyzer_db
from app.webapp import db as web_db
from app.webapp import main as web_main
from app.webapp.analyzer_client import AnalyzerClient, RemoteError


V1 = {
    "type": "record", "name": "InstrumentEvent", "namespace": "lab.warehouse",
    "fields": [
        {"name": "sample_id", "type": "string"},
        {"name": "project", "type": "string"},
        {"name": "measurement", "type": "double"},
    ],
}


def writer_v2():
    s = copy.deepcopy(V1)
    # added optional field with default: old readers stay compatible; numeric
    # promotion int->long inside an array exercises arrays + promotion.
    s["fields"].append({"name": "instrument_id", "type": "string",
                        "default": "UNKNOWN"})
    s["fields"].append({"name": "tags",
                        "type": {"type": "array", "items": "int"},
                        "default": []})
    return s


def bad_writer_v2():
    s = copy.deepcopy(V1)
    # string -> int has no resolution path: must be blocked
    s["fields"][0] = {"name": "sample_id", "type": "int"}
    return s


def reader_v2_with_default():
    s = copy.deepcopy(V1)
    s["fields"].append({"name": "temperature", "type": "double",
                        "default": 0.0})
    s["fields"].append({"name": "quality_flag",
                        "type": ["null", "string"], "default": None})
    return s


def bad_reader_drops_required():
    s = copy.deepcopy(V1)
    # reader no longer contains 'measurement' but retained v1 events have it;
    # field removal on the reader side against old writers is fine in Avro,
    # so instead add a required field with no default to force failure.
    s["fields"].append({"name": "new_required", "type": "string"})
    return s


class _TestClientTransport:
    """Transport adapter exposing the synchronous httpx transport surface on
    top of Starlette's TestClient portal (httpx 0.28's ASGITransport is
    async-only)."""

    def __init__(self, asgi_app):
        self._tc = TestClient(asgi_app)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        tc = self._tc
        method = request.method
        url = str(request.url).split(request.url.scheme + "://"
                                     + request.url.host)[-1]
        if not url:
            url = "/"
        content = request.content if request.content else None
        headers = dict(request.headers)
        r = tc.request(method, url, content=content, headers=headers)
        return httpx.Response(r.status_code, headers=dict(r.headers),
                              content=r.content, request=request)


class ASGIAnalyzerClient(AnalyzerClient):
    """HTTP client talking to an ASGI app over an in-process transport."""

    def __init__(self, asgi_app):
        super().__init__("http://asgi-local")
        self._transport = _TestClientTransport(asgi_app)

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, transport=self._transport,
                            timeout=10)


class PlatformHarness:
    """Spin up webapp + two analyzers with isolated temp databases."""

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="avroplat-")
        self.web_db_path = os.path.join(self.tmp, "webapp.db")
        self.a_db = os.path.join(self.tmp, "a.db")
        self.b_db = os.path.join(self.tmp, "b.db")

        # Two fully independent analyzer apps, each its own SQLite database.
        analyzer_db.init(self.a_db)
        analyzer_db.init(self.b_db)
        self.app_a = create_app("analyzer-a", self.a_db)
        self.app_b = create_app("analyzer-b", self.b_db)

        # Webapp
        os.environ["WEBAPP_DB"] = self.web_db_path
        web_db.init(self.web_db_path)
        self.service = web_main.SwitchService(
            self.web_db_path,
            {"analyzer-a": ASGIAnalyzerClient(self.app_a),
             "analyzer-b": ASGIAnalyzerClient(self.app_b)},
        )
        web_main.service = self.service
        self.web = TestClient(web_main.app)


class TestFullFlow(unittest.TestCase):
    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        # Keep the background reconciler quiet; tests drive reconcile manually.
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self._ctx = self.web
        self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)
        web_main._bg_stop.set()

    # ------------------------------------------------------------------
    def test_01_initial_state_and_basic_ingest_decode(self):
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)
        for a in ("analyzer-a", "analyzer-b"):
            self.assertTrue(st["analyzers"][a]["reachable"])
            self.assertEqual(st["analyzers"][a]["effective_version"], 1)

        r = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-1", "project": "P1", "measurement": 2.5},
            "decode_on": ["analyzer-a", "analyzer-b"],
        }).json()
        self.assertTrue(r["ok"])
        self.assertEqual(r["writer_version"], 1)
        self.assertEqual(len(r["decodes"]), 2)
        for d in r["decodes"]:
            self.assertTrue(d["decoded_ok"], d)
            self.assertEqual(d["datum"]["sample_id"], "S-1")

    def test_02_incompatible_writer_switch_blocked_with_field_path(self):
        r = self.web.post("/api/switches/writer",
                          json={"schema": bad_writer_v2()})
        self.assertEqual(r.status_code, 409)
        self.assertIn("sample_id", r.json()["error"])
        # writer must remain v1
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)

    def test_03_compatible_writer_switch_confirmed(self):
        r = self.web.post("/api/switches/writer",
                          json={"schema": writer_v2()})
        self.assertEqual(r.status_code, 200, r.text)
        sw = r.json()["switch"]
        self.assertEqual(sw["status"], "confirmed")
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 2)

        # New event is encoded with v2; old v1 readers still decode via
        # schema resolution with the defaulted new field.
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-2", "project": "P2", "measurement": 4.0,
                        "instrument_id": "GC-7", "tags": [1, 2, 3]},
            "decode_on": ["analyzer-a"],
        }).json()
        self.assertTrue(r["ok"])
        d = r["decodes"][0]
        self.assertTrue(d["decoded_ok"], d)
        # v1 reader projects away unknown fields and keeps known ones
        self.assertNotIn("instrument_id", d["datum"])
        self.assertEqual(d["datum"]["project"], "P2")

    def test_04_reader_switch_guard_covers_all_retained_events(self):
        # make one v1 event first
        self.web.post("/api/events", json={
            "payload": {"sample_id": "S-old", "project": "P",
                        "measurement": 1.0}})
        # advance writer to v2
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        # and one v2 event
        self.web.post("/api/events", json={
            "payload": {"sample_id": "S-new", "project": "P", "measurement": 2.0,
                        "instrument_id": "X", "tags": []}})

        # bad reader: required new field -> cannot read retained old writers
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": bad_reader_drops_required()})
        self.assertEqual(r.status_code, 409)
        err = r.json()["error"]
        self.assertIn("new_required", err)
        self.assertIn("current writer", err)
        # analyzer stays at v1
        st = self.web.get("/api/status").json()
        self.assertEqual(st["analyzers"]["analyzer-a"]["effective_version"], 1)

        # good reader with defaults reads both v1 and v2 retained events
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a",
            "schema": reader_v2_with_default()})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["switch"]["status"], "confirmed")
        st = self.web.get("/api/status").json()
        self.assertEqual(st["analyzers"]["analyzer-a"]["effective_version"], 2)

        # It can actually decode both retained events
        events = st["events"]
        v1_event = next(e for e in events if e["writer_version"] == 1)
        r = self.web.post("/api/events/decode", json={
            "event_id": v1_event["id"],
            "analyzer_id": "analyzer-a"}).json()
        self.assertTrue(r["result"]["decoded_ok"])
        self.assertEqual(r["result"]["datum"]["temperature"], 0.0)

    def test_05_reader_switch_idempotent_replay(self):
        client = self.h.service.clients["analyzer-b"]
        schema = reader_v2_with_default()
        first = client.reader_switch(
            switch_id="fixed-id-1", version=2, base_version=1,
            reader_schema=schema, writer_schema=V1)
        self.assertEqual(first["status"], "applied")
        second = client.reader_switch(
            switch_id="fixed-id-1", version=2, base_version=1,
            reader_schema=schema, writer_schema=V1)
        self.assertEqual(second["status"], "already_applied")

    def test_06_late_switch_fenced(self):
        client = self.h.service.clients["analyzer-b"]
        schema = reader_v2_with_default()
        # base v1 -> v2 succeeds
        client.reader_switch(switch_id="s1", version=2, base_version=1,
                             reader_schema=schema, writer_schema=V1)
        # a late duplicate based on the stale v1 base must be rejected
        late = client.reader_switch(switch_id="s2", version=99, base_version=1,
                                    reader_schema=schema, writer_schema=V1)
        self.assertEqual(late["status"], "stale")
        # and must not have changed effective state
        state = client.reader_state()
        self.assertEqual(state["effective_version"], 2)

    def test_07_lost_reader_receipt_then_reconcile_confirms(self):
        # Arm a one-shot dropped response inside analyzer A.
        a_db_path = self.h.a_db
        analyzer_db.set_sim(a_db_path, 0, True, False)

        schema = reader_v2_with_default()
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": schema})
        # Platform must not have reported success.
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["switch"]["status"], "pending")

        # Crucially the durable remote mutation DID happen before the drop:
        # the analyzer's own state must already be at v2.
        state = self.h.service.clients["analyzer-a"].reader_state()
        self.assertEqual(state["effective_version"], 2)

        # Reconciliation observes real state and confirms.
        rec = self.web.post("/api/reconcile").json()["result"]["reader"]
        outcomes = [x["outcome"] for x in rec]
        self.assertIn("confirmed_from_remote_state", outcomes)
        st = self.web.get("/api/status").json()
        pending = [s for s in st["reader_switches"] if s["status"] == "pending"]
        self.assertEqual(pending, [])

    def test_08_lost_reader_receipt_without_remote_apply_retries(self):
        # Simulate at the *webapp* side: the request never reaches the
        # analyzer, so no durable mutation exists; reconcile must retry.
        client = self.h.service.clients["analyzer-b"]
        original = client.reader_switch
        calls = {"n": 0}

        def flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RemoteError("simulated connection reset")
            return original(*a, **kw)

        with mock.patch.object(client, "reader_switch", side_effect=flaky):
            r = self.web.post("/api/switches/reader", json={
                "analyzer_id": "analyzer-b",
                "schema": reader_v2_with_default()})
        self.assertEqual(r.json()["switch"]["status"], "pending")
        self.assertEqual(calls["n"], 1)

        # Reconcile retries the same idempotent switch successfully.
        rec = self.web.post("/api/reconcile").json()["result"]["reader"]
        self.assertTrue(any(x["outcome"] == "confirmed_on_retry" for x in rec))
        self.assertEqual(
            client.reader_state()["effective_version"], 2)

    def test_09_lost_writer_receipt_blocks_success_and_reconciles(self):
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)  # arm local writer receipt drop
        conn.commit()
        conn.close()

        r = self.web.post("/api/switches/writer",
                          json={"schema": writer_v2()})
        sw = r.json()["switch"]
        self.assertEqual(sw["status"], "pending")  # never reported confirmed

        # New events must still be ingestible with the *confirmed* v1 writer;
        # interleaving must not be blocked or lost.
        ev = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-while-pending", "project": "P",
                        "measurement": 9.0}}).json()
        self.assertTrue(ev["ok"])
        self.assertEqual(ev["writer_version"], 1)

        # Reconcile resumes the switch and confirms.
        rec = self.web.post("/api/reconcile").json()["result"]["writer"]
        self.assertTrue(rec)
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 2)
        self.assertTrue(all(s["status"] == "confirmed"
                            for s in st["writer_switches"]))
        # and the interleaved v1 event is still retained and decodable
        ids = {e["id"]: e for e in st["events"]}
        self.assertIn(ev["event_id"], ids)
        self.assertEqual(ids[ev["event_id"]]["writer_version"], 1)

    def test_10_pending_writer_switch_blocks_new_proposal(self):
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)
        conn.commit()
        conn.close()
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        r = self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        self.assertEqual(r.status_code, 409)
        self.assertIn("still unconfirmed", r.json()["error"])
        # clear and reconcile
        self.web.post("/api/reconcile")

    def test_11_rejected_switch_never_changes_format(self):
        self.web.post("/api/switches/writer",
                      json={"schema": bad_writer_v2()})
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)
        # no pending switch row should have been created for a guard failure
        self.assertEqual(st["writer_switches"], [])

    def test_12_encoding_error_surfaces(self):
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": 123, "project": "P", "measurement": 1.0}})
        self.assertEqual(r.status_code, 422)
        self.assertIn("Avro encoding failed", r.json()["error"])

    def test_13_numeric_promotion_and_array_round_trip(self):
        # Writer v2 carries int array + string default; promote reader view.
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        promoted = reader_v2_with_default()
        # reader asks for tags as long[] and instrument_id stays string
        promoted["fields"].append({
            "name": "tags",
            "type": {"type": "array", "items": "long"},
            "default": []})
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": promoted})
        self.assertEqual(r.status_code, 200, r.text)
        ev = self.web.post("/api/events", json={
            "payload": {"sample_id": "S", "project": "P", "measurement": 1.0,
                        "instrument_id": "GC", "tags": [1, 2]},
            "decode_on": ["analyzer-a"]}).json()
        d = ev["decodes"][0]
        self.assertTrue(d["decoded_ok"], d)
        self.assertEqual(d["datum"]["tags"], [1, 2])
        self.assertIsInstance(d["datum"]["tags"][0], int)


if __name__ == "__main__":
    unittest.main(verbosity=2)
