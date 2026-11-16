"""Regression tests for the upgrade / ingestion hand-off.

Each test pins one of the failure modes that used to let an accepted event
become undecodable:

1. a write-format switch left ``pending`` after a lost receipt while a reader
   upgrade is then confirmed against a format the *pending* writer will
   produce (int unit vs. string unit);
2. a retained event written before the writer dropped a field, then a reader
   whose field type is a defaulted string (the event is not in the warehouse
   yet: it is still held by an in-flight ingestion lease);
3. conflict upgrades must not take effect, and a write format must never be
   released before confirmation;
4. lost receipt / restart / late execution verify the real effective state and
   an old switch never overwrites a newer one;
5. ingestion and status queries still complete while verification is slow.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_upgrade_handoff -v
"""
import copy
import threading
import time
import unittest

from app.webapp import db as web_db
from app.webapp import main as web_main
from app.webapp.services import SwitchService
from tests.test_integration import ASGIAnalyzerClient, PlatformHarness, V1


def unit_int_writer():
    s = copy.deepcopy(V1)
    s["fields"].append({"name": "unit", "type": "int"})
    return s


def unit_string_reader():
    s = copy.deepcopy(V1)
    s["fields"].append({"name": "unit", "type": "string", "default": ""})
    return s


def writer_without_measurement():
    s = copy.deepcopy(V1)
    s["fields"] = [f for f in s["fields"] if f["name"] != "measurement"]
    return s


def reader_measurement_string_with_default():
    s = copy.deepcopy(V1)
    s["fields"][2] = {"name": "measurement", "type": "string", "default": ""}
    return s


def reader_measurement_default():
    s = copy.deepcopy(V1)
    s["fields"][2] = {"name": "measurement", "type": "double", "default": 0.0}
    return s


class TestUpgradeHandoff(unittest.TestCase):
    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self.web.__enter__()

    def tearDown(self):
        self.web.__exit__(None, None, None)
        web_main._bg_stop.set()

    # -- helpers ----------------------------------------------------------
    def arm_writer_drop(self):
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)
        conn.commit()
        conn.close()

    def status(self):
        return self.web.get("/api/status").json()

    def analyzer_version(self, analyzer_id):
        return self.h.service.clients[analyzer_id].reader_state()

    def ingest(self, payload, decode_on=None):
        body = {"payload": payload}
        if decode_on:
            body["decode_on"] = decode_on
        return self.web.post("/api/events", json=body).json()

    # -- 1. pending writer must constrain reader upgrades ----------------
    def test_pending_writer_format_constrains_reader_upgrade(self):
        self.arm_writer_drop()
        r = self.web.post("/api/switches/writer",
                          json={"schema": unit_int_writer()})
        self.assertEqual(r.json()["switch"]["status"], "pending")
        # The write format was *not* released before confirmation.
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 1)

        # A reader that maps `unit` to a defaulted string would make the
        # pending writer unreadable: it must be rejected, not confirmed.
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": unit_string_reader()})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertIn("pending writer switch", r.json()["error"])
        self.assertIn("unit", r.json()["error"])
        self.assertEqual(self.analyzer_version("analyzer-a")["effective_version"], 1)

        # Reconcile releases the pending writer; now the *same* reader upgrade
        # is still rejected because it cannot read the effective v2 writer.
        rec = self.web.post("/api/reconcile").json()["result"]["writer"]
        self.assertTrue(rec)
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 2)
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": unit_string_reader()})
        self.assertEqual(r.status_code, 409, r.text)

        # Both analyzers decode the accepted event; A stays on its old reader.
        ev = self.ingest({"sample_id": "S", "project": "P",
                          "measurement": 1.0, "unit": 7},
                         decode_on=["analyzer-a", "analyzer-b"])
        self.assertTrue(ev["ok"])
        self.assertEqual(ev["writer_version"], 2)
        for d in ev["decodes"]:
            self.assertTrue(d["decoded_ok"], d)

    # -- 2. landed old-format event blocks a defaulted string reader ----
    def test_retained_old_format_event_blocks_defaulted_string_reader(self):
        # Step 1 from the report: the reader measurement field already has a
        # default on both services, so the writer may later drop it.
        for analyzer_id in ("analyzer-a", "analyzer-b"):
            r = self.web.post("/api/switches/reader", json={
                "analyzer_id": analyzer_id,
                "schema": reader_measurement_default()})
            self.assertEqual(r.json()["switch"]["status"], "confirmed", r.text)

        # An old-format (v1) event is retained before the writer drops the
        # measurement field.
        old = self.ingest({"sample_id": "S-old", "project": "P",
                           "measurement": 1.5})
        self.assertEqual(old["writer_version"], 1)

        # Writer v3 drops `measurement`: safe for the current readers because
        # they now supply a default.
        r = self.web.post("/api/switches/writer",
                          json={"schema": writer_without_measurement()})
        self.assertEqual(r.json()["switch"]["status"], "confirmed", r.text)

        # Reader wants measurement as a defaulted string: statically "fine"
        # against the current writer, but it cannot decode the retained v1
        # event whose measurement is a double.  Must be blocked.
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a",
            "schema": reader_measurement_string_with_default()})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertIn("retained event", r.json()["error"])
        self.assertEqual(self.analyzer_version("analyzer-a")["effective_version"], 2)
        # The old event is still decodable by both accepted readers.
        res = self.web.post("/api/events/decode", json={
            "event_id": old["event_id"], "analyzer_id": "analyzer-a"}).json()
        self.assertTrue(res["result"]["decoded_ok"], res)

    # -- 3. in-flight (not yet landed) event is covered by the lease -----
    def test_in_flight_ingest_format_is_guarded(self):
        for analyzer_id in ("analyzer-a", "analyzer-b"):
            self.web.post("/api/switches/reader", json={
                "analyzer_id": analyzer_id,
                "schema": reader_measurement_default()})
        # The old-format event is NOT in the warehouse yet: it has only taken
        # a lease on the v1 writer while it encodes.
        conn = self.h.service._conn()
        from app.avro_codec import fingerprint
        lease_id = web_db.begin_ingest_lease(
            conn, writer_fp=fingerprint(V1), writer_version=1)
        conn.commit()
        conn.close()
        try:
            st = self.status()
            self.assertEqual([l["lease_id"] for l in st["in_flight_ingests"]],
                             [lease_id])
            r = self.web.post("/api/switches/writer",
                              json={"schema": writer_without_measurement()})
            self.assertEqual(r.json()["switch"]["status"], "confirmed", r.text)
            st = self.status()
            self.assertEqual(st["effective_writer"]["version"], 2)

            # Without the lease, the defaulted-string reader would pass every
            # static check against the current (field-less) writer; the lease
            # makes the in-flight old format visible and blocks it.
            r = self.web.post("/api/switches/reader", json={
                "analyzer_id": "analyzer-a",
                "schema": reader_measurement_string_with_default()})
            self.assertEqual(r.status_code, 409, r.text)
            self.assertIn("in-flight ingest", r.json()["error"])
            self.assertEqual(self.analyzer_version("analyzer-a")["effective_version"], 2)
        finally:
            conn = self.h.service._conn()
            web_db.finish_ingest_lease(conn, lease_id)
            conn.commit()
            conn.close()

        # With the lease gone (and no retained old event) the same upgrade is
        # accepted, and an in-flight event would have been the only hazard.
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a",
            "schema": reader_measurement_string_with_default()})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["switch"]["status"], "confirmed")

    # -- 4. conflicting release is rejected and rolled back --------------
    def test_conflicting_upgrade_does_not_take_effect(self):
        self.arm_writer_drop()
        self.web.post("/api/switches/writer", json={"schema": unit_int_writer()})
        self.assertEqual(self.status()["effective_writer"]["version"], 1)
        # A pending reader format that would conflict is impossible to create
        # now (guard covers pending writers), so inject one directly to model
        # a pre-existing pending reader switch racing with the writer.
        conn = self.h.service._conn()
        version = web_db.next_version(conn, "reader")
        web_db.register_schema(conn, "reader", version, unit_string_reader())
        sid = web_db.insert_reader_switch(
            conn, analyzer_id="analyzer-a", version=version, base_version=1,
            schema=unit_string_reader(), writer_fp=V1 and "x")
        conn.commit()
        conn.close()

        # Reconciliation of the writer must refuse to release a conflicting
        # format; it stays pending (no effect), with a field-path explanation.
        rec = self.web.post("/api/reconcile").json()["result"]["writer"]
        self.assertEqual(rec[0]["outcome"], "rejected")
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 1)
        wsw = [s for s in st["writer_switches"] if s["switch_id"] == rec[0]["switch_id"]]
        self.assertEqual(wsw[0]["status"], "rejected")
        self.assertIn("unit", wsw[0]["last_error"])
        # The conflicting writer upgrade did not land: the pointer and the
        # accepted events stay decodable by the analyzer's real reader.
        ev = self.ingest({"sample_id": "S", "project": "P", "measurement": 2.0},
                         decode_on=["analyzer-a"])
        self.assertEqual(ev["writer_version"], 1)
        self.assertTrue(ev["decodes"][0]["decoded_ok"], ev["decodes"][0])

    # -- 5. lost receipt + restart verifies real state -------------------
    def test_restart_reconciles_lost_writer_receipt(self):
        self.arm_writer_drop()
        self.web.post("/api/switches/writer", json={"schema": unit_int_writer()})
        self.assertEqual(self.status()["effective_writer"]["version"], 1)
        self.assertEqual(len(self.status()["writer_switches"][0]["last_error"] or ""),
                         len(self.status()["writer_switches"][0]["last_error"]))

        # Restart: new service instance on the same databases + endpoints.
        restarted = SwitchService(
            self.h.web_db_path,
            {"analyzer-a": ASGIAnalyzerClient(self.h.app_a),
             "analyzer-b": ASGIAnalyzerClient(self.h.app_b)})
        rec = restarted.reconcile_all()
        self.assertTrue(rec["writer"])
        # The simulated receipt was consumed by the first attempt: the restart
        # verifies real state and completes the release exactly once.
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 2)
        self.assertTrue(all(s["status"] != "pending"
                            for s in st["writer_switches"]))

    # -- 6. late reader switch cannot roll back a newer reader -----------
    def test_late_reader_switch_never_overwrites_newer(self):
        newer = copy.deepcopy(V1)
        newer["fields"].append({"name": "temperature", "type": "double",
                                "default": 0.0})
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": newer})
        self.assertEqual(r.json()["switch"]["status"], "confirmed")
        state = self.analyzer_version("analyzer-a")
        self.assertEqual(state["effective_version"], 2)

        # A late old switch that only asks for a field projection (v3 would
        # not be late, so replay the *same* version with a different switch id
        # and a stale base).
        late = self.h.service.clients["analyzer-a"].reader_switch(
            switch_id="late-old", version=2, base_version=1,
            reader_schema=unit_string_reader(), writer_schema=V1)
        self.assertEqual(late["status"], "stale")
        state = self.analyzer_version("analyzer-a")
        self.assertEqual(state["effective_version"], 2)
        self.assertEqual(state["reader_fp"],
                         __import__("app.avro_codec", fromlist=["fingerprint"])
                         .fingerprint(newer))

    # -- 7. slow verification does not block ingestion / status ----------
    def test_slow_verification_keeps_ingest_and_status_alive(self):
        # Arm a one-shot writer receipt drop so a pending switch exists, then
        # make every analyzer call slow (2s).
        self.arm_writer_drop()
        self.web.post("/api/switches/writer", json={"schema": unit_int_writer()})
        for analyzer_id in ("analyzer-a", "analyzer-b"):
            self.h.service.clients[analyzer_id].simulate(
                delay_ms=2000, drop_switch_response=False,
                drop_decode_response=False)

        done = {}

        def reconcile():
            done["result"] = self.web.post("/api/reconcile").json()["result"]

        t = threading.Thread(target=reconcile)
        start = time.time()
        t.start()
        time.sleep(0.4)  # verification is now in flight
        ev = self.ingest({"sample_id": "S-mid", "project": "P",
                          "measurement": 3.0},
                         decode_on=["analyzer-a"])
        ingest_elapsed = time.time() - start
        self.assertTrue(ev["ok"], ev)
        # Decoding itself is delayed by the simulation, but ingestion and
        # status must not wait for the whole reconcile pass (4x2s).
        self.assertLess(ingest_elapsed, 6.0)
        st = self.status()
        self.assertTrue(st["analyzers"]["analyzer-a"]["reachable"])
        self.assertLess(time.time() - start, 6.0)
        t.join(timeout=30)
        self.assertIn("writer", done["result"])
        self.assertEqual(self.status()["effective_writer"]["version"], 2)
        # reset delay for other tests
        for analyzer_id in ("analyzer-a", "analyzer-b"):
            self.h.service.clients[analyzer_id].simulate(
                delay_ms=0, drop_switch_response=False,
                drop_decode_response=False)

    # -- 8. released-but-unverified still accepts compatible events ----
    def test_released_unverified_switch_does_not_block_ingest(self):
        # A released format whose analyzer verification is still in flight is
        # already guard-validated; compatible events must keep landing, and a
        # status query must keep answering.
        conn = self.h.service._conn()
        version = web_db.next_version(conn, "writer")
        w2 = copy.deepcopy(V1)
        w2["fields"].append({"name": "batch_no", "type": "string",
                             "default": "B"})
        fp = web_db.register_schema(conn, "writer", version, w2)
        sid = web_db.insert_writer_switch(
            conn, version=version, base_version=1, new_fp=fp,
            schema=w2, target="local")
        conn.commit()
        conn.close()

        # Unreleased pending switch: ingestion proceeds at v1.
        ev = self.ingest({"sample_id": "S", "project": "P", "measurement": 1.0})
        self.assertEqual(ev["writer_version"], 1)

        # Released, not yet verified (pointer moved, row still pending).
        conn = self.h.service._conn()
        web_db.set_writer_state(conn, fp, version, sid)
        web_db.update_writer_switch(conn, sid, "pending",
                                    "post-release verification unconfirmed")
        conn.commit()
        conn.close()
        ev = self.ingest({"sample_id": "S2", "project": "P",
                          "measurement": 2.0, "batch_no": "B-1"},
                         decode_on=["analyzer-a", "analyzer-b"])
        self.assertEqual(ev["writer_version"], 2)
        for d in ev["decodes"]:
            self.assertTrue(d["decoded_ok"], d)
        self.assertTrue(self.status()["analyzers"]["analyzer-b"]["reachable"])

        # Reconcile verifies and confirms the release.
        self.web.post("/api/reconcile")
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 2)
        self.assertTrue(all(s["status"] == "confirmed"
                            for s in st["writer_switches"]))

    # -- 9. an armed fault is not swallowed by a rejected attempt --------
    def test_armed_drop_applies_to_the_next_actual_switch(self):
        self.arm_writer_drop()
        # A rejected proposal must not consume the one-shot fault.
        bad = copy.deepcopy(V1)
        bad["fields"][0] = {"name": "sample_id", "type": "int"}
        r = self.web.post("/api/switches/writer", json={"schema": bad})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(self.status()["effective_writer"]["version"], 1)
        # The next valid switch is the one that loses its receipt.
        r = self.web.post("/api/switches/writer",
                          json={"schema": unit_int_writer()})
        self.assertEqual(r.json()["switch"]["status"], "pending")
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 1)
        self.web.post("/api/reconcile")
        st = self.status()
        self.assertEqual(st["effective_writer"]["version"], 2)
        self.assertTrue(all(s["status"] == "confirmed"
                            for s in st["writer_switches"]))

    # -- 10. duplicate switch id with a different payload is a conflict --
    def test_replayed_switch_id_with_different_schema_conflicts(self):
        client = self.h.service.clients["analyzer-b"]
        first = client.reader_switch(
            switch_id="same-id", version=2, base_version=1,
            reader_schema=unit_string_reader(), writer_schema=V1)
        self.assertEqual(first["status"], "applied")
        conflict = client.reader_switch(
            switch_id="same-id", version=2, base_version=1,
            reader_schema=reader_measurement_string_with_default(),
            writer_schema=V1)
        self.assertEqual(conflict["status"], "conflict")
        state = self.analyzer_version("analyzer-b")
        self.assertEqual(state["effective_version"], 2)
        self.assertEqual(state["reader_schema"], unit_string_reader())


if __name__ == "__main__":
    unittest.main(verbosity=2)
