"""Live smoke test against three real uvicorn processes.

Assumes services on 127.0.0.1:8000/8001/8002 with fresh databases.
Run: PYTHONPATH=/workspace/.pylibs python3 tests/smoke_live.py
"""
import copy
import sys

import httpx

WEB = "http://127.0.0.1:8000"
A = "http://127.0.0.1:8001"
B = "http://127.0.0.1:8002"

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    mark = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    print(f"[{mark}] {name}" + (f" :: {detail}" if detail else ""))


def main():
    web = httpx.Client(base_url=WEB, timeout=15)

    # 1. initial state
    st = web.get("/api/status").json()
    check("initial writer v1", st["effective_writer"]["version"] == 1)
    check("both analyzers reachable at reader v1",
          all(st["analyzers"][x]["reachable"]
              and st["analyzers"][x]["effective_version"] == 1
              for x in ("analyzer-a", "analyzer-b")))

    # 2. ingest v1 event + decode on both
    r = web.post("/api/events", json={
        "payload": {"sample_id": "S-1", "project": "P1", "measurement": 2.5},
        "decode_on": ["analyzer-a", "analyzer-b"]}).json()
    check("v1 event ingested", r["ok"] and r["writer_version"] == 1)
    check("both analyzers decode v1",
          all(d["decoded_ok"] for d in r["decodes"]))

    # 3. incompatible writer switch is blocked with field path
    bad = copy.deepcopy(st["effective_writer"]["schema"])
    bad["fields"][0] = {"name": "sample_id", "type": "int"}
    r = web.post("/api/switches/writer", json={"schema": bad})
    check("incompatible writer blocked 409", r.status_code == 409)
    check("error names field path",
          "sample_id" in r.json()["error"], r.json()["error"])
    check("writer stays v1 after block",
          web.get("/api/status").json()["effective_writer"]["version"] == 1)

    # 4. compatible writer v2 (default field + int array)
    v2 = copy.deepcopy(st["effective_writer"]["schema"])
    v2["fields"].append({"name": "instrument_id", "type": "string",
                         "default": "UNKNOWN"})
    v2["fields"].append({"name": "tags",
                         "type": {"type": "array", "items": "int"},
                         "default": []})
    r = web.post("/api/switches/writer", json={"schema": v2}).json()
    check("writer v2 confirmed", r["switch"]["status"] == "confirmed")
    st = web.get("/api/status").json()
    check("effective writer now v2",
          st["effective_writer"]["version"] == 2)

    # 5. v2 event decoded by v1 readers (projection + defaults)
    r = web.post("/api/events", json={
        "payload": {"sample_id": "S-2", "project": "P2", "measurement": 4.0,
                    "instrument_id": "GC-7", "tags": [1, 2]},
        "decode_on": ["analyzer-a", "analyzer-b"]}).json()
    check("v2 event ingested", r["ok"] and r["writer_version"] == 2)
    check("v1 readers project v2 event",
          all(d["decoded_ok"] and "instrument_id" not in d["datum"]
              for d in r["decodes"]))

    # 6. bad reader switch (required new field) blocked with paths
    bad_reader = copy.deepcopy(st["effective_writer"]["schema"])
    bad_reader["fields"] = [f for f in bad_reader["fields"]
                            if f["name"] in ("sample_id", "project", "measurement")]
    bad_reader["fields"].append({"name": "must_have", "type": "string"})
    r = web.post("/api/switches/reader",
                 json={"analyzer_id": "analyzer-a", "schema": bad_reader})
    check("bad reader blocked", r.status_code == 409
          and "must_have" in r.json()["error"], r.json().get("error"))

    # 7. good reader switch on analyzer-a
    good_reader = copy.deepcopy(st["effective_writer"]["schema"])
    good_reader["fields"] = [f for f in good_reader["fields"]
                             if f["name"] in ("sample_id", "project", "measurement")]
    good_reader["fields"].append(
        {"name": "temperature", "type": "double", "default": 0.0})
    r = web.post("/api/switches/reader",
                 json={"analyzer_id": "analyzer-a", "schema": good_reader}).json()
    check("analyzer-a reader v2 confirmed",
          r["switch"]["status"] == "confirmed")
    st = web.get("/api/status").json()
    check("analyzer-a at v2, analyzer-b still v1",
          st["analyzers"]["analyzer-a"]["effective_version"] == 2
          and st["analyzers"]["analyzer-b"]["effective_version"] == 1)

    # 8. analyzer-a decodes BOTH retained v1 and new v2 events
    events = st["events"]
    v1_id = next(e["id"] for e in events if e["writer_version"] == 1)
    r = web.post("/api/events/decode",
                 json={"event_id": v1_id, "analyzer_id": "analyzer-a"}).json()
    check("new reader decodes retained v1 event with default",
          r["result"]["decoded_ok"]
          and r["result"]["datum"]["temperature"] == 0.0)

    # 9. simulated lost receipt on reader switch (analyzer-b side, one-shot)
    before = httpx.get(B + "/reader/state").json()["effective_version"]
    httpx.post(B + "/simulate", json={
        "delay_ms": 0, "drop_switch_response": True,
        "drop_decode_response": False})
    r = web.post("/api/switches/reader",
                 json={"analyzer_id": "analyzer-b", "schema": good_reader})
    check("lost receipt -> pending, no false success",
          r.json()["switch"]["status"] == "pending")
    # remote mutation was durable before the drop
    rb = httpx.get(B + "/reader/state").json()
    check("analyzer-b durable state advanced despite lost receipt",
          rb["effective_version"] >= before + 1,
          f"before={before} after={rb['effective_version']}")
    # reconcile confirms from observed remote state. The platform's own
    # background reconciler (5s) may already have done it, so accept either
    # an explicit reconcile outcome or an already-confirmed switch.
    rec = web.post("/api/reconcile").json()["result"]
    st2 = web.get("/api/status").json()
    confirmed_now = all(
        x["status"] == "confirmed" for x in st2["reader_switches"]
        if x["analyzer_id"] == "analyzer-b")
    check("lost reader switch ends confirmed (manual or background reconcile)",
          confirmed_now or
          any(x["outcome"] == "confirmed_from_remote_state"
              for x in rec["reader"]))

    # 10. simulated lost receipt on writer switch (webapp side)
    web.post("/api/simulate", json={
        "delay_ms": 0, "drop_writer_switch": True,
        "drop_reader_switch": False})
    v3 = copy.deepcopy(v2)
    v3["fields"].append({"name": "batch_no", "type": "string",
                         "default": "B-0"})
    r = web.post("/api/switches/writer", json={"schema": v3})
    check("writer switch stays pending after lost receipt",
          r.json()["switch"]["status"] == "pending")
    # interleaved compatible event still lands using confirmed v2 writer
    ev = web.post("/api/events", json={
        "payload": {"sample_id": "S-3", "project": "P3", "measurement": 7.0,
                    "instrument_id": "GC-9", "tags": []}}).json()
    check("interleaved event ingested at confirmed v2",
          ev["ok"] and ev["writer_version"] == 2)
    rec = web.post("/api/reconcile").json()["result"]
    check("reconcile drives writer switch to confirmed",
          all(x["outcome"] in ("confirmed_from_observed_state", "confirmed")
              for x in rec["writer"]))
    st = web.get("/api/status").json()
    check("effective writer reaches v3 after reconcile",
          st["effective_writer"]["version"] == 3)
    check("interleaved v2 event retained and decodable",
          any(e["id"] == ev["event_id"] and e["writer_version"] == 2
              for e in st["events"]))

    # 11. no pending switches remain
    check("no pending writer switches",
          all(s["status"] != "pending" for s in st["writer_switches"]))
    check("no pending reader switches",
          all(s["status"] != "pending" for s in st["reader_switches"]))

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
