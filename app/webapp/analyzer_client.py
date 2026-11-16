"""HTTP client used by the webapp to talk to the two analyzer services."""
from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional

import httpx


class RemoteError(Exception):
    def __init__(self, message: str, status: Optional[int] = None,
                 payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class AnalyzerClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=self._timeout)

    def health(self) -> Dict[str, Any]:
        with self._client() as c:
            r = c.get("/health")
            r.raise_for_status()
            return r.json()

    def reader_state(self) -> Dict[str, Any]:
        with self._client() as c:
            r = c.get("/reader/state")
            r.raise_for_status()
            return r.json()

    def writer_check(self, writer_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Read-only compatibility proof against the analyzer's reader."""
        with self._client() as c:
            r = c.post("/writer/check", json={"writer_schema": writer_schema})
            if r.status_code >= 400:
                raise RemoteError(f"writer check failed: {r.text}", r.status_code,
                                  _safe_json(r))
            return r.json()

    def reader_switch(self, *, switch_id: str, version: int, base_version: int,
                      reader_schema: Dict[str, Any], writer_schema: Dict[str, Any]
                      ) -> Dict[str, Any]:
        body = {
            "switch_id": switch_id,
            "version": version,
            "base_version": base_version,
            "reader_schema": reader_schema,
            "writer_schema": writer_schema,
        }
        with self._client() as c:
            r = c.post("/reader/switch", json=body)
            if r.status_code >= 400:
                raise RemoteError(f"reader switch failed: {r.text}", r.status_code,
                                  _safe_json(r))
            return r.json()

    def decode(self, *, event_id: str, blob: bytes,
               writer_schema: Dict[str, Any]) -> Dict[str, Any]:
        body = {
            "event_id": event_id,
            "blob_b64": base64.b64encode(blob).decode("ascii"),
            "writer_schema": writer_schema,
        }
        with self._client() as c:
            r = c.post("/events/decode", json=body)
            if r.status_code >= 400:
                raise RemoteError(f"decode failed: {r.text}", r.status_code, _safe_json(r))
            return r.json()

    def simulate(self, *, delay_ms: int, drop_switch_response: bool,
                 drop_decode_response: bool) -> Dict[str, Any]:
        with self._client() as c:
            r = c.post("/simulate", json={
                "delay_ms": delay_ms,
                "drop_switch_response": drop_switch_response,
                "drop_decode_response": drop_decode_response,
            })
            r.raise_for_status()
            return r.json()

    def get_simulate(self) -> Dict[str, Any]:
        with self._client() as c:
            r = c.get("/simulate")
            r.raise_for_status()
            return r.json()


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return r.text


def build_clients(urls: Dict[str, str]) -> Dict[str, AnalyzerClient]:
    return {name: AnalyzerClient(url) for name, url in urls.items()}


ANALYZER_ORDER: List[str] = ["analyzer-a", "analyzer-b"]
