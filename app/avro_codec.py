"""Avro encoding/decoding helpers built on fastavro."""
from __future__ import annotations

import hashlib
import io
import json
from typing import Any, Dict, Optional, Tuple

import fastavro

from .avro_compat import explain


class SchemaError(ValueError):
    pass


class CodecError(ValueError):
    pass


def parse_schema(schema: Any) -> Any:
    """Validate an Avro schema and return its parsed fastavro form."""
    try:
        return fastavro.parse_schema(schema)
    except Exception as exc:  # fastavro raises a variety of exception types
        raise SchemaError(f"invalid Avro schema: {exc}") from exc


def canonical_json(schema: Any) -> str:
    return json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(schema: Any) -> str:
    """Stable identity hash for a schema (stored alongside every event)."""
    return hashlib.sha256(canonical_json(schema).encode("utf-8")).hexdigest()[:16]


def encode(schema: Any, datum: Dict[str, Any]) -> bytes:
    """Encode one record with the given *writer* schema."""
    parsed = parse_schema(schema)
    buf = io.BytesIO()
    try:
        fastavro.schemaless_writer(buf, parsed, datum)
    except Exception as exc:
        raise CodecError(f"Avro encoding failed: {exc}") from exc
    return buf.getvalue()


def decode(reader_schema: Any, blob: bytes, writer_schema: Optional[Any] = None
           ) -> Tuple[Dict[str, Any], Optional[str]]:
    """Decode ``blob`` with ``reader_schema``.

    When ``writer_schema`` is supplied it is used for schema resolution, which
    exercises real projection/promotion/default behaviour. Returns
    (decoded_datum, compatibility_error); on incompatibility the datum is None
    and the second element is a human-readable field-path explanation.
    """
    parsed_reader = parse_schema(reader_schema)
    if writer_schema is not None:
        err = explain(parsed_reader, parse_schema(writer_schema))
        if err:
            return None, err
        parsed_writer = parse_schema(writer_schema)
    else:
        parsed_writer = parsed_reader
    buf = io.BytesIO(blob)
    try:
        return fastavro.schemaless_reader(buf, parsed_writer, parsed_reader), None
    except Exception as exc:
        return None, f"Avro decoding failed at runtime: {exc}"


# ---------------------------------------------------------------------------
# Sample datum synthesis: prove a proposed schema pair works end-to-end by
# round-tripping an actual binary payload through fastavro.
# ---------------------------------------------------------------------------

def _sample_for(schema: Any) -> Any:
    if isinstance(schema, list):  # union -> first branch
        return _sample_for(schema[0])
    if isinstance(schema, str):
        return {
            "null": None, "boolean": True, "int": 1, "long": 1,
            "float": 1.5, "double": 1.5, "bytes": "Yg==", "string": "x",
        }[schema]
    t = schema.get("type")
    if t in _PRIMITIVE_SAMPLES:
        if "default" in schema:
            return schema["default"]
        return _PRIMITIVE_SAMPLES[t]
    if t == "record":
        return {f["name"]: _sample_for(f["type"]) for f in schema.get("fields", [])}
    if t == "enum":
        return schema["symbols"][0]
    if t == "fixed":
        return "0" * int(schema["size"])
    if t == "array":
        return [_sample_for(schema["items"])]
    if t == "map":
        return {"k": _sample_for(schema["values"])}
    raise SchemaError(f"cannot synthesise sample for schema fragment {schema}")


_PRIMITIVE_SAMPLES = {
    "null": None, "boolean": True, "int": 1, "long": 1,
    "float": 1.5, "double": 1.5, "bytes": "Yg==", "string": "sample",
}


def roundtrip_check(reader_schema: Any, writer_schema: Any) -> Optional[str]:
    """Static compatibility plus an actual encode/decode round-trip.

    Returns None on success, otherwise an error message with a field path.
    """
    err = explain(parse_schema(reader_schema), parse_schema(writer_schema))
    if err:
        return err
    try:
        sample = _sample_for(parse_schema(writer_schema))
        blob = encode(writer_schema, sample)
        out, derr = decode(reader_schema, blob, writer_schema)
        if derr:
            return derr
    except (SchemaError, CodecError) as exc:
        return str(exc)
    return None


# A realistic starter instrument-event schema (v1).
DEFAULT_SCHEMA_V1: Dict[str, Any] = {
    "type": "record",
    "name": "InstrumentEvent",
    "namespace": "lab.warehouse",
    "fields": [
        {"name": "sample_id", "type": "string"},
        {"name": "project", "type": "string"},
        {"name": "measurement", "type": "double"},
    ],
}
