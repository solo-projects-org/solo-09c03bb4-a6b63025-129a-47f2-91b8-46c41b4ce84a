"""Avro schema-resolution compatibility guard.

Implements the Avro 1.11 specification "Schema Resolution" rules so that the
platform can decide, *before* any format switch takes effect, whether a reader
schema can decode data written with a writer schema.

Supported evolution features:
  * all Avro primitive types and the full numeric promotion chain
    (int -> long -> float -> double; string <-> bytes)
  * record add/remove/reorder fields, field *defaults* for added reader fields
  * nested records, enums, fixed, maps
  * arrays (element-level recursive resolution)
  * unions (on either or both sides), including default-branch selection
  * aliases / namespace changes for named types

Every failure carries a field path such as ``record InstrumentEvent.measurements[].value``
so the UI can explain *which* field blocks a switch instead of just saying
"incompatible".
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

_PRIMITIVES = {"null", "boolean", "int", "long", "float", "double",
               "bytes", "string"}

# writer primitive -> set of reader primitives that can resolve it
_PROMOTION = {
    "int": {"int", "long", "float", "double"},
    "long": {"long", "float", "double"},
    "float": {"float", "double"},
    "double": {"double"},
    "string": {"string", "bytes"},
    "bytes": {"bytes", "string"},
}


def _type_name(schema: Any) -> str:
    if isinstance(schema, str):
        return schema
    if isinstance(schema, dict):
        if schema.get("type") == "array":
            return "array"
        if schema.get("type") == "map":
            return "map"
        if schema.get("type") == "record":
            return "record"
        if schema.get("type") == "enum":
            return "enum"
        if schema.get("type") == "fixed":
            return "fixed"
        return str(schema.get("type"))
    if isinstance(schema, list):
        return "union"
    return str(schema)


def _fullname(schema: dict) -> str:
    name = schema.get("name", "")
    namespace = schema.get("namespace")
    if namespace and "." not in name:
        return f"{namespace}.{name}"
    return name


def _aliases(schema: dict) -> set:
    result = set()
    for alias in schema.get("aliases", []) or []:
        ns = schema.get("namespace")
        if ns and "." not in alias:
            result.add(f"{ns}.{alias}")
        else:
            result.add(alias)
    return result


def _named_matches(reader: dict, writer: dict) -> bool:
    """Named-schema match by full name or reader-declared alias."""
    w_name = _fullname(writer)
    if _fullname(reader) == w_name:
        return True
    return w_name in _aliases(reader)


def _primitives_compatible(reader_t: str, writer_t: str) -> bool:
    if reader_t == writer_t:
        return True
    return reader_t in _PROMOTION.get(writer_t, set())


def can_read(reader: Any, writer: Any, path: str = "$") -> Tuple[bool, Optional[str]]:
    """Return (ok, error). error is None when ok, otherwise explains the path."""
    # ---- union reader: pick the first reader branch that can read writer ----
    if isinstance(reader, list):
        branch_errors = []
        for branch in reader:
            ok, err = can_read(branch, writer, path)
            if ok:
                return True, None
            branch_errors.append(err)
        return False, (
            f"{path}: union reader has no branch able to read writer type "
            f"'{_type_name(writer)}' (tried: {'; '.join(e for e in branch_errors if e)})"
        )

    # ---- union writer: every writer branch must be readable by the reader ----
    if isinstance(writer, list):
        for branch in writer:
            ok, err = can_read(reader, branch, f"{path}(union branch {_type_name(branch)})")
            if not ok:
                return False, err
        return True, None

    # both are now either string primitives or dict schemas
    if isinstance(reader, str) and isinstance(writer, str):
        if _primitives_compatible(reader, writer):
            return True, None
        return False, (f"{path}: reader '{reader}' cannot resolve writer '{writer}' "
                       f"(no numeric/type promotion path)")

    r_type = reader.get("type") if isinstance(reader, dict) else reader
    w_type = writer.get("type") if isinstance(writer, dict) else writer

    # primitive writer vs complex reader or vice versa
    if r_type in _PRIMITIVES or w_type in _PRIMITIVES:
        if r_type == w_type:
            return True, None
        if isinstance(r_type, str) and isinstance(w_type, str):
            if _primitives_compatible(r_type, w_type):
                return True, None
        return False, (f"{path}: type mismatch - reader '{_type_name(reader)}' "
                       f"cannot resolve writer '{_type_name(writer)}'")

    # ---- record ----
    if r_type == "record" or w_type == "record":
        if r_type != "record" or w_type != "record":
            return False, (f"{path}: type mismatch - reader '{_type_name(reader)}' "
                           f"cannot resolve writer '{_type_name(writer)}'")
        if not _named_matches(reader, writer):
            return False, (f"{path}: record name mismatch - reader '{_fullname(reader)}' "
                           f"cannot resolve writer record '{_fullname(writer)}' "
                           f"(use aliases to rename)")
        writer_fields = {f["name"]: f for f in writer.get("fields", [])}
        rec_path = f"{path}.{_fullname(writer)}"
        for rf in reader.get("fields", []):
            fname = rf["name"]
            fpath = f"{rec_path}.{fname}"
            if fname in writer_fields:
                ok, err = can_read(rf["type"], writer_fields[fname]["type"], fpath)
                if not ok:
                    return False, err
            else:
                # reader field absent from writer: a default is mandatory
                if "default" not in rf:
                    return False, (f"{fpath}: field added in reader but missing from writer "
                                   f"and has no 'default' value")
        # writer-only fields are simply skipped (field removal is always safe)
        return True, None

    # ---- enum ----
    if r_type == "enum" or w_type == "enum":
        if r_type != "enum" or w_type != "enum":
            return False, f"{path}: type mismatch - reader '{r_type}' vs writer '{w_type}'"
        if not _named_matches(reader, writer):
            return False, (f"{path}: enum name mismatch - reader '{_fullname(reader)}' "
                           f"vs writer '{_fullname(writer)}'")
        w_symbols = set(writer.get("symbols", []))
        missing = w_symbols - set(reader.get("symbols", []))
        if missing:
            return False, (f"{path}: writer enum contains symbols absent from reader "
                           f"({sorted(missing)}); only reader-side symbol additions are safe")
        return True, None

    # ---- fixed ----
    if r_type == "fixed" or w_type == "fixed":
        if r_type != "fixed" or w_type != "fixed":
            return False, f"{path}: type mismatch - reader '{r_type}' vs writer '{w_type}'"
        if not _named_matches(reader, writer):
            return False, (f"{path}: fixed name mismatch - reader '{_fullname(reader)}' "
                           f"vs writer '{_fullname(writer)}'")
        if reader.get("size") != writer.get("size"):
            return False, (f"{path}: fixed size mismatch - reader size {reader.get('size')} "
                           f"vs writer size {writer.get('size')} (fixed size cannot evolve)")
        return True, None

    # ---- array ----
    if r_type == "array" or w_type == "array":
        if r_type != "array" or w_type != "array":
            return False, f"{path}: type mismatch - reader '{r_type}' vs writer '{w_type}'"
        return can_read(reader["items"], writer["items"], f"{path}[]")

    # ---- map ----
    if r_type == "map" or w_type == "map":
        if r_type != "map" or w_type != "map":
            return False, f"{path}: type mismatch - reader '{r_type}' vs writer '{w_type}'"
        return can_read(reader["values"], writer["values"], f"{path}<value>")

    return False, f"{path}: unsupported schema combination reader={reader} writer={writer}"


def explain(reader: Any, writer: Any) -> Optional[str]:
    """Convenience wrapper: None means compatible."""
    ok, err = can_read(reader, writer)
    return None if ok else err
