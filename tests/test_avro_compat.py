"""Unit tests for the Avro schema-resolution guard.

Run: PYTHONPATH=/workspace python3 -m unittest tests.test_avro_compat -v
"""
import unittest

from app.avro_codec import roundtrip_check
from app.avro_compat import can_read, explain


def rec(name, fields, namespace="lab"):
    return {"type": "record", "name": name, "namespace": namespace, "fields": fields}


class TestPrimitivesAndPromotion(unittest.TestCase):
    def test_same_primitive(self):
        self.assertIsNone(explain("string", "string"))
        self.assertIsNone(explain("int", "int"))

    def test_numeric_promotion_chain(self):
        for writer, reader in [("int", "long"), ("int", "float"), ("int", "double"),
                              ("long", "float"), ("long", "double"),
                              ("float", "double")]:
            self.assertIsNone(explain(reader, writer), f"{writer}->{reader}")

    def test_numeric_demotion_rejected(self):
        for writer, reader in [("long", "int"), ("double", "float"),
                               ("float", "long"), ("double", "int")]:
            err = explain(reader, writer)
            self.assertIsNotNone(err, f"{writer}->{reader} must be rejected")
            self.assertIn("cannot resolve", err)

    def test_string_bytes_bidirectional(self):
        self.assertIsNone(explain("bytes", "string"))
        self.assertIsNone(explain("string", "bytes"))

    def test_boolean_not_promotable(self):
        err = explain("int", "boolean")
        self.assertIsNotNone(err)
        self.assertIn("$", err)


class TestRecordsAndDefaults(unittest.TestCase):
    BASE = rec("InstrumentEvent", [
        {"name": "sample_id", "type": "string"},
        {"name": "project", "type": "string"},
        {"name": "measurement", "type": "double"},
    ])

    def test_add_reader_field_requires_default(self):
        no_default = rec("InstrumentEvent", [
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "string"},
            {"name": "measurement", "type": "double"},
            {"name": "instrument_id", "type": "string"},
        ])
        ok, err = can_read(no_default, self.BASE)
        self.assertFalse(ok)
        # field path must be present in the explanation
        self.assertIn("InstrumentEvent.instrument_id", err)
        self.assertIn("default", err)

    def test_add_reader_field_with_default_ok(self):
        with_default = rec("InstrumentEvent", [
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "string"},
            {"name": "measurement", "type": "double"},
            {"name": "instrument_id", "type": "string", "default": "X"},
        ])
        self.assertIsNone(explain(with_default, self.BASE))

    def test_remove_writer_field_is_safe(self):
        # Writer dropping a field is safe as long as every reader-required
        # field still exists in the writer (here the reader only keeps two).
        writer = rec("InstrumentEvent", [
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "string"},
            {"name": "measurement", "type": "double"},
            {"name": "legacy_note", "type": "string"},
        ])
        self.assertIsNone(explain(self.BASE, writer))

    def test_reader_required_field_missing_from_writer_rejected(self):
        # Avro rule: a reader field absent from the writer needs a default.
        writer = rec("InstrumentEvent", [
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "string"},
        ])
        err = explain(self.BASE, writer)
        self.assertIsNotNone(err)
        self.assertIn("InstrumentEvent.measurement", err)

    def test_field_type_change_without_promotion_rejected_with_path(self):
        reader = rec("InstrumentEvent", [
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "int"},
            {"name": "measurement", "type": "double"},
        ])
        err = explain(reader, self.BASE)
        self.assertIsNotNone(err)
        self.assertIn(".project", err)

    def test_field_reorder_is_safe(self):
        reordered = rec("InstrumentEvent", [
            {"name": "measurement", "type": "double"},
            {"name": "sample_id", "type": "string"},
            {"name": "project", "type": "string"},
        ])
        self.assertIsNone(explain(reordered, self.BASE))


class TestArray(unittest.TestCase):
    def test_array_compatible_items(self):
        writer = rec("Batch", [{"name": "ids", "type": {
            "type": "array", "items": "int"}}])
        reader = rec("Batch", [{"name": "ids", "type": {
            "type": "array", "items": "long"}}])
        self.assertIsNone(explain(reader, writer))

    def test_array_incompatible_items_reports_array_path(self):
        writer = rec("Batch", [{"name": "ids", "type": {
            "type": "array", "items": "string"}}])
        reader = rec("Batch", [{"name": "ids", "type": {
            "type": "array", "items": "int"}}])
        err = explain(reader, writer)
        self.assertIsNotNone(err)
        self.assertIn("ids[]", err)


class TestUnion(unittest.TestCase):
    def test_union_reader_branch_match(self):
        writer = "string"
        reader = ["null", "string"]
        self.assertIsNone(explain(reader, writer))

    def test_union_reader_no_branch(self):
        err = explain(["null", "int"], "string")
        self.assertIsNotNone(err)
        self.assertIn("union reader has no branch", err)

    def test_union_writer_all_branches_must_resolve(self):
        writer = ["null", "string", "int"]
        reader = ["null", "long"]
        err = explain(reader, writer)
        self.assertIsNotNone(err)  # string branch unresolvable

    def test_optional_field_with_default(self):
        writer = rec("E", [{"name": "f", "type": "string"}])
        reader = rec("E", [
            {"name": "f", "type": "string"},
            {"name": "g", "type": ["null", "double"], "default": None},
        ])
        self.assertIsNone(explain(reader, writer))

    def test_union_numeric_promotion_inside(self):
        writer = rec("E", [{"name": "f", "type": ["int", "long"]}])
        reader = rec("E", [{"name": "f", "type": ["long", "double"]}])
        self.assertIsNone(explain(reader, writer))


class TestNestedAndNamed(unittest.TestCase):
    def test_nested_record_path(self):
        writer = rec("Outer", [{"name": "inner", "type": rec("Inner", [
            {"name": "v", "type": "int"}])}])
        reader = rec("Outer", [{"name": "inner", "type": rec("Inner", [
            {"name": "v", "type": "string"}])}])
        err = explain(reader, writer)
        self.assertIsNotNone(err)
        self.assertIn("lab.Inner.v", err)

    def test_rename_via_alias(self):
        writer = rec("OldName", [{"name": "f", "type": "int"}])
        reader = {
            "type": "record", "name": "NewName", "namespace": "lab",
            "aliases": ["lab.OldName"],
            "fields": [{"name": "f", "type": "long"}],
        }
        self.assertIsNone(explain(reader, writer))

    def test_rename_without_alias_rejected(self):
        writer = rec("OldName", [{"name": "f", "type": "int"}])
        reader = rec("NewName", [{"name": "f", "type": "long"}])
        err = explain(reader, writer)
        self.assertIsNotNone(err)
        self.assertIn("record name mismatch", err)


class TestRoundTrip(unittest.TestCase):
    def test_roundtrip_add_default_field(self):
        writer = rec("E", [{"name": "f", "type": "int"}])
        reader = rec("E", [
            {"name": "f", "type": "long"},
            {"name": "g", "type": "string", "default": "d"},
        ])
        self.assertIsNone(roundtrip_check(reader, writer))

    def test_roundtrip_incompatible_returns_error(self):
        writer = rec("E", [{"name": "f", "type": "boolean"}])
        reader = rec("E", [{"name": "f", "type": "int"}])
        self.assertIsNotNone(roundtrip_check(reader, writer))


if __name__ == "__main__":
    unittest.main(verbosity=2)
