# -*- coding: utf8 -*-

import collections
import json

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *
import pytest


##################################################################

def stable_json(obj):
    return json.dumps(obj, sort_keys=True)


POSITIONAL_YSON = yson.loads("<complex_type_mode=positional>yson")

class TypeTester(object):
    class DynamicHelper(object):
        def make_schema(self, type_v3):
            return make_schema(
                [
                    make_sorted_column("key", "int64"),
                    make_column("column", type_v3),
                ],
                unique_keys=True,
            )

        def write(self, path, value):
            insert_rows(path, [{"key": 0, "column": value}])

    class StaticHelper(object):
        def make_schema(self, type_v3):
            return make_schema([make_column("column", type_v3)])

        def write(self, path, value):
            tx_write_table(path, [{"column": value}], input_format=POSITIONAL_YSON)

    def __init__(self, type_list, dynamic=False):
        self.types = {}
        if dynamic:
            self._helper = self.DynamicHelper()
        else:
            self._helper = self.StaticHelper()

        for i,t in enumerate(type_list):
            path = "//tmp/table{}".format(i)
            self.types[stable_json(t)] = path
            create("table", path, force=True, attributes={
                "schema": self._helper.make_schema(t),
                "dynamic": dynamic,
            })

        if dynamic:
            for p in self.types.values():
                mount_table(p)
            for p in self.types.values():
                wait_for_tablet_state(path, "mounted")

    def check_good_value(self, logical_type, value):
        path = self.types.get(stable_json(logical_type), None)
        if path is None:
            raise ValueError("type is unknown")
        self._helper.write(path, value)

    def check_bad_value(self, logical_type, value):
        with raises_yt_error(SchemaViolation):
            self.check_good_value(logical_type, value)

    def check_conversion_error(self, logical_type, value):
        with raises_yt_error("Unable to convert"):
            self.check_good_value(logical_type, value)


class SingleColumnTable(object):
    def __init__(self, column_type, optimize_for, path="//tmp/table"):
        self.path = path
        create("table", self.path, force=True, attributes={
            "optimize_for": optimize_for,
            "schema": make_schema(
                [make_column("column", column_type)],
                strict=True,
                unique_keys=False)
        })

    def check_good_value(self, value):
        tx_write_table(self.path, [{"column": value}], input_format=POSITIONAL_YSON)

    def check_bad_value(self, value):
        with raises_yt_error(SchemaViolation):
            self.check_good_value(value)

TypeV1 = collections.namedtuple("TypeV1", ["type", "required"])

def type_v3_to_type_v1(type_v3):
    table = "//tmp/type_v3_to_type_v1_helper"
    create("table", table, force=True, attributes={
        "schema": make_schema(
            [make_column("column", type_v3)],
            strict=True,
            unique_keys=False,
        )
    })
    column_schema = get(table + "/@schema/0")
    remove(table)
    return TypeV1(column_schema["type"], column_schema["required"])

@authors("ermolovd")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestComplexTypes(YTEnvSetup):
    @authors("ermolovd")
    def test_complex_optional(self, optimize_for):
        type_v3 = optional_type(optional_type("int8"))
        assert type_v3_to_type_v1(type_v3) == TypeV1("any", False)

        test_table = SingleColumnTable(type_v3, optimize_for)
        test_table.check_good_value(None)
        test_table.check_good_value([None])
        test_table.check_good_value([-42])

        test_table.check_bad_value([])
        test_table.check_bad_value([257])

    @authors("ermolovd")
    def test_struct(self, optimize_for):
        type_v3 = struct_type([
            ("a", "utf8"),
            ("b", optional_type("int64")),
        ])
        assert type_v3_to_type_v1(type_v3) == TypeV1("any", True)

        test_table = SingleColumnTable(type_v3, optimize_for)
        test_table.check_good_value(["one", 1])
        test_table.check_good_value(["two", None])
        test_table.check_good_value(["three"])

        test_table.check_bad_value([])
        test_table.check_bad_value(None)
        test_table.check_bad_value(["one", 2, 3])
        test_table.check_bad_value(["bar", "baz"])

    @authors("ermolovd")
    def test_malformed_struct(self, optimize_for):
        with raises_yt_error("Name of struct field #0 is empty"):
            SingleColumnTable(
                struct_type([
                    ("", "int64"),
                ]),
                optimize_for,
            )

    @authors("ermolovd")
    def test_list(self, optimize_for):
        type_v3 = list_type(optional_type("string"))

        assert type_v3_to_type_v1(type_v3) == TypeV1("any", True)

        test_table = SingleColumnTable(type_v3, optimize_for)
        test_table.check_good_value([])
        test_table.check_good_value(["one"])
        test_table.check_good_value(["one", "two"])
        test_table.check_good_value(["one", "two", None])

        test_table.check_bad_value(None)
        test_table.check_bad_value({})
        test_table.check_bad_value([1,None])

    @authors("ermolovd")
    def test_tuple(self, optimize_for):
        type_v3 = tuple_type([
            "utf8",
            optional_type("int64")
        ])
        assert type_v3_to_type_v1(type_v3) == TypeV1("any", True)

        test_table = SingleColumnTable(type_v3, optimize_for)
        test_table.check_good_value(["one", 1])
        test_table.check_good_value(["two", None])

        test_table.check_bad_value(["three"])
        test_table.check_bad_value([])
        test_table.check_bad_value(None)
        test_table.check_bad_value(["one", 2, 3])
        test_table.check_bad_value(["bar", "baz"])


    @pytest.mark.parametrize("logical_type", [
        variant_tuple_type(["utf8", optional_type("int64")]),
        variant_struct_type([("a", "utf8"), ("b", optional_type("int64"))]),
    ])
    @authors("ermolovd")
    def test_variant(self, logical_type, optimize_for):
        assert type_v3_to_type_v1(logical_type) == TypeV1("any", True)

        test_table = SingleColumnTable(logical_type, optimize_for)
        test_table.check_good_value([0, "foo"])
        test_table.check_good_value([1, None])
        test_table.check_good_value([1, 42])

        test_table.check_bad_value(None)
        test_table.check_bad_value([])
        test_table.check_bad_value(["three"])
        test_table.check_bad_value([0, "one", 2])
        test_table.check_bad_value([1, 3.14])
        test_table.check_bad_value([2, None])

    @pytest.mark.parametrize("null_type", ["null", "void"])
    @authors("ermolovd")
    def test_null_type(self, optimize_for, null_type):
        def check_schema():
            column_schema = get("//tmp/table/@schema/0")
            assert column_schema["required"] == False
            assert column_schema["type"] == null_type
            assert column_schema["type_v3"] == null_type

        create("table", "//tmp/table", force=True, attributes={
            "optimize_for": optimize_for,
            "schema": make_schema([{
                "name": "column",
                "type_v3": null_type,
            }])
        })
        check_schema()

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "column",
                "type": null_type,
            }])
        })
        check_schema()

        create("table", "//tmp/table", force=True, attributes={
            "optimize_for": optimize_for,
            "schema": make_schema([{
                "name": "column",
                "type": null_type,
                "required": False,
            }]),
        })
        check_schema()

        # no exception
        tx_write_table("//tmp/table", [{}, {"column": None}])
        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/table", [{"column": 0}])


        with raises_yt_error("Null type cannot be required"):
            create("table", "//tmp/table", force=True, attributes={
                "schema": make_schema([{
                    "name": "column",
                    "type": null_type,
                    "required": True,
                }])
        })

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "column",
                "type_v3": list_type(null_type),
            }])
        })
        tx_write_table("//tmp/table", [{"column": []}, {"column": [None]}])
        tx_write_table("//tmp/table", [{"column": []}, {"column": [None, None]}])
        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/table", [{"column": [0]}])

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "column",
                "type_v3": optional_type(null_type)
            }])
        })
        tx_write_table("//tmp/table", [{"column": None}, {"column": [None]}])

        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/table", [{"column": []}])

        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/table", [{"column": []}])

    @authors("ermolovd")
    def test_dict(self, optimize_for):
        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "column",
                "type_v3": dict_type(optional_type("string"), "int64"),
            }]),
            "optimize_for": optimize_for,
        })
        assert get("//tmp/table/@schema/0/type") == "any"
        assert get("//tmp/table/@schema/0/required") == True

        tx_write_table("//tmp/table", [
            {"column": []},
            {"column": [["one", 1]]},
            {"column": [["one", 1], ["two", 2]]},
            {"column": [[None, 1], [None, 2]]},
        ])

        def check_bad(value):
            with raises_yt_error(SchemaViolation):
                tx_write_table("//tmp/table", [
                    {"column": value},
                ])
        check_bad(None)
        check_bad({})
        check_bad(["one", 1])
        check_bad([["one"]])
        check_bad([["one", 1, 1]])
        check_bad([["one", None]])

    @authors("ermolovd")
    def test_tagged(self, optimize_for):
        logical_type1 = struct_type([
            ("a", tagged_type("yt.cluster_name", "utf8")),
            ("b", optional_type("int64")),
        ])
        assert type_v3_to_type_v1(logical_type1) == TypeV1("any", True)

        table1 = SingleColumnTable(logical_type1, optimize_for)

        table1.check_good_value(["hume", 1])
        table1.check_good_value(["freud", None])
        table1.check_good_value(["hahn"])

        table1.check_bad_value([])
        table1.check_bad_value(None)
        table1.check_bad_value(["sakura", 2, 3])
        table1.check_bad_value(["betula", "redwood"])

        logical_type2 = tagged_type("even", optional_type("int64"))
        assert type_v3_to_type_v1(logical_type2) == TypeV1("int64", False)

        table2 = SingleColumnTable(logical_type2, optimize_for)

        table2.check_good_value(0)
        table2.check_good_value(2)
        table2.check_good_value(None)

        table2.check_bad_value("1")
        table2.check_bad_value(3.0)


@authors("ermolovd")
class TestComplexTypesMisc(YTEnvSetup):
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    def test_set_old_schema(self):
        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema(
                [
                    {
                        "name": "foo",
                        "type": "int64",
                    }
                ],
                strict=True,
                unique_keys=False)
        })

        assert get("//tmp/table/@schema/0/type_v3") == optional_type("int64")

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema(
                [
                    {
                        "name": "foo",
                        "type": "uint8",
                        "required": True,
                    }
                ],
                strict=True,
                unique_keys=False)
        })

        assert get("//tmp/table/@schema/0/type_v3") == "uint8"

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema(
                [
                    {
                        "name": "foo",
                        "type": "utf8",
                        "required": False,
                    }
                ],
                strict=True,
                unique_keys=False
            )
        })

        assert get("//tmp/table/@schema/0/type_v3") == optional_type("utf8")

    @authors("ermolovd")
    def test_set_new_schema(self):
        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema(
                [
                    {
                        "name": "foo",
                        "type_v3": optional_type("int8"),
                    }
                ],
                strict=True,
                unique_keys=False
            )
        })

        assert get("//tmp/table/@schema/0/type") == "int8"
        assert get("//tmp/table/@schema/0/required") == False

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema(
                [
                    {
                        "name": "foo",
                        "type_v3": "string",
                    }
                ],
                strict=True,
                unique_keys=False
            )
        })

        assert get("//tmp/table/@schema/0/type") == "string"
        assert get("//tmp/table/@schema/0/required") == True

    @authors("ermolovd")
    def test_set_both_schemas(self):
        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "foo",
                "type": "uint32",
                "type_v3": "uint32"
            }])
        })

        assert get("//tmp/table/@schema/0/type") == "uint32"
        assert get("//tmp/table/@schema/0/required") == True
        assert get("//tmp/table/@schema/0/type_v3") == "uint32"

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "foo",
                "type": "double",
                "required": False,
                "type_v3": optional_type("double")
            }])
        })

        assert get("//tmp/table/@schema/0/type") == "double"
        assert get("//tmp/table/@schema/0/required") == False
        assert get("//tmp/table/@schema/0/type_v3") == optional_type("double")

        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "foo",
                "type": "boolean",
                "required": True,
                "type_v3": "bool"
            }])
        })

        assert get("//tmp/table/@schema/0/type") == "boolean"
        assert get("//tmp/table/@schema/0/required") == True
        assert get("//tmp/table/@schema/0/type_v3") == "bool"

        with raises_yt_error("Error validating column"):
            create("table", "//tmp/table", force=True, attributes={
                "schema": make_schema([{
                    "name": "foo",
                    "type": "double",
                    "type_v3": "string",
                }])
            })


    @authors("ermolovd")
    def test_complex_types_disallowed_in_dynamic_tables(self):
        sync_create_cells(1)
        with raises_yt_error("Complex types are not allowed in dynamic tables"):
            create("table", "//test-dynamic-table", attributes={
                "schema": make_schema([
                    {
                        "name": "key",
                        "type_v3": "string",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "value",
                        "type_v3": optional_type(optional_type("string")),
                    },
                ], unique_keys=True),
                "dynamic": True})

        with raises_yt_error("Complex types are not allowed in dynamic tables"):
            create("table", "//test-dynamic-table", attributes={
                "schema": make_schema([
                    {
                        "name": "key",
                        "type_v3": "string",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "value",
                        "type_v3": optional_type(optional_type("string")),
                    },
                ], unique_keys=True),
                "dynamic": True})

    @authors("ermolovd")
    def test_complex_types_disallowed_alter(self):
        create("table", "//table", attributes={
            "schema": make_schema([
                make_column("column", list_type("int64")),
            ])
        })
        tx_write_table("//table", [{"column": []}])
        with raises_yt_error("Type mismatch for column"):
            alter_table("//table", schema=make_schema([
                make_column("column", optional_type(list_type("int64")))
            ]))

        with raises_yt_error("Cannot insert a new required column"):
            alter_table("//table", schema=make_schema([
                make_column("column", list_type("int64")),
                make_column("column2", list_type("int64")),
            ]))

        alter_table("//table", schema=make_schema([
            make_column("column", list_type("int64")),
            make_column("column2", optional_type(list_type("int64"))),
        ]))

    @authors("ermolovd")
    def test_infer_tagged_schema(self):
        table = "//tmp/input1"
        create("table", table, attributes={"schema": make_schema([
            {"name": "value", "type_v3": tagged_type("some-tag", "string")}
        ], unique_keys=False, strict=True)})
        table = "//tmp/input2"
        create("table", table, attributes={"schema": make_schema([
            {"name": "value", "type_v3": "string"},
        ], unique_keys=False, strict=True)})

        tx_write_table("//tmp/input1", [{"value": "foo"}])
        tx_write_table("//tmp/input2", [{"value": "bar"}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            merge(in_=["//tmp/input1", "//tmp/input2"], out="//tmp/output", mode="unordered")

        merge(in_=["//tmp/input1", "//tmp/input2"],
              out="<schema=[{name=value;type_v3=utf8}]>//tmp/output",
              spec={"schema_inference_mode" : "from_output"},
              mode="unordered")

    @authors("ermolovd")
    def test_infer_null_void(self):
        table = "//tmp/input1"
        create("table", table, attributes={"schema": make_schema([
            {"name": "value", "type_v3": "void"},
        ], unique_keys=False, strict=True)})
        table = "//tmp/input2"
        create("table", table, attributes={"schema": make_schema([
            {"name": "value", "type_v3": "null"},
        ], unique_keys=False, strict=True)})

        tx_write_table("//tmp/input1", [{"value": None}])
        tx_write_table("//tmp/input2", [{"value": None}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            merge(in_=["//tmp/input1", "//tmp/input2"], out="//tmp/output", mode="unordered")

        merge(in_=["//tmp/input1", "//tmp/input2"],
              out="<schema=[{name=value;type_v3=null}]>//tmp/output",
              spec={"schema_inference_mode" : "from_output"},
              mode="unordered")


class TestLogicalType(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    @authors("ermolovd")
    @pytest.mark.parametrize("table_type", ["static", "dynamic"])
    def test_logical_types(self, table_type):
        dynamic = table_type == "dynamic"
        if dynamic:
            sync_create_cells(1)

        type_tester = TypeTester([
            "int8",
            "int16",
            "int32",
            "int64",

            "uint8",
            "uint16",
            "uint32",
            "uint64",

            "utf8",
            "string",
            "null",

            "date",
            "datetime",
            "timestamp",
            "interval",
        ], dynamic=dynamic)

        type_tester.check_good_value("int8", 2 ** 7 - 1)
        type_tester.check_good_value("int8", 0)
        type_tester.check_good_value("int8", - 2 ** 7)
        type_tester.check_bad_value("int8", 2 ** 7)
        type_tester.check_bad_value("int8", - 2 ** 7 - 1)

        type_tester.check_good_value("int16", 2 ** 15 - 1)
        type_tester.check_good_value("int16", 0)
        type_tester.check_good_value("int16", - 2 ** 15)
        type_tester.check_bad_value("int16", 2 ** 15)
        type_tester.check_bad_value("int16", - 2 ** 15 - 1)

        type_tester.check_good_value("int32", 2 ** 31 - 1)
        type_tester.check_good_value("int32", 0)
        type_tester.check_good_value("int32", - 2 ** 31)
        type_tester.check_bad_value("int32", 2 ** 31)
        type_tester.check_bad_value("int32", - 2 ** 31 - 1)

        type_tester.check_good_value("int64", 2 ** 63 - 1)
        type_tester.check_good_value("int64", 0)
        type_tester.check_good_value("int64", - 2 ** 63)
        type_tester.check_conversion_error("int64", 2 ** 63)

        type_tester.check_good_value("uint8", 0)
        type_tester.check_good_value("uint8", 1)
        type_tester.check_good_value("uint8", 2 ** 8 - 1)
        type_tester.check_bad_value("uint8", 2 ** 8)
        type_tester.check_conversion_error("uint8", -1)

        type_tester.check_good_value("uint16", 0)
        type_tester.check_good_value("uint16", 2 ** 16 - 1)
        type_tester.check_bad_value("uint16", 2 ** 16)
        type_tester.check_conversion_error("uint16", -1)

        type_tester.check_good_value("uint32", 0)
        type_tester.check_good_value("uint32", 2 ** 32 - 1)
        type_tester.check_bad_value("uint32", 2 ** 32)
        type_tester.check_conversion_error("uint32", -1)

        type_tester.check_good_value("uint64", 0)
        type_tester.check_good_value("uint64", 2 ** 64 - 1)
        type_tester.check_conversion_error("uint64", -1)

        type_tester.check_good_value("utf8", "ff")
        type_tester.check_good_value("utf8", "ЫТЬ")
        type_tester.check_bad_value("utf8", "\xFF")
        type_tester.check_bad_value("utf8", 1)

        type_tester.check_good_value("string", "ff")
        type_tester.check_good_value("string", "ЫТЬ")
        type_tester.check_good_value("string", "\xFF")
        type_tester.check_bad_value("string", 1)

        type_tester.check_good_value("null", None)
        type_tester.check_bad_value("null", 0)
        type_tester.check_bad_value("null", False)
        type_tester.check_bad_value("null", "")

        date_upper_bound = 49673
        type_tester.check_good_value("date", 0)
        type_tester.check_good_value("date", 5)
        type_tester.check_good_value("date", date_upper_bound - 1)
        type_tester.check_conversion_error("date", -1)
        type_tester.check_bad_value("date", date_upper_bound)

        datetime_upper_bound = date_upper_bound * 86400
        type_tester.check_good_value("datetime", 0)
        type_tester.check_good_value("datetime", 5)
        type_tester.check_good_value("datetime", datetime_upper_bound - 1)
        type_tester.check_conversion_error("datetime", -1)
        type_tester.check_bad_value("datetime", datetime_upper_bound)

        timestamp_upper_bound = datetime_upper_bound * 10 ** 6
        type_tester.check_good_value("timestamp", 0)
        type_tester.check_good_value("timestamp", 5)
        type_tester.check_good_value("timestamp", timestamp_upper_bound - 1)
        type_tester.check_conversion_error("timestamp", -1)
        type_tester.check_bad_value("timestamp", timestamp_upper_bound)

        type_tester.check_good_value("interval", 0)
        type_tester.check_good_value("interval", 5)
        type_tester.check_good_value("interval", timestamp_upper_bound - 1)
        type_tester.check_good_value("interval", -timestamp_upper_bound + 1)
        type_tester.check_bad_value("interval", timestamp_upper_bound)
        type_tester.check_bad_value("interval", -timestamp_upper_bound)

    @authors("ermolovd")
    def test_bad_alter_table(self):
        def expect_error_alter_table(schema_before, schema_after):
            remove("//test-alter-table", force=True)
            create("table", "//test-alter-table", attributes={"schema": schema_before})
            # Make table nonempty, since empty table allows any alter
            tx_write_table("//test-alter-table", [{}])
            with raises_yt_error("Cannot alter type" ):
                alter_table("//test-alter-table", schema=schema_after)

        for (source_type, bad_destination_type_list) in [
            ("int8", ["uint64", "uint8", "string"]),
            ("int16", ["uint16", "uint16", "string", "int8"]),
            ("int32", ["uint32", "uint32", "string", "int8", "int16"]),
        ]:
            for destination_type in bad_destination_type_list:
                expect_error_alter_table(
                    [{"name": "column_name", "type": source_type}],
                    [{"name": "column_name", "type": destination_type}])

    @authors("ermolovd")
    def test_logical_type_column_constrains(self):
        with raises_yt_error("Computed column \"key1\" type mismatch: declared type"):
            create("table", "//test-table",
                   attributes={"schema": [
                       {"name": "key1", "type": "int32", "expression": "100"},
                   ]})

        with raises_yt_error("Aggregated column \"key1\" is forbiden to have logical type"):
            create("table", "//test-table",
                   attributes={"schema": [
                       {"name": "key1", "type": "int32", "aggregate": "sum"},
                   ]})

class TestRequiredOption(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1
    @authors("ermolovd")
    def test_required_static_tables(self):
        create("table", "//tmp/required_table",
               attributes={
                   "schema": [
                       {
                           "name": "value",
                           "type": "string",
                           "required": True,
                       }
                   ],
               })

        tx_write_table("//tmp/required_table", [{"value": "foo"}])
        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/required_table", [{"value": 100500}])
        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/required_table", [{"value": None}])
        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/required_table", [{}])

    @authors("ermolovd")
    def test_required_any_is_disallowed(self):
        with raises_yt_error("Column of type \"any\" cannot be \"required\""):
            create("table", "//tmp/required_table",
                   attributes={
                       "schema": [
                           {
                               "name": "value",
                               "type": "any",
                               "required": True,
                           }
                       ],
                   })
        with raises_yt_error("Column of type \"any\" cannot be \"required\""):
            create("table", "//tmp/dynamic_required_table",
                   attributes={
                       "dynamic": True,
                       "schema": [
                           {
                               "name": "key",
                               "type": "string",
                               "sort_order": "ascending",
                           },
                           {
                               "name": "value",
                               "type": "any",
                               "required": True,
                           }
                       ],
                   })

    @authors("ermolovd")
    def test_alter_required_column(self):
        table = "//tmp/static_table"
        create("table", table,
               attributes={
                   "schema": [
                       {
                           "name": "column",
                           "type": "string",
                       }
                   ],
               })
        tx_write_table(table, [{"column": None}])
        with raises_yt_error("Cannot alter type"):
            alter_table(
                table,
                schema=[
                    {
                        "name": "column",
                        "type": "string",
                        "required": True,
                    }
                ]
            )
        tx_write_table(table, [{"column": None}])

        create("table", table,
               force=True,
               attributes={
                   "schema": [
                       {
                           "name": "column",
                           "type": "string",
                           "required": True,
                       }
                   ],
               })
        tx_write_table(table, [{"column": "foo"}])

        # No exception.
        alter_table(
            table,
            schema=[
                {
                    "name": "column",
                    "type": "string",
                }
            ]
        )

        create("table", table,
               force=True,
               attributes={
                   "schema": [
                       {
                           "name": "column1",
                           "type": "string",
                       }
                   ],
               })
        tx_write_table(table, [{"column1": "foo"}])

        with raises_yt_error("Cannot insert a new required column "):
            alter_table(
                table,
                schema=[
                    {
                        "name": "column1",
                        "type": "string",
                    },
                    {
                        "name": "column2",
                        "type": "string",
                        "required": True,
                    }
                ]
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("sorted_table", [False, True])
    def test_infer_required_column(self, sorted_table):
        if sorted_table:
            schema = make_schema([
                {"name": "key", "type": "string", "required": False, "sort_order": "ascending"},
                {"name": "value", "type": "string", "required": True},
            ], unique_keys=False, strict=True)
        else:
            schema = make_schema([
                {"name": "key", "type": "string", "required": False},
                {"name": "value", "type": "string", "required": True},
            ], unique_keys=False, strict=True)
        table = "//tmp/input1"
        create("table", table, attributes={"schema": schema})
        table = "//tmp/input2"
        create("table", table, attributes={"schema": schema})
        tx_write_table("//tmp/input1", [{"key": "foo", "value": "bar"}])
        tx_write_table("//tmp/input2", [{"key": "foo", "value": "baz"}])

        create("table", "//tmp/output")

        mode = "sorted" if sorted_table else "unordered"
        merge(in_=["//tmp/input1", "//tmp/input2"], out="//tmp/output", mode=mode)

        assert normalize_schema(get("//tmp/output/@schema")) == schema

    @authors("ermolovd")
    def test_infer_mixed_requiredness(self):
        table = "//tmp/input1"
        create("table", table, attributes={"schema": make_schema([
            make_column("value", "string")
        ], unique_keys=False, strict=True)})
        table = "//tmp/input2"
        create("table", table, attributes={"schema": make_schema([
            make_column("value", optional_type("string"))
        ], unique_keys=False, strict=True)})

        tx_write_table("//tmp/input1", [{"value": "foo"}])
        tx_write_table("//tmp/input2", [{"value": "bar"}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            # Schemas are incompatible
            merge(in_=["//tmp/input1", "//tmp/input2"], out="//tmp/output", mode="unordered")

    @authors("ifsmirnov")
    def test_required_columns_in_dynamic_tables_schema(self):
        schema = [
                {"name": "key_req", "type": "int64", "sort_order": "ascending", "required": True},
                {"name": "key_opt", "type": "int64", "sort_order": "ascending"},
                {"name": "value_req", "type": "string", "required": True},
                {"name": "value_opt", "type": "string"}]

        sync_create_cells(1)
        create("table", "//tmp/t", attributes={"schema": schema, "dynamic": True})

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key_req": 1, "key_opt": 2, "value_req": "x", "value_opt": "y"}])
        sync_unmount_table("//tmp/t")

        # Required columns cannot be added
        with raises_yt_error("Cannot insert a new required column"):
            alter_table("//tmp/t", schema=schema + [{"name": "value3_req", "type": "string", "required": True}])

        # Adding non-required columns is OK
        schema += [{"name": "value3_opt", "type": "string", "required": False}]
        alter_table("//tmp/t", schema=schema)

        # Old column cannot become required
        bad_schema = [i.copy() for i in schema]
        bad_schema[3]["required"] = True
        with raises_yt_error("Cannot alter type"):
            alter_table("//tmp/t", schema=bad_schema)

        # Removing 'required' attribute is OK
        good_schema = [i.copy() for i in schema]
        good_schema[2]["required"] = False
        alter_table("//tmp/t", schema=good_schema)

class TestSchemaDeduplication(YTEnvSetup):
    @authors("ermolovd")
    def test_empty_schema(self):
        create("table", "//tmp/table")
        assert get("//tmp/table/@schema_duplicate_count") == 0

    @authors("ermolovd")
    def test_simple_schema(self):
        def get_schema(strict):
            return make_schema([make_column("value", "string")], unique_keys=False, strict=strict)

        create("table", "//tmp/table1", attributes={"schema": get_schema(True)})
        create("table", "//tmp/table2", attributes={"schema": get_schema(True)})
        create("table", "//tmp/table3", attributes={"schema": get_schema(False)})

        assert get("//tmp/table1/@schema_duplicate_count") == 2
        assert get("//tmp/table2/@schema_duplicate_count") == 2
        assert get("//tmp/table3/@schema_duplicate_count") == 1

        alter_table("//tmp/table2", schema=get_schema(False))

        assert get("//tmp/table1/@schema_duplicate_count") == 1
        assert get("//tmp/table2/@schema_duplicate_count") == 2
        assert get("//tmp/table3/@schema_duplicate_count") == 2


class TestSchemaValidation(YTEnvSetup):
    @authors("ermolovd")
    def test_schema_complexity(self):
        def make_schema(size):
            return [
                {"name": "column{}".format(i), "type": "int64"}
                for i in range(size)
            ]
        def make_row(size):
            return {"column{}".format(i) : i for i in range(size)}

        bad_size = 32 * 1024
        with raises_yt_error("Table schema is too complex"):
            create("table", "//tmp/bad-schema-1", attributes={"schema": make_schema(bad_size)})

        create("table", "//tmp/bad-schema-2")
        with raises_yt_error("Too many columns in row"):
            tx_write_table("//tmp/bad-schema-2", [make_row(bad_size)])

        ok_size = bad_size - 1
        create("table", "//tmp/ok-schema", attributes={"schema": make_schema(ok_size)})
        tx_write_table("//tmp/ok-schema", [make_row(ok_size)])


@authors("ermolovd")
class TestErrorCodes(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    def test_YT_11522_missing_column(self):
        schema = [
            {"name": "foo", "type": "int64", "sort_order": "ascending", "required": True},
            {"name": "bar", "type": "int64"},
        ]

        sync_create_cells(1)
        create("table", "//tmp/t", attributes={"schema": schema, "dynamic": True})

        sync_mount_table("//tmp/t")
        with raises_yt_error(SchemaViolation):
            insert_rows("//tmp/t", [{"baz": 1}])
        sync_unmount_table("//tmp/t")

    def test_YT_11522_convesion_error(self):
        schema = [
            {"name": "foo", "type": "uint64"}
        ]

        create("table", "//tmp/t", attributes={"schema": schema})

        with raises_yt_error(SchemaViolation):
            tx_write_table("//tmp/t", [{"foo": -1}])
