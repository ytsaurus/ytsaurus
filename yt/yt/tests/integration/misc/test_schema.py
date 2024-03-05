# -*- coding: utf8 -*-

from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, ls, get, copy, remove, set,
    exists, create_table,
    start_transaction, abort_transaction, commit_transaction,
    insert_rows, select_rows, lookup_rows,
    alter_table, read_table, write_table,
    merge, sort,
    mount_table, wait_for_tablet_state, sync_create_cells,
    sync_compact_table, sync_flush_table, sync_mount_table, sync_unmount_table,
    raises_yt_error, get_driver)

from decimal_helpers import decode_decimal, encode_decimal, YtNaN, MAX_DECIMAL_PRECISION

from yt_type_helpers import (
    make_schema, normalize_schema, make_sorted_column, make_column, make_deleted_column,
    optional_type, list_type, dict_type, struct_type, tuple_type, variant_tuple_type, variant_struct_type,
    decimal_type, tagged_type)

from yt_helpers import skip_if_renaming_disabled, skip_if_renaming_not_differentiated

import yt_error_codes

from yt.common import YtError
import yt.yson as yson

import pytest

import builtins
import collections
import decimal
import json
import random
from copy import deepcopy


# Run our tests on all decimal precisions might be expensive so we create
# representative sample of possible precisions.
INTERESTING_DECIMAL_PRECISION_LIST = [
    1, 5, 9,  # 4 bytes
    10, 15, 18,  # 8 bytes
    19, 25, MAX_DECIMAL_PRECISION,  # 16 bytes
]

POSITIONAL_YSON = yson.loads(b"<complex_type_mode=positional>yson")

##################################################################


def stable_json(obj):
    return json.dumps(obj, sort_keys=True)


def tx_write_table(*args, **kwargs):
    """
    Write rows to table transactionally.

    If write_table fails with some error it is not guaranteed that table is not locked.
    Locks can linger for some time and prevent from working with this table.

    This function avoids such lingering locks by explicitly creating external transaction
    and aborting it explicitly in case of error.
    """
    parent_tx = kwargs.pop("tx", "0-0-0-0")
    timeout = kwargs.pop("timeout", 60000)

    try:
        tx = start_transaction(timeout=timeout, tx=parent_tx)
    except Exception as e:
        raise AssertionError("Cannot start transaction: {}".format(e))

    try:
        write_table(*args, tx=tx, **kwargs)
    except YtError:
        try:
            abort_transaction(tx)
        except Exception as e:
            raise AssertionError("Cannot abort wrapper transaction: {}".format(e))
        raise

    commit_transaction(tx)


def remove_entity(obj):
    if isinstance(obj, dict):
        return {
            key: remove_entity(value)
            for key, value in obj.items()
            if value is not None and value != yson.YsonEntity()
        }
    elif isinstance(obj, list):
        return [
            remove_entity(value) for value in obj
            if value is not None and value != yson.YsonEntity()
        ]
    else:
        return obj


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

        for i, t in enumerate(type_list):
            path = "//tmp/table{}".format(i)
            self.types[stable_json(t)] = path
            create(
                "table",
                path,
                force=True,
                attributes={
                    "schema": self._helper.make_schema(t),
                    "dynamic": dynamic,
                },
            )

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
        with raises_yt_error(yt_error_codes.SchemaViolation):
            self.check_good_value(logical_type, value)

    def check_conversion_error(self, logical_type, value):
        with raises_yt_error("Unable to convert"):
            self.check_good_value(logical_type, value)


class SingleColumnTable(object):
    def __init__(self, column_type, optimize_for, path="//tmp/table"):
        self.path = path
        create(
            "table",
            self.path,
            force=True,
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema([make_column("column", column_type)], strict=True, unique_keys=False),
            },
        )

    def check_good_value(self, value):
        tx_write_table(self.path, [{"column": value}], input_format=POSITIONAL_YSON)

    def check_bad_value(self, value):
        with raises_yt_error(yt_error_codes.SchemaViolation):
            self.check_good_value(value)


TypeV1 = collections.namedtuple("TypeV1", ["type", "required"])


def type_v3_to_type_v1(type_v3):
    table = "//tmp/type_v3_to_type_v1_helper"
    create(
        "table",
        table,
        force=True,
        attributes={
            "schema": make_schema(
                [make_column("column", type_v3)],
                strict=True,
                unique_keys=False,
            )
        },
    )
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
        type_v3 = struct_type(
            [
                ("a", "utf8"),
                ("b", optional_type("int64")),
            ]
        )
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
                struct_type(
                    [
                        ("", "int64"),
                    ]
                ),
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
        test_table.check_bad_value([1, None])

    @authors("ermolovd")
    def test_tuple(self, optimize_for):
        type_v3 = tuple_type(["utf8", optional_type("int64")])
        assert type_v3_to_type_v1(type_v3) == TypeV1("any", True)

        test_table = SingleColumnTable(type_v3, optimize_for)
        test_table.check_good_value(["one", 1])
        test_table.check_good_value(["two", None])

        test_table.check_bad_value(["three"])
        test_table.check_bad_value([])
        test_table.check_bad_value(None)
        test_table.check_bad_value(["one", 2, 3])
        test_table.check_bad_value(["bar", "baz"])

    @pytest.mark.parametrize(
        "logical_type",
        [
            variant_tuple_type(["utf8", optional_type("int64")]),
            variant_struct_type([("a", "utf8"), ("b", optional_type("int64"))]),
        ],
    )
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
            assert not column_schema["required"]
            assert column_schema["type"] == null_type
            assert column_schema["type_v3"] == null_type

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type_v3": null_type,
                        }
                    ]
                ),
            },
        )
        check_schema()

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type": null_type,
                        }
                    ]
                )
            },
        )
        check_schema()

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type": null_type,
                            "required": False,
                        }
                    ]
                ),
            },
        )
        check_schema()

        # no exception
        tx_write_table("//tmp/table", [{}, {"column": None}])
        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/table", [{"column": 0}])

        with raises_yt_error("Null type cannot be required"):
            create(
                "table",
                "//tmp/table",
                force=True,
                attributes={
                    "schema": make_schema(
                        [
                            {
                                "name": "column",
                                "type": null_type,
                                "required": True,
                            }
                        ]
                    )
                },
            )

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type_v3": list_type(null_type),
                        }
                    ]
                )
            },
        )
        tx_write_table("//tmp/table", [{"column": []}, {"column": [None]}])
        tx_write_table("//tmp/table", [{"column": []}, {"column": [None, None]}])
        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/table", [{"column": [0]}])

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={"schema": make_schema([{"name": "column", "type_v3": optional_type(null_type)}])},
        )
        tx_write_table("//tmp/table", [{"column": None}, {"column": [None]}])

        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/table", [{"column": []}])

        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/table", [{"column": []}])

    @authors("ermolovd")
    def test_dict(self, optimize_for):
        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type_v3": dict_type(optional_type("string"), "int64"),
                        }
                    ]
                ),
                "optimize_for": optimize_for,
            },
        )
        assert get("//tmp/table/@schema/0/type") == "any"
        assert get("//tmp/table/@schema/0/required")

        tx_write_table(
            "//tmp/table",
            [
                {"column": []},
                {"column": [["one", 1]]},
                {"column": [["one", 1], ["two", 2]]},
                {"column": [[None, 1], [None, 2]]},
            ],
        )

        def check_bad(value):
            with raises_yt_error(yt_error_codes.SchemaViolation):
                tx_write_table(
                    "//tmp/table",
                    [
                        {"column": value},
                    ],
                )

        check_bad(None)
        check_bad({})
        check_bad(["one", 1])
        check_bad([["one"]])
        check_bad([["one", 1, 1]])
        check_bad([["one", None]])

    @authors("ermolovd")
    def test_tagged(self, optimize_for):
        logical_type1 = struct_type(
            [
                ("a", tagged_type("yt.cluster_name", "utf8")),
                ("b", optional_type("int64")),
            ]
        )
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
    def test_decimal(self, optimize_for):
        assert type_v3_to_type_v1(decimal_type(3, 2)) == TypeV1("string", True)

        table = SingleColumnTable(
            decimal_type(3, 2),
            optimize_for)
        table.check_good_value(encode_decimal("3.12", 3, 2))
        table.check_good_value(encode_decimal("-2.7", 3, 2))
        table.check_good_value(encode_decimal("Nan", 3, 2))
        table.check_good_value(encode_decimal("Inf", 3, 2))
        table.check_good_value(encode_decimal("-Inf", 3, 2))

        table.check_bad_value(encode_decimal("43.12", 3, 2))
        table.check_bad_value(3.14)

        table = SingleColumnTable(
            optional_type(decimal_type(3, 2)),
            optimize_for)
        table.check_good_value(encode_decimal("3.12", 3, 2))
        table.check_good_value(encode_decimal("-2.7", 3, 2))
        table.check_bad_value(encode_decimal("43.12", 3, 2))
        table.check_bad_value(3.14)
        table.check_good_value(None)

        table = SingleColumnTable(
            list_type(decimal_type(3, 2)),
            optimize_for)
        table.check_good_value([encode_decimal("3.12", 3, 2)])
        table.check_bad_value([encode_decimal("43.12", 3, 2)])
        table.check_bad_value([3.12])

    @authors("ermolovd")
    def test_uuid(self, optimize_for):
        uuid = b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF"
        table = SingleColumnTable("uuid", optimize_for)
        table.check_good_value(uuid)

        table.check_bad_value(b"")
        table.check_bad_value(uuid[:-1])
        table.check_bad_value(uuid + b"a")

        table = SingleColumnTable(list_type("uuid"), optimize_for)
        table.check_good_value([uuid])
        table.check_bad_value([b""])
        table.check_bad_value([uuid[:-1]])
        table.check_bad_value([uuid + b"a"])


@authors("ermolovd")
class TestComplexTypesMisc(YTEnvSetup):
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    @pytest.mark.parametrize("precision", list(range(3, 36)))
    def test_decimal_various_precision(self, precision):
        table1 = SingleColumnTable(decimal_type(precision, 2), "lookup")
        table1.check_good_value(encode_decimal(decimal.Decimal("3.12"), precision, 2))
        table1.check_good_value(encode_decimal(decimal.Decimal("inf"), precision, 2))
        table1.check_good_value(encode_decimal(decimal.Decimal("nan"), precision, 2))
        table1.check_good_value(encode_decimal(decimal.Decimal("-nan"), precision, 2))
        table1.check_bad_value("")
        table1.check_bad_value("foo")
        table1.check_bad_value(None)

    @authors("ermolovd")
    @pytest.mark.parametrize("type_v3", [
        decimal_type(MAX_DECIMAL_PRECISION, 2),
        optional_type(decimal_type(MAX_DECIMAL_PRECISION, 2)),
        tagged_type("foo", decimal_type(MAX_DECIMAL_PRECISION, 2)),
        optional_type(tagged_type("foo", decimal_type(MAX_DECIMAL_PRECISION, 2))),
        tagged_type("bar", optional_type(decimal_type(MAX_DECIMAL_PRECISION, 2))),
        tagged_type("bar", optional_type(tagged_type("foo", decimal_type(MAX_DECIMAL_PRECISION, 2)))),
    ])
    def test_decimal_optional_tagged_combinations(self, type_v3):
        table1 = SingleColumnTable(type_v3, "lookup")
        table1.check_good_value(encode_decimal(decimal.Decimal("3.12"), MAX_DECIMAL_PRECISION, 2))
        table1.check_good_value(encode_decimal(decimal.Decimal("1" * 33), MAX_DECIMAL_PRECISION, 2))
        table1.check_bad_value(encode_decimal(decimal.Decimal("1" * 34), MAX_DECIMAL_PRECISION, 2))

        table1.check_bad_value(5)
        table1.check_bad_value("foo")
        table1.check_bad_value(5.5)

    @authors("ermolovd")
    def test_decimal_sort(self):
        create_table(
            "//tmp/table",
            schema=make_schema([{"name": "key", "type_v3": decimal_type(3, 2)}])
        )

        data = [
            decimal.Decimal("Infinity"),
            decimal.Decimal("-Infinity"),
            decimal.Decimal("Nan"),
            decimal.Decimal("2.71"),
            decimal.Decimal("3.14"),
            decimal.Decimal("-6.62"),
            decimal.Decimal("0"),
        ]

        write_table("//tmp/table", [{"key": encode_decimal(d, 3, 2)} for d in data])
        sort(in_="//tmp/table", out="//tmp/table", sort_by=["key"])

        actual = [decode_decimal(row["key"], 3, 2) for row in read_table("//tmp/table")]

        assert [
            decimal.Decimal("-Infinity"),
            decimal.Decimal("-6.62"),
            decimal.Decimal("0"),
            decimal.Decimal("2.71"),
            decimal.Decimal("3.14"),
            decimal.Decimal("Infinity"),
            YtNaN,
        ] == actual

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("precision", [p for p in INTERESTING_DECIMAL_PRECISION_LIST])
    @authors("ermolovd")
    def test_decimal_sort_random(self, precision):
        scale = precision // 2
        digits = "0123456789"
        rnd = random.Random()
        rnd.seed(42)

        def generate_random_decimal():
            decimal_text = (
                "".join(rnd.choice(digits) for _ in range(precision - scale))
                + "." + "".join(rnd.choice(digits) for _ in range(scale))
            )
            return decimal.Decimal(decimal_text)

        data = [generate_random_decimal() for _ in range(1000)]

        for d in data:
            assert d == decode_decimal(encode_decimal(d, precision, scale), precision, scale)

        create_table(
            "//tmp/table",
            schema=make_schema([{"name": "key", "type_v3": decimal_type(precision, scale)}])
        )

        write_table("//tmp/table", [{"key": encode_decimal(d, precision, scale)} for d in data])
        sort(in_="//tmp/table", out="//tmp/table", sort_by=["key"])

        actual = [decode_decimal(row["key"], precision, scale) for row in read_table("//tmp/table")]
        expected = list(sorted(data))

        if actual != expected:
            assert len(actual) == len(expected)
            for i in range(len(actual)):
                assert actual[i] == expected[i], "Mismatch on position {}; {} != {} ".format(i, actual[i], expected[i])

    @authors("ermolovd")
    def test_set_old_schema(self):
        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type": "int64",
                        }
                    ],
                    strict=True,
                    unique_keys=False,
                )
            },
        )

        assert get("//tmp/table/@schema/0/type_v3") == optional_type("int64")

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type": "uint8",
                            "required": True,
                        }
                    ],
                    strict=True,
                    unique_keys=False,
                )
            },
        )

        assert get("//tmp/table/@schema/0/type_v3") == "uint8"

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type": "utf8",
                            "required": False,
                        }
                    ],
                    strict=True,
                    unique_keys=False,
                )
            },
        )

        assert get("//tmp/table/@schema/0/type_v3") == optional_type("utf8")

    @authors("ermolovd")
    def test_set_new_schema(self):
        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type_v3": optional_type("int8"),
                        }
                    ],
                    strict=True,
                    unique_keys=False,
                )
            },
        )

        assert get("//tmp/table/@schema/0/type") == "int8"
        assert not get("//tmp/table/@schema/0/required")

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type_v3": "string",
                        }
                    ],
                    strict=True,
                    unique_keys=False,
                )
            },
        )

        assert get("//tmp/table/@schema/0/type") == "string"
        assert get("//tmp/table/@schema/0/required")

    @authors("ermolovd")
    def test_set_both_schemas(self):
        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={"schema": make_schema([{"name": "foo", "type": "uint32", "type_v3": "uint32"}])},
        )

        assert get("//tmp/table/@schema/0/type") == "uint32"
        assert get("//tmp/table/@schema/0/required")
        assert get("//tmp/table/@schema/0/type_v3") == "uint32"

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type": "double",
                            "required": False,
                            "type_v3": optional_type("double"),
                        }
                    ]
                )
            },
        )

        assert get("//tmp/table/@schema/0/type") == "double"
        assert not get("//tmp/table/@schema/0/required")
        assert get("//tmp/table/@schema/0/type_v3") == optional_type("double")

        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "foo",
                            "type": "boolean",
                            "required": True,
                            "type_v3": "bool",
                        }
                    ]
                )
            },
        )

        assert get("//tmp/table/@schema/0/type") == "boolean"
        assert get("//tmp/table/@schema/0/required")
        assert get("//tmp/table/@schema/0/type_v3") == "bool"

        with raises_yt_error("Error validating column"):
            create(
                "table",
                "//tmp/table",
                force=True,
                attributes={
                    "schema": make_schema(
                        [
                            {
                                "name": "foo",
                                "type": "double",
                                "type_v3": "string",
                            }
                        ]
                    )
                },
            )

    @authors("ermolovd")
    def test_complex_types_alter(self):
        create(
            "table",
            "//table",
            attributes={
                "schema": make_schema(
                    [
                        make_column("column", list_type("int64")),
                    ]
                )
            },
        )
        tx_write_table("//table", [{"column": []}])

        with raises_yt_error("Cannot insert a new required column"):
            alter_table(
                "//table",
                schema=make_schema(
                    [
                        make_column("column", list_type("int64")),
                        make_column("column2", list_type("int64")),
                    ]
                ),
            )

        alter_table(
            "//table",
            schema=make_schema(
                [
                    make_column("column", list_type("int64")),
                    make_column("column2", optional_type(list_type("int64"))),
                ]
            ),
        )

    @authors("ermolovd")
    def test_infer_tagged_schema(self):
        table = "//tmp/input1"
        create(
            "table",
            table,
            attributes={
                "schema": make_schema(
                    [{"name": "value", "type_v3": tagged_type("some-tag", "string")}],
                    unique_keys=False,
                    strict=True,
                )
            },
        )
        table = "//tmp/input2"
        create(
            "table",
            table,
            attributes={
                "schema": make_schema(
                    [
                        {"name": "value", "type_v3": "string"},
                    ],
                    unique_keys=False,
                    strict=True,
                )
            },
        )

        tx_write_table("//tmp/input1", [{"value": "foo"}])
        tx_write_table("//tmp/input2", [{"value": "bar"}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            merge(
                in_=["//tmp/input1", "//tmp/input2"],
                out="//tmp/output",
                mode="unordered",
            )

        merge(
            in_=["//tmp/input1", "//tmp/input2"],
            out="<schema=[{name=value;type_v3=utf8}]>//tmp/output",
            spec={"schema_inference_mode": "from_output"},
            mode="unordered",
        )

    @authors("ermolovd")
    def test_infer_null_void(self):
        table = "//tmp/input1"
        create(
            "table",
            table,
            attributes={
                "schema": make_schema(
                    [
                        {"name": "value", "type_v3": "void"},
                    ],
                    unique_keys=False,
                    strict=True,
                )
            },
        )
        table = "//tmp/input2"
        create(
            "table",
            table,
            attributes={
                "schema": make_schema(
                    [
                        {"name": "value", "type_v3": "null"},
                    ],
                    unique_keys=False,
                    strict=True,
                )
            },
        )

        tx_write_table("//tmp/input1", [{"value": None}])
        tx_write_table("//tmp/input2", [{"value": None}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            merge(
                in_=["//tmp/input1", "//tmp/input2"],
                out="//tmp/output",
                mode="unordered",
            )

        merge(
            in_=["//tmp/input1", "//tmp/input2"],
            out="<schema=[{name=value;type_v3=null}]>//tmp/output",
            spec={"schema_inference_mode": "from_output"},
            mode="unordered",
        )


class TestLogicalType(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    @authors("ermolovd")
    @pytest.mark.parametrize("table_type", ["static", "dynamic"])
    def test_logical_types(self, table_type):
        type_list = [
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
            "date",
            "datetime",
            "timestamp",
            "interval",
            "float",
            "json",
        ]

        dynamic = table_type == "dynamic"
        if dynamic:
            sync_create_cells(1)
        else:
            type_list.append("null")

        type_tester = TypeTester(
            type_list,
            dynamic=dynamic,
        )

        type_tester.check_good_value("int8", 2 ** 7 - 1)
        type_tester.check_good_value("int8", 0)
        type_tester.check_good_value("int8", -(2 ** 7))
        type_tester.check_bad_value("int8", 2 ** 7)
        type_tester.check_bad_value("int8", -(2 ** 7) - 1)

        type_tester.check_good_value("int16", 2 ** 15 - 1)
        type_tester.check_good_value("int16", 0)
        type_tester.check_good_value("int16", -(2 ** 15))
        type_tester.check_bad_value("int16", 2 ** 15)
        type_tester.check_bad_value("int16", -(2 ** 15) - 1)

        type_tester.check_good_value("int32", 2 ** 31 - 1)
        type_tester.check_good_value("int32", 0)
        type_tester.check_good_value("int32", -(2 ** 31))
        type_tester.check_bad_value("int32", 2 ** 31)
        type_tester.check_bad_value("int32", -(2 ** 31) - 1)

        type_tester.check_good_value("int64", 2 ** 63 - 1)
        type_tester.check_good_value("int64", 0)
        type_tester.check_good_value("int64", -(2 ** 63))
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
        type_tester.check_bad_value("utf8", b"\xFF")
        type_tester.check_bad_value("utf8", 1)

        type_tester.check_good_value("string", "ff")
        type_tester.check_good_value("string", "ЫТЬ")
        type_tester.check_good_value("string", b"\xFF")
        type_tester.check_bad_value("string", 1)

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

        type_tester.check_good_value("float", -3.14)
        type_tester.check_good_value("float", -3.14e35)
        type_tester.check_good_value("float", 3.14e35)
        type_tester.check_good_value("float", float("inf"))
        type_tester.check_good_value("float", float("-inf"))
        type_tester.check_good_value("float", float("nan"))

        type_tester.check_bad_value("float", 3.14e135)
        type_tester.check_bad_value("float", -3.14e135)

        type_tester.check_good_value("json", "null")
        type_tester.check_good_value("json", "true")
        type_tester.check_good_value("json", "false")
        type_tester.check_good_value("json", "3.25")
        type_tester.check_good_value("json", "3.25e14")
        type_tester.check_good_value("json", '"x"')
        type_tester.check_good_value("json", '{"x": "y"}')
        type_tester.check_good_value("json", '[2, "foo", null, {}]')
        type_tester.check_bad_value("json", "without_quoutes")
        type_tester.check_bad_value("json", "False")
        type_tester.check_bad_value("json", "}{")
        type_tester.check_bad_value("json", '{3: "wrong key type"}')
        type_tester.check_bad_value("json", b"Non-utf8: \xFF")

        if not dynamic:
            type_tester.check_good_value("null", None)
            type_tester.check_bad_value("null", 0)
            type_tester.check_bad_value("null", False)
            type_tester.check_bad_value("null", "")

    @authors("ermolovd")
    def test_special_float_values(self):
        create(
            "table",
            "//tmp/test-table",
            attributes={
                "schema": [
                    {"name": "value", "type": "float"}
                ]
            },
        )
        write_table("//tmp/test-table", [
            {"value": float("nan")},
            {"value": float("-nan")},
            {"value": float("inf")},
            {"value": float("+inf")},
            {"value": float("-inf")},
        ])
        rows = read_table("//tmp/test-table")
        assert [str(r["value"]) for r in rows] == [
            "nan",
            "nan",
            "inf",
            "inf",
            "-inf",
        ]

    @authors("ermolovd")
    def test_bad_alter_table(self):
        def expect_error_alter_table(schema_before, schema_after):
            remove("//tmp/test-alter-table", force=True)
            create("table", "//tmp/test-alter-table", attributes={"schema": schema_before})
            # Make table nonempty, since empty table allows any alter
            tx_write_table("//tmp/test-alter-table", [{}])
            with raises_yt_error(yt_error_codes.IncompatibleSchemas):
                alter_table("//tmp/test-alter-table", schema=schema_after)

        for (source_type, bad_destination_type_list) in [
            ("int8", ["uint64", "uint8", "string"]),
            ("int16", ["uint16", "uint16", "string", "int8"]),
            ("int32", ["uint32", "uint32", "string", "int8", "int16"]),
        ]:
            for destination_type in bad_destination_type_list:
                expect_error_alter_table(
                    [{"name": "column_name", "type": source_type}],
                    [{"name": "column_name", "type": destination_type}],
                )

    @authors("ermolovd")
    def test_logical_type_column_constrains(self):
        remove("//test-table", force=True)
        with raises_yt_error('Computed column "key1" type mismatch: declared type'):
            create(
                "table",
                "//tmp/test-table",
                attributes={
                    "schema": [
                        {"name": "key1", "type": "int32", "expression": "100"},
                    ]
                },
            )

        with raises_yt_error('Aggregated column "key1" is forbidden to have logical type'):
            create(
                "table",
                "//tmp/test-table",
                attributes={
                    "schema": [
                        {"name": "key1", "type": "int32", "aggregate": "sum"},
                    ]
                },
            )


class TestRequiredOption(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    def test_required_static_tables(self):
        create(
            "table",
            "//tmp/required_table",
            attributes={
                "schema": [
                    {
                        "name": "value",
                        "type": "string",
                        "required": True,
                    }
                ],
            },
        )

        tx_write_table("//tmp/required_table", [{"value": "foo"}])
        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/required_table", [{"value": 100500}])
        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/required_table", [{"value": None}])
        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/required_table", [{}])

    @authors("ermolovd")
    def test_required_any_is_disallowed(self):
        with raises_yt_error('Column of type "any" cannot be "required"'):
            create(
                "table",
                "//tmp/required_table",
                attributes={
                    "schema": [
                        {
                            "name": "value",
                            "type": "any",
                            "required": True,
                        }
                    ],
                },
            )
        with raises_yt_error('Column of type "any" cannot be "required"'):
            create(
                "table",
                "//tmp/dynamic_required_table",
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
                        },
                    ],
                },
            )

    @authors("ermolovd")
    def test_alter_required_column(self):
        table = "//tmp/static_table"
        create(
            "table",
            table,
            attributes={
                "schema": [
                    {
                        "name": "column",
                        "type": "string",
                    }
                ],
            },
        )
        tx_write_table(table, [{"column": None}])
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table(
                table,
                schema=[
                    {
                        "name": "column",
                        "type": "string",
                        "required": True,
                    }
                ],
            )
        tx_write_table(table, [{"column": None}])

        create(
            "table",
            table,
            force=True,
            attributes={
                "schema": [
                    {
                        "name": "column",
                        "type": "string",
                        "required": True,
                    }
                ],
            },
        )
        tx_write_table(table, [{"column": "foo"}])

        # No exception.
        alter_table(
            table,
            schema=[
                {
                    "name": "column",
                    "type": "string",
                }
            ],
        )

        create(
            "table",
            table,
            force=True,
            attributes={
                "schema": [
                    {
                        "name": "column1",
                        "type": "string",
                    }
                ],
            },
        )
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
                    },
                ],
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("sorted_table", [False, True])
    def test_infer_required_column(self, sorted_table):
        if sorted_table:
            schema = make_schema(
                [
                    {
                        "name": "key",
                        "type": "string",
                        "required": False,
                        "sort_order": "ascending",
                    },
                    {"name": "value", "type": "string", "required": True},
                ],
                unique_keys=False,
                strict=True,
            )
        else:
            schema = make_schema(
                [
                    {"name": "key", "type": "string", "required": False},
                    {"name": "value", "type": "string", "required": True},
                ],
                unique_keys=False,
                strict=True,
            )
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
        create(
            "table",
            table,
            attributes={"schema": make_schema([make_column("value", "string")], unique_keys=False, strict=True)},
        )
        table = "//tmp/input2"
        create(
            "table",
            table,
            attributes={
                "schema": make_schema(
                    [make_column("value", optional_type("string"))],
                    unique_keys=False,
                    strict=True,
                )
            },
        )

        tx_write_table("//tmp/input1", [{"value": "foo"}])
        tx_write_table("//tmp/input2", [{"value": "bar"}])

        create("table", "//tmp/output")

        with raises_yt_error("tables have incompatible schemas"):
            # Schemas are incompatible
            merge(
                in_=["//tmp/input1", "//tmp/input2"],
                out="//tmp/output",
                mode="unordered",
            )

    @authors("ifsmirnov")
    def test_required_columns_in_dynamic_tables_schema(self):
        schema = [
            {
                "name": "key_req",
                "type": "int64",
                "sort_order": "ascending",
                "required": True,
            },
            {"name": "key_opt", "type": "int64", "sort_order": "ascending"},
            {"name": "value_req", "type": "string", "required": True},
            {"name": "value_opt", "type": "string"},
        ]

        sync_create_cells(1)
        create("table", "//tmp/t", attributes={"schema": schema, "dynamic": True})

        sync_mount_table("//tmp/t")
        insert_rows(
            "//tmp/t",
            [{"key_req": 1, "key_opt": 2, "value_req": "x", "value_opt": "y"}],
        )
        sync_unmount_table("//tmp/t")

        # Required columns cannot be added
        with raises_yt_error("Cannot insert a new required column"):
            alter_table(
                "//tmp/t",
                schema=schema + [{"name": "value3_req", "type": "string", "required": True}],
            )

        # Adding non-required columns is OK
        schema += [{"name": "value3_opt", "type": "string", "required": False}]
        alter_table("//tmp/t", schema=schema)

        # Old column cannot become required
        bad_schema = [i.copy() for i in schema]
        bad_schema[3]["required"] = True
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table("//tmp/t", schema=bad_schema)

        # Removing 'required' attribute is OK
        good_schema = [i.copy() for i in schema]
        good_schema[2]["required"] = False
        alter_table("//tmp/t", schema=good_schema)


class TestSchemaDeduplication(YTEnvSetup):
    def _get_schema(self, strict):
        return make_schema([make_column("value", "string")], unique_keys=False, strict=strict)

    @authors("ermolovd", "shakurov")
    def test_empty_schema(self):
        create("table", "//tmp/table")
        # 1 from the table, 1 from self-ref, 1 from event_log.
        assert get("//tmp/table/@schema_duplicate_count") == 3

    @authors("ermolovd")
    def test_simple_schema(self):
        create("table", "//tmp/table1", attributes={"schema": self._get_schema(True)})
        create("table", "//tmp/table2", attributes={"schema": self._get_schema(True)})
        create("table", "//tmp/table3", attributes={"schema": self._get_schema(False)})

        assert get("//tmp/table1/@schema_duplicate_count") == 2
        assert get("//tmp/table2/@schema_duplicate_count") == 2
        assert get("//tmp/table3/@schema_duplicate_count") == 1

        alter_table("//tmp/table2", schema=self._get_schema(False))

        assert get("//tmp/table1/@schema_duplicate_count") == 1
        assert get("//tmp/table2/@schema_duplicate_count") == 2
        assert get("//tmp/table3/@schema_duplicate_count") == 2

    @authors("h0pless")
    def test_alter_by_schema_id(self):
        create("table", "//tmp/schema_holder", attributes={"schema": self._get_schema(True)})
        create("table", "//tmp/table", attributes={"schema": self._get_schema(False)})

        schema_id = get("//tmp/schema_holder/@schema_id")
        alter_table("//tmp/table", schema_id=schema_id)

        assert get("//tmp/table/@schema_id") == schema_id
        assert get("//tmp/table/@schema") == get("//tmp/schema_holder/@schema")


class TestSchemaDeduplicationRpcProxy(TestSchemaDeduplication):
    NUM_RPC_PROXIES = 1


class TestSchemaObjects(TestSchemaDeduplication):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("shakurov", "h0pless")
    def test_schema_map(self):
        create("table", "//tmp/empty_schema_holder", attributes={"external_cell_tag": 11})
        empty_schema_0_id = get("//tmp/empty_schema_holder/@schema_id")
        table_0_id = get("//tmp/empty_schema_holder/@id")

        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p1/empty_schema_holder", attributes={"external_cell_tag": 12})
        empty_schema_1_id = get("//tmp/p1/empty_schema_holder/@schema_id")
        table_1_id = get("//tmp/p1/empty_schema_holder/@id")

        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})
        create("table", "//tmp/p2/empty_schema_holder", attributes={"external": False})
        empty_schema_2_id = get("//tmp/p2/empty_schema_holder/@schema_id")

        assert empty_schema_0_id != empty_schema_1_id
        assert empty_schema_0_id != empty_schema_2_id
        assert empty_schema_1_id != empty_schema_2_id

        assert empty_schema_0_id == get("#{}/@schema_id".format(table_0_id), driver=get_driver(1))
        assert empty_schema_1_id == get("#{}/@schema_id".format(table_1_id), driver=get_driver(2))

        create("table", "//tmp/schema_holder1", attributes={"schema": self._get_schema(True)})
        create("table", "//tmp/schema_holder2", attributes={"schema": self._get_schema(False)})
        schema1_id = get("//tmp/schema_holder1/@schema_id")
        schema2_id = get("//tmp/schema_holder2/@schema_id")
        assert schema1_id != schema2_id

        expected_schemas = {
            empty_schema_0_id,
            empty_schema_1_id,
            empty_schema_2_id,
            schema1_id,
            schema2_id}
        assert len(expected_schemas) == 5  # Validate there're no duplicates.
        actual_schemas = {schema_id for schema_id in ls("//sys/master_table_schemas")}
        for expected_schema_id in expected_schemas:
            assert expected_schema_id in actual_schemas

        remove("//tmp/schema_holder2")
        wait(lambda: not exists("#" + schema2_id))

        expected_schemas = {
            empty_schema_0_id,
            empty_schema_1_id,
            empty_schema_2_id,
            schema1_id}
        actual_schemas = {schema_id for schema_id in ls("//sys/master_table_schemas")}
        for expected_schema_id in expected_schemas:
            assert expected_schema_id in actual_schemas

    @authors("shakurov", "h0pless")
    def test_schema_id(self):
        # @schema only.
        create("table", "//tmp/table1", attributes={"schema": self._get_schema(True)})

        schema = get("//tmp/table1/@schema")
        schema_id = get("//tmp/table1/@schema_id")

        # @schema_id only.
        create("table", "//tmp/table2", attributes={"schema_id": schema_id})
        assert get("//tmp/table2/@schema_id") == schema_id
        assert get("//tmp/table2/@schema") == schema

        # Both @schema and @schema_id.
        create("table", "//tmp/table3", attributes={"schema_id": schema_id, "schema": schema})
        assert get("//tmp/table3/@schema_id") == schema_id
        assert get("//tmp/table3/@schema") == schema

        # Invalid @schema_id only.
        with raises_yt_error("No such schema"):
            create("table", "//tmp/table4", attributes={"schema_id": "a-b-c-d"})

        # @schema and invalid @schema_id.
        with raises_yt_error("No such schema"):
            create("table", "//tmp/table5", attributes={"schema_id": "a-b-c-d", "schema": schema})

        other_schema = make_schema([make_column("some_column", "int8")], unique_keys=False, strict=True)

        # Hitherto-unseen @schema and a mismatching @schema_id.
        with raises_yt_error("Both \"schema\" and \"schema_id\" specified and the schemas do not match"):
            create("table", "//tmp/table6", attributes={"schema_id": schema_id, "schema": other_schema})

        create("table", "//tmp/other_schema_holder", attributes={"schema": other_schema})

        # @schema and a mismatching @schema_id.
        with raises_yt_error("Both \"schema\" and \"schema_id\" specified and they refer to different schemas"):
            create("table", "//tmp/table7", attributes={"schema_id": schema_id, "schema": other_schema})

        assert get("#" + schema_id + "/@ref_counter") == 3
        assert get("//tmp/table1/@schema_duplicate_count") == 3
        assert get("//tmp/table2/@schema_duplicate_count") == 3
        assert get("//tmp/table3/@schema_duplicate_count") == 3

    @authors("shakurov", "h0pless")
    def test_create_with_schema(self):
        create("table", "//tmp/table1", attributes={"schema": self._get_schema(True), "external_cell_tag": 11})
        table_id = get("//tmp/table1/@id")
        schema_id = get("//tmp/table1/@schema_id")
        external_schema_id = get("#" + table_id + "/@schema_id", driver=get_driver(1))
        assert schema_id == external_schema_id
        schema = get("//tmp/table1/@schema")
        external_schema = get("#" + table_id + "/@schema", driver=get_driver(1))
        assert schema == external_schema

    @authors("shakurov", "h0pless")
    def test_create_with_schema_id(self):
        # Just to have a pre-existing schema to refer to by ID.
        create("table", "//tmp/schema_holder", attributes={"schema": self._get_schema(True)})
        schema_id = get("//tmp/schema_holder/@schema_id")

        create("table", "//tmp/table1", attributes={"schema_id": schema_id, "external_cell_tag": 11})
        table_id = get("//tmp/table1/@id")
        assert get("//tmp/table1/@schema_id") == schema_id
        external_schema_id = get("#" + table_id + "/@schema_id", driver=get_driver(1))
        assert schema_id == external_schema_id
        schema = get("//tmp/table1/@schema")
        external_schema = get("#" + table_id + "/@schema", driver=get_driver(1))
        assert schema == external_schema

    @authors("shakurov", "h0pless")
    @pytest.mark.parametrize("cross_shard", [False, True])
    def test_copy_with_schema(self, cross_shard):
        create("table", "//tmp/table1", attributes={"schema": self._get_schema(True), "external_cell_tag": 11})
        if cross_shard:
            create("portal_entrance", "//tmp/d", attributes={"exit_cell_tag": 12})
        else:
            create("map_node", "//tmp/d")

        copy("//tmp/table1", "//tmp/d/table1_copy")

        src_table_id = get("//tmp/table1/@id")
        dst_table_id = get("//tmp/d/table1_copy/@id")

        src_schema = get("//tmp/table1/@schema")
        src_schema_id = get("//tmp/table1/@schema_id")
        external_src_schema = get("#" + src_table_id + "/@schema", driver=get_driver(1))
        external_src_schema_id = get("#" + src_table_id + "/@schema_id", driver=get_driver(1))
        dst_schema = get("//tmp/d/table1_copy/@schema")
        dst_schema_id = get("//tmp/d/table1_copy/@schema_id")
        external_dst_schema = get("#" + dst_table_id + "/@schema", driver=get_driver(1))
        external_dst_schema_id = get("#" + dst_table_id + "/@schema_id", driver=get_driver(1))

        # All schemas are identical.
        assert src_schema == external_src_schema
        assert src_schema == dst_schema
        assert src_schema == external_dst_schema

        # Native and external schemas differ when native cells are different.
        if cross_shard:
            assert src_schema_id != dst_schema_id
            assert src_schema_id != external_dst_schema_id
            assert dst_schema_id != external_src_schema_id
            assert external_src_schema_id != external_dst_schema_id
        else:
            assert src_schema_id == dst_schema_id
            assert src_schema_id == external_dst_schema_id
            assert dst_schema_id == external_src_schema_id
            assert external_src_schema_id == external_dst_schema_id

        # External schemas alwsays have same ids as native ones.
        assert src_schema_id == external_src_schema_id
        assert dst_schema_id == external_dst_schema_id

    @authors("cookiedoth")
    def test_schema_get(self):
        create("table", "//tmp/schema_holder", attributes={"schema": self._get_schema(True)})
        schema_id = get("//tmp/schema_holder/@schema_id")

        schema = get("//tmp/schema_holder/@schema")
        assert get("#{}".format(schema_id)) == schema
        assert get("#{}/@value".format(schema_id)) == schema
        with pytest.raises(YtError):
            set("#{}/@value".format(schema_id), self._get_schema(True))
        with pytest.raises(YtError):
            set("#{}".format(schema_id), self._get_schema(True))
        with pytest.raises(YtError):
            remove("#{}/@value".format(schema_id))

    @authors("h0pless")
    def test_schema_export_ref_counters(self):
        schema = self._get_schema(True)
        create("table", "//tmp/table_non_external", attributes={"external": False, "schema": schema})
        schema_id = get("//tmp/table_non_external/@schema_id")
        assert get("#{}/@export_ref_counter".format(schema_id)) == {}

        create("table", "//tmp/table_11", attributes={"external_cell_tag": 11, "schema": schema})
        assert get("#{}/@export_ref_counter".format(schema_id)) == {"11": 1}

        create("table", "//tmp/table_12_1", attributes={"external_cell_tag": 12, "schema": schema})
        assert get("#{}/@export_ref_counter".format(schema_id)) == {"11": 1, "12": 1}

        create("table", "//tmp/table_12_2", attributes={"external_cell_tag": 12, "schema": schema})
        assert get("#{}/@export_ref_counter".format(schema_id)) == {"11": 1, "12": 2}

        remove("//tmp/table_11")
        remove("//tmp/table_12_1")
        remove("//tmp/table_12_2")
        wait(lambda: get("#{}/@export_ref_counter".format(schema_id)) == {})


class TestSchemaValidation(YTEnvSetup):
    @authors("ermolovd")
    def test_schema_complexity(self):
        def make_schema(size):
            return [{"name": "column{}".format(i), "type": "int64"} for i in range(size)]

        def make_row(size):
            return {"column{}".format(i): i for i in range(size)}

        bad_size = 32 * 1024
        with raises_yt_error("Table schema is too complex"):
            create(
                "table",
                "//tmp/bad-schema-1",
                attributes={"schema": make_schema(bad_size)},
            )

        create("table", "//tmp/bad-schema-2")
        with raises_yt_error("Too many columns in row"):
            tx_write_table("//tmp/bad-schema-2", [make_row(bad_size)])

        ok_size = bad_size - 1
        create("table", "//tmp/ok-schema", attributes={"schema": make_schema(ok_size)})
        tx_write_table("//tmp/ok-schema", [make_row(ok_size)])

    @authors("levysotsky")
    def test_is_comparable(self):
        def check_comparable(type):
            schema = [{"name": "column", "type_v3": type, "sort_order": "ascending"}]
            create("table", "//tmp/t", attributes={"schema": schema})
            remove("//tmp/t")

        def check_not_comparable(type):
            with raises_yt_error("Key column cannot be of"):
                check_comparable(type)

        for t in [
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
            optional_type("yson"),
            "float",
            list_type(optional_type("int32")),
            tuple_type([list_type("date"), optional_type("datetime")]),
        ]:
            check_comparable(t)

        for t in [
            "json",
            struct_type(
                [
                    ("a", "int64"),
                    ("b", "string"),
                ]
            ),
            tuple_type(["json", "int8"]),
            tuple_type([optional_type("yson")]),
        ]:
            check_not_comparable(t)

    @authors("levysotsky")
    def test_stable_name(self):
        skip_if_renaming_disabled(self.Env)

        schema = make_schema([
            make_column("a", "int64", stable_name="a_stable", sort_order="ascending"),
            make_column("b", "int64"),
            make_column("c", "int64", stable_name="c_stable"),
        ])
        create("table", "//tmp/t", attributes={"schema": schema})
        read_schema = get("//tmp/t/@schema")
        assert read_schema[0]["name"] == "a"
        assert read_schema[0]["type_v3"] == "int64"
        assert read_schema[0]["stable_name"] == "a_stable"

        assert read_schema[1]["name"] == "b"
        assert read_schema[1]["type_v3"] == "int64"
        assert "stable_name" not in read_schema[1]

        assert read_schema[2]["name"] == "c"
        assert read_schema[2]["type_v3"] == "int64"
        assert read_schema[2]["stable_name"] == "c_stable"

        remove("//tmp/t")

        def check_schema(schema):
            create("table", "//tmp/t", attributes={"schema": schema})
            remove("//tmp/t")

        with raises_yt_error("empty"):
            schema = make_schema([make_column("a", "int64", stable_name="")])
            check_schema(schema)


@authors("ermolovd")
class TestErrorCodes(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    def test_YT_11522_missing_column(self):
        schema = [
            {
                "name": "foo",
                "type": "int64",
                "sort_order": "ascending",
                "required": True,
            },
            {"name": "bar", "type": "int64"},
        ]

        sync_create_cells(1)
        create("table", "//tmp/t", attributes={"schema": schema, "dynamic": True})

        sync_mount_table("//tmp/t")
        with raises_yt_error(yt_error_codes.SchemaViolation):
            insert_rows("//tmp/t", [{"baz": 1}])
        sync_unmount_table("//tmp/t")

    def test_YT_11522_conversion_error(self):
        schema = [{"name": "foo", "type": "uint64"}]

        create("table", "//tmp/t", attributes={"schema": schema})

        with raises_yt_error(yt_error_codes.SchemaViolation):
            tx_write_table("//tmp/t", [{"foo": -1}])


@authors("ermolovd")
class TestAlterTable(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    _TABLE_PATH = "//tmp/test-alter-table"

    def get_default_value_for_type(self, type_v3):
        if isinstance(type_v3, str):
            if type_v3.startswith("int"):
                return 0
            elif type_v3.startswith("uint"):
                return yson.YsonUint64(0)
            elif type_v3 == "bool":
                return False
            elif type_v3 in ["string", "utf8"]:
                return ""
            elif type_v3 == "null":
                return None
            raise ValueError("Type {} is not supported".format(type_v3))
        type_name = type_v3["type_name"]
        if type_name == "optional":
            return None
        elif type_name == "list":
            return []
        elif type_name == "tuple":
            return [
                self.get_default_value_for_type(t["type"]) for t in type_v3["elements"]
            ]
        elif type_name == "struct":
            return {
                m["name"]: self.get_default_value_for_type(m["type"]) for m in type_v3["members"]
            }
        elif type_name == "variant":
            if "members" in type_v3:
                member = type_v3["members"][0]
                return [member["name"], self.get_default_value_for_type(member["type"])]
            elif "elements" in type_v3:
                return [0, self.get_default_value_for_type(type_v3["elements"][0]["type"])]
            raise ValueError("Unknown kind of variant: {}".format(type_v3))
        elif type_name == "dict":
            return []
        elif type_name == "tagged":
            return self.get_default_value_for_type(type_v3["item"])
        raise ValueError("Type {} is not supported".format(type_name))

    def get_default_row(self, schema):
        return {
            column["name"]: self.get_default_value_for_type(column["type_v3"])
            for column in schema
        }

    def _create_test_schema_with_type(self, type_v3):
        # Schema is compatible with both static and dynamic tables
        return make_schema([
            make_column("key", "int64", sort_order="ascending"),
            make_column("value", type_v3),
        ], unique_keys=True, strict=True)

    def prepare_table(self, schema, dynamic=False):
        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema,
        })
        write_table(self._TABLE_PATH, [self.get_default_row(schema)])
        if dynamic:
            alter_table(self._TABLE_PATH, dynamic=True)

    def check_table_readable(self, create_schema, dynamic):
        expected_data = remove_entity([self.get_default_row(create_schema)])

        assert remove_entity(read_table(self._TABLE_PATH)) == expected_data

        if dynamic:
            sync_mount_table(self._TABLE_PATH)
            assert remove_entity(list(lookup_rows(self._TABLE_PATH, [{"key": 0}]))) == expected_data
            sync_unmount_table(self._TABLE_PATH)

    def check_both_ways_alter_type(self, old_type_v3, new_type_v3, dynamic=False):
        old_schema = self._create_test_schema_with_type(old_type_v3)
        new_schema = self._create_test_schema_with_type(new_type_v3)
        self.prepare_table(old_schema, dynamic=dynamic)

        alter_table(self._TABLE_PATH, schema=new_schema)
        # Check that table is still readable after alter.
        self.check_table_readable(old_schema, dynamic)

        alter_table(self._TABLE_PATH, schema=old_schema)
        self.check_table_readable(old_schema, dynamic)

    def check_one_way_alter_type(self, old_type_v3, new_type_v3, dynamic=False):
        """
        Check that we can alter column type from old_type_v3 to new_type_v3 but cannot alter it back.
        """
        old_schema = self._create_test_schema_with_type(old_type_v3)
        new_schema = self._create_test_schema_with_type(new_type_v3)
        self.prepare_table(old_schema, dynamic=dynamic)

        alter_table(self._TABLE_PATH, schema=new_schema)
        # Check that table is still readable after alter.
        self.check_table_readable(old_schema, dynamic)

        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table(self._TABLE_PATH, schema=old_schema)

    def check_bad_alter_type(self, old_type_v3, new_type_v3, dynamic=False):
        """
        Check that we can alter column type from old_type_v3 to new_type_v3 but cannot alter it back.
        """
        old_schema = self._create_test_schema_with_type(old_type_v3)
        new_schema = self._create_test_schema_with_type(new_type_v3)
        self.prepare_table(old_schema, dynamic=dynamic)

        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table(self._TABLE_PATH, schema=new_schema)

    def check_bad_both_way_alter_type(self, lhs_type_v3, rhs_type_v3, dynamic=False):
        self.check_bad_alter_type(lhs_type_v3, rhs_type_v3, dynamic)
        self.check_bad_alter_type(rhs_type_v3, lhs_type_v3, dynamic)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_add_column(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": 0, "b": "foo"}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", optional_type("string")),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

    @authors("ermolovd", "dakovalkov")
    @pytest.mark.parametrize("dynamic", [False, True])
    def test_alter_simple_types(self, dynamic):
        if dynamic:
            sync_create_cells(1)

        self.check_one_way_alter_type("int8", "int16", dynamic=dynamic)
        self.check_one_way_alter_type("int8", "int32", dynamic=dynamic)
        self.check_one_way_alter_type("int8", "int64", dynamic=dynamic)

        self.check_one_way_alter_type("uint8", "uint16", dynamic=dynamic)
        self.check_one_way_alter_type("uint8", "uint32", dynamic=dynamic)
        self.check_one_way_alter_type("uint8", "uint64", dynamic=dynamic)

        self.check_one_way_alter_type("utf8", "string", dynamic=dynamic)

        self.check_one_way_alter_type("int64", optional_type("int64"), dynamic=dynamic)
        self.check_one_way_alter_type("int8", optional_type("int64"), dynamic=dynamic)
        self.check_one_way_alter_type("null", optional_type("null"), dynamic=dynamic)

        self.check_one_way_alter_type("int64", optional_type("yson"), dynamic=dynamic)

        self.check_bad_both_way_alter_type("uint8", "int64", dynamic=dynamic)
        # TODO(dakovalkov): can be supported.
        self.check_bad_both_way_alter_type("null", optional_type("int64"), dynamic=dynamic)

    @authors("ermolovd", "dakovalkov")
    @pytest.mark.parametrize("dynamic", [False, True])
    def test_alter_composite_types(self, dynamic):
        if dynamic:
            sync_create_cells(1)

        # List
        self.check_one_way_alter_type(
            list_type("int64"),
            optional_type(list_type("int64")),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            list_type("int64"),
            list_type(optional_type("int64")),
            dynamic=dynamic)

        self.check_bad_both_way_alter_type(
            list_type("int64"),
            optional_type(optional_type(list_type("int64"))),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            list_type("int64"),
            optional_type("yson"),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            optional_type("yson"),
            list_type("int64"),
            dynamic=dynamic)

        # Tuple
        self.check_one_way_alter_type(
            tuple_type(["int32"]),
            tuple_type(["int64"]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            tuple_type(["int64"]),
            tuple_type([optional_type("int64")]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            tuple_type(["int32"]),
            tuple_type([optional_type("int64")]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            tuple_type(["int64"]),
            tuple_type(["int64", "int64"]),
            dynamic=dynamic)
        # TODO(dakovalkov): can be supported.
        self.check_bad_both_way_alter_type(
            tuple_type(["int64"]),
            optional_type("yson"),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            optional_type("yson"),
            tuple_type(["int64"]),
            dynamic=dynamic)

        # Struct
        self.check_one_way_alter_type(
            struct_type([("a", "int32")]),
            struct_type([("a", "int64")]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("a", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            struct_type([("a", "int32")]),
            struct_type([("a", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("a", optional_type(optional_type("int64")))]),
            dynamic=dynamic)

        self.check_one_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("a", "int64"), ("b", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("a", "int64"), ("b", optional_type(optional_type("int64")))]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("a", "int64"), ("b", "int64")]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            struct_type([("a", "int64")]),
            struct_type([("b", optional_type("int64")), ("a", "int64")]),
            dynamic=dynamic)

        self.check_bad_both_way_alter_type(
            struct_type([("a", "int64")]),
            optional_type("yson"),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            optional_type("yson"),
            struct_type([("a", "int64")]),
            dynamic=dynamic)

        # Variant over tuple
        self.check_one_way_alter_type(
            variant_tuple_type(["int32"]),
            variant_tuple_type(["int64"]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_tuple_type(["int64"]),
            variant_tuple_type([optional_type("int64")]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_tuple_type(["int32"]),
            variant_tuple_type([optional_type("int64")]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_tuple_type(["int64"]),
            variant_tuple_type(["int64", "int64"]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_tuple_type(["int64"]),
            variant_tuple_type(["int64", optional_type("int64")]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            variant_tuple_type(["int64"]),
            optional_type("yson"),
            dynamic=dynamic)

        # Variant over struct
        self.check_one_way_alter_type(
            variant_struct_type([("a", "int32")]),
            variant_struct_type([("a", "int64")]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("a", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_struct_type([("a", "int32")]),
            variant_struct_type([("a", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("a", optional_type(optional_type("int64")))]),
            dynamic=dynamic)

        self.check_one_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("a", "int64"), ("b", optional_type("int64"))]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("a", "int64"), ("b", optional_type(optional_type("int64")))]),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("a", "int64"), ("b", "int64")]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            variant_struct_type([("a", "int64")]),
            variant_struct_type([("b", optional_type("int64")), ("a", "int64")]),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            variant_struct_type([("a", "int64")]),
            optional_type("yson"),
            dynamic=dynamic)

        # Dict
        self.check_one_way_alter_type(
            dict_type("utf8", "int8"),
            dict_type("utf8", optional_type("int8")),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            dict_type("utf8", "int8"),
            dict_type(optional_type("utf8"), "int8"),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            dict_type("utf8", "int8"),
            dict_type(optional_type("string"), "int8"),
            dynamic=dynamic)
        self.check_one_way_alter_type(
            dict_type("utf8", "int8"),
            dict_type("string", optional_type("int64")),
            dynamic=dynamic)

        self.check_bad_both_way_alter_type(
            dict_type("utf8", "int8"),
            optional_type("yson"),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            dict_type("utf8", "uint8"),
            dict_type("utf8", "int64"),
            dynamic=dynamic)
        self.check_bad_both_way_alter_type(
            dict_type("utf8", "uint8"),
            dict_type("int8", "uint8"),
            dynamic=dynamic)

        # Tagged
        self.check_both_ways_alter_type(
            optional_type("int8"),
            tagged_type("foo", optional_type("int8")),
            dynamic=dynamic)
        self.check_both_ways_alter_type(
            optional_type("int8"),
            optional_type(tagged_type("foo", "int8")),
            dynamic=dynamic)
        self.check_both_ways_alter_type(
            optional_type("int8"),
            tagged_type("bar", optional_type(tagged_type("foo", "int8"))),
            dynamic=dynamic)
        self.check_both_ways_alter_type(
            tagged_type("qux", optional_type("int8")),
            tagged_type("bar", optional_type(tagged_type("foo", "int8"))),
            dynamic=dynamic)


class TestSchemaDepthLimit(YTEnvSetup):
    YSON_DEPTH_LIMIT = 256

    # Keep consistent with code.
    SCHEMA_DEPTH_LIMIT = 32
    VERY_LARGE_DEPTH = 60

    DELTA_DRIVER_CONFIG = {
        "cypress_write_yson_nesting_level_limit": YSON_DEPTH_LIMIT,
    }

    def _create_schema(self, depth):
        cur_type = "int64"
        for _ in range(depth):
            cur_type = struct_type([("a", cur_type)])
        return make_schema([make_column("column", cur_type)])

    @authors("levysotsky")
    def test_depth_limit(self):
        good_schema = self._create_schema(self.SCHEMA_DEPTH_LIMIT - 1)
        create("table", "//tmp/t1", force=True, attributes={
            "schema": good_schema,
        })

        bad_schema = self._create_schema(self.SCHEMA_DEPTH_LIMIT + 1)
        with raises_yt_error("depth limit"):
            create("table", "//tmp/t2", force=True, attributes={
                "schema": bad_schema,
            })

        bad_schema = self._create_schema(self.VERY_LARGE_DEPTH)
        # No crash here.
        with raises_yt_error("depth limit"):
            create("table", "//tmp/t3", force=True, attributes={
                "schema": bad_schema,
            })


class TestRenameColumnsStatic(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    ENABLE_DYNAMIC_TABLE_COLUMN_RENAMES = False
    NUM_SCHEDULERS = 1

    _TABLE_PATH = "//tmp/test-alter-table"

    def create_default_schema(self):
        return make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

    def create_and_populate_table(self, path, optimize_for, schema1, index):
        create("table", path, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows1 = [{"a": index, "b": "foo", "c": True}]

        write_table(path, rows1)
        assert read_table(path) == rows1
        return rows1

    def rename_columns(self, path, index):
        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("b_new", "string", stable_name="b"),
        ], unique_keys=True, strict=True)

        alter_table(path, schema=schema2)

        rows2 = [{"a": index, "b_new": "foo", "c_new": True}]
        assert read_table(path) == rows2

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_static(self, optimize_for):
        schema1 = self.create_default_schema()
        rows1 = self.create_and_populate_table(self._TABLE_PATH, optimize_for, schema1, 0)
        self.rename_columns(self._TABLE_PATH, 0)

        table_path_with_filter = "<columns=[a;b_new]>" + self._TABLE_PATH
        rows2_filtered = [{"a": 0, "b_new": "foo"}]
        assert read_table(table_path_with_filter) == rows2_filtered

        alter_table(self._TABLE_PATH, schema=schema1)

        assert read_table(self._TABLE_PATH) == rows1

        table_path_with_filter = "<columns=[a;c]>" + self._TABLE_PATH
        rows1_filtered = [{"a": 0, "c": True}]
        assert read_table(table_path_with_filter) == rows1_filtered

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_no_teleportation(self, optimize_for):
        in1 = "//tmp/in1"
        in2 = "//tmp/in2"

        schema1 = self.create_default_schema()
        self.create_and_populate_table(in1, optimize_for, schema1, 0)
        self.create_and_populate_table(in2, optimize_for, schema1, 1)

        m1 = "//tmp/m1"
        create("table", m1)
        merge(in_=[in1, in2], out=m1)

        in1_chunks = get(in1 + "/@chunk_ids")
        in2_chunks = get(in2 + "/@chunk_ids")
        m1_chunks = get(m1 + "/@chunk_ids")

        assert m1_chunks == in1_chunks + in2_chunks

        self.rename_columns(in1, 0)
        self.rename_columns(in2, 1)

        m2 = "//tmp/m2"
        create("table", m2)
        merge(in_=[in1, in2], out=m2)

        m2_schema = get(m2 + "/@schema")
        m2_chunks = get(m2 + "/@chunk_ids")

        assert 'stable_name' not in m2_schema[1]
        assert 'stable_name' not in m2_schema[2]
        assert not builtins.set(m2_chunks).intersection(builtins.set(in1_chunks + in2_chunks))

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_static_several_chunks(self, optimize_for):
        table_path_with_append = "<append=%true>" + self._TABLE_PATH

        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows1 = [{"a": 0, "b": "foo", "c": True}]
        write_table(self._TABLE_PATH, rows1)
        assert read_table(self._TABLE_PATH) == rows1

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("b_new", "string", stable_name="b"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema2)

        rows2 = [{"a": 0, "b_new": "foo", "c_new": True}]
        assert read_table(self._TABLE_PATH) == rows2

        write_table(table_path_with_append, [{"a": 1, "b_new": "bar", "c_new": True}])

        rows2_new = rows2 + [{"a": 1, "b_new": "bar", "c_new": True}]
        assert read_table(self._TABLE_PATH) == rows2_new

        schema3 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("d", type_v3=optional_type("int64")),
            make_column("b_newer", "string", stable_name="b"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema3)

        rows3 = [
            {"a": 0, "b_newer": "foo", "c_new": True},
            {"a": 1, "b_newer": "bar", "c_new": True},
        ]
        assert read_table(self._TABLE_PATH) == rows3

        write_table(table_path_with_append, [{"a": 22, "b_newer": "booh", "c_new": False, "d": -12}])

        rows3_new = [
            {"a": 0, "b_newer": "foo", "c_new": True},
            {"a": 1, "b_newer": "bar", "c_new": True},
            {"a": 22, "b_newer": "booh", "c_new": False, "d": -12},
        ]
        assert read_table(self._TABLE_PATH) == rows3_new

        schema4 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string", stable_name="b"),
            make_column("c", "bool", stable_name="c"),
            make_column("d_new", type_v3=optional_type("int64"), stable_name="d"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema4)

        rows4 = [
            {"a": 0, "b": "foo", "c": True},
            {"a": 1, "b": "bar", "c": True},
            {"a": 22, "b": "booh", "c": False, "d_new": -12},
        ]
        assert read_table(self._TABLE_PATH) == rows4

    @authors("orlovorlov")
    def test_rename_dynamic_disabled(self):
        skip_if_renaming_not_differentiated(self.Env)

        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": "scan",
        })

        rows1 = [{"a": 0, "b": "blah", "c": False}]

        write_table(self._TABLE_PATH, rows1)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("cc", "bool", stable_name="c"),
        ], unique_keys=True, strict=True)

        with raises_yt_error("Table column renaming is not available yet"):
            alter_table(self._TABLE_PATH, schema=schema2)

    @authors("orlovorlov")
    def test_rename_static_change_to_dynamic(self):
        skip_if_renaming_not_differentiated(self.Env)

        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": "scan",
        })

        rows1 = [{"a": 0, "b": "blah", "c": False}]

        write_table(self._TABLE_PATH, rows1)
        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("cc", "bool", stable_name="c"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2)

        sync_create_cells(1)
        with raises_yt_error("Table column renaming is not available yet"):
            alter_table(self._TABLE_PATH, dynamic=True)


class TestRenameColumnsDynamic(YTEnvSetup):
    _TABLE_PATH = "//tmp/test-alter-table"
    USE_DYNAMIC_TABLES = True

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_sorted_dynamic(self, optimize_for):
        def sorted_by_a(rows):
            return sorted(rows, key=lambda row: row["a"])

        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows1 = [{"a": 0, "b": "blah", "c": False}]

        write_table(self._TABLE_PATH, rows1)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        sync_mount_table(self._TABLE_PATH)
        assert list(lookup_rows(self._TABLE_PATH, [{"a": 0}])) == rows1
        assert sorted_by_a(select_rows("* from [{}]".format(self._TABLE_PATH))) == rows1

        insert_rows(self._TABLE_PATH, [{"a": 0, "b": "boo", "c": True}, {"a": 1, "b": "foo", "c": True}])

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool", stable_name="c"),
            make_column("b_new", "string", stable_name="b"),
        ], unique_keys=True, strict=True)
        sync_unmount_table(self._TABLE_PATH)
        alter_table(self._TABLE_PATH, schema=schema2)

        rows2 = [
            {"a": 0, "b_new": "boo", "c": True},
            {"a": 1, "b_new": "foo", "c": True},
        ]

        assert read_table(self._TABLE_PATH) == rows2

        sync_mount_table(self._TABLE_PATH)

        assert list(lookup_rows(self._TABLE_PATH, [{"a": 0}, {"a": 1}])) == rows2
        assert sorted_by_a(select_rows("* from [{}]".format(self._TABLE_PATH))) == rows2
        assert sorted_by_a(select_rows("* from [{}] where b_new = \"foo\"".format(self._TABLE_PATH))) == [rows2[1]]

        rows2_filtered = [
            {"a": 0, "b_new": "boo"},
            {"a": 1, "b_new": "foo"},
        ]
        assert list(lookup_rows(self._TABLE_PATH, [{"a": 0}, {"a": 1}], column_names=["a", "b_new"])) == rows2_filtered
        assert sorted_by_a(select_rows("a, b_new from [{}]".format(self._TABLE_PATH))) == rows2_filtered
        assert sorted_by_a(select_rows("a, b_new from [{}] where b_new = \"foo\"".format(self._TABLE_PATH))) == [rows2_filtered[1]]

        insert_rows(self._TABLE_PATH, [{"a": 1, "b_new": "updated", "c": False}])

        sync_unmount_table(self._TABLE_PATH)
        alter_table(self._TABLE_PATH, schema=schema1)

        rows3 = [
            {"a": 0, "b": "boo", "c": True},
            {"a": 1, "b": "updated", "c": False},
        ]

        assert read_table(self._TABLE_PATH) == rows3

        sync_mount_table(self._TABLE_PATH)

        assert list(lookup_rows(self._TABLE_PATH, [{"a": 0}, {"a": 1}])) == rows3
        assert sorted_by_a(select_rows("* from [{}]".format(self._TABLE_PATH))) == rows3
        assert sorted_by_a(select_rows("* from [{}] where b = \"boo\"".format(self._TABLE_PATH))) == [rows3[0]]

        rows3_filtered = [
            {"a": 0, "b": "boo"},
            {"a": 1, "b": "updated"},
        ]
        assert list(lookup_rows(self._TABLE_PATH, [{"a": 0}, {"a": 1}], column_names=["a", "b"])) == rows3_filtered
        assert sorted_by_a(select_rows("a,b from [{}]".format(self._TABLE_PATH))) == rows3_filtered
        assert sorted_by_a(select_rows("a,b from [{}] where b = \"boo\"".format(self._TABLE_PATH))) == [rows3_filtered[0]]

    @staticmethod
    def _to_ordered_dyntable_rows(rows):
        res = []
        for i, row in enumerate(rows):
            row = deepcopy(row)
            row.update({
                "$tablet_index": 0,
                "$row_index": i,
            })
            res.append(row)
        return res

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_ordered_dynamic(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "int64"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows1 = [{"a": 0, "b": "blah", "c": False}]

        write_table(self._TABLE_PATH, rows1)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        sync_mount_table(self._TABLE_PATH)
        assert list(select_rows("* from [{}]".format(self._TABLE_PATH))) == self._to_ordered_dyntable_rows(rows1)

        insert_rows(self._TABLE_PATH, [{"a": 0, "b": "boo", "c": True}, {"a": 1, "b": "foo", "c": True}])

        schema2 = make_schema([
            make_column("a", "int64"),
            make_column("c", "bool", stable_name="c"),
            make_column("b_new", "string", stable_name="b"),
        ], strict=True)
        sync_unmount_table(self._TABLE_PATH)
        alter_table(self._TABLE_PATH, schema=schema2)

        rows2 = [
            {"a": 0, "b_new": "blah", "c": False},
            {"a": 0, "b_new": "boo", "c": True},
            {"a": 1, "b_new": "foo", "c": True},
        ]

        assert read_table(self._TABLE_PATH) == rows2

        sync_mount_table(self._TABLE_PATH)

        rows2_ordered_dyntable = self._to_ordered_dyntable_rows(rows2)
        assert list(select_rows("* from [{}]".format(self._TABLE_PATH))) == rows2_ordered_dyntable
        assert list(select_rows("* from [{}] where b_new != \"foo\"".format(self._TABLE_PATH))) == rows2_ordered_dyntable[:2]

        insert_rows(self._TABLE_PATH, [{"a": 1, "b_new": "updated", "c": False}])

        sync_unmount_table(self._TABLE_PATH)
        alter_table(self._TABLE_PATH, schema=schema1)

        rows3 = [
            {"a": 0, "b": "blah", "c": False},
            {"a": 0, "b": "boo", "c": True},
            {"a": 1, "b": "foo", "c": True},
            {"a": 1, "b": "updated", "c": False},
        ]

        assert read_table(self._TABLE_PATH) == rows3

        sync_mount_table(self._TABLE_PATH)

        rows3_ordered_dyntable = self._to_ordered_dyntable_rows(rows3)
        assert list(select_rows("* from [{}]".format(self._TABLE_PATH))) == rows3_ordered_dyntable
        assert list(select_rows("* from [{}] where b != \"blah\"".format(self._TABLE_PATH))) == rows3_ordered_dyntable[1:]

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_static_several_chunks(self, optimize_for):
        table_path_with_append = "<append=%true>" + self._TABLE_PATH

        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows1 = [{"a": 0, "b": "foo", "c": True}]
        write_table(self._TABLE_PATH, rows1)
        assert read_table(self._TABLE_PATH) == rows1

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("b_new", "string", stable_name="b"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema2)

        rows2 = [{"a": 0, "b_new": "foo", "c_new": True}]
        assert read_table(self._TABLE_PATH) == rows2

        write_table(table_path_with_append, [{"a": 1, "b_new": "bar", "c_new": True}])

        rows2_new = rows2 + [{"a": 1, "b_new": "bar", "c_new": True}]
        assert read_table(self._TABLE_PATH) == rows2_new

        schema3 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("d", type_v3=optional_type("int64")),
            make_column("b_newer", "string", stable_name="b"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema3)

        rows3 = [
            {"a": 0, "b_newer": "foo", "c_new": True},
            {"a": 1, "b_newer": "bar", "c_new": True},
        ]
        assert read_table(self._TABLE_PATH) == rows3

        write_table(table_path_with_append, [{"a": 22, "b_newer": "booh", "c_new": False, "d": -12}])

        rows3_new = [
            {"a": 0, "b_newer": "foo", "c_new": True},
            {"a": 1, "b_newer": "bar", "c_new": True},
            {"a": 22, "b_newer": "booh", "c_new": False, "d": -12},
        ]
        assert read_table(self._TABLE_PATH) == rows3_new

        schema4 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string", stable_name="b"),
            make_column("c", "bool", stable_name="c"),
            make_column("d_new", type_v3=optional_type("int64"), stable_name="d"),
        ], unique_keys=True, strict=True)

        alter_table(self._TABLE_PATH, schema=schema4)

        rows4 = [
            {"a": 0, "b": "foo", "c": True},
            {"a": 1, "b": "bar", "c": True},
            {"a": 22, "b": "booh", "c": False, "d_new": -12},
        ]
        assert read_table(self._TABLE_PATH) == rows4


class TestDeleteColumnsDisabledStatic(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    ENABLE_STATIC_DROP_COLUMN = False

    _TABLE_PATH = "//tmp/test-alter-table"

    @authors("orlovorlov")
    def test_drop_column(self):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": "lookup",
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        with raises_yt_error('Cannot remove column "c" from a strict schema'):
            alter_table(self._TABLE_PATH, schema=schema2, verbose=True)


class TestDeleteColumnsDisabledDynamic(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    ENABLE_STATIC_DROP_COLUMN = True
    ENABLE_DYNAMIC_DROP_COLUMN = False

    _TABLE_PATH = "//tmp/test-alter-table"

    @authors("orlovorlov")
    def test_drop_change_to_dynamic(self):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": "lookup",
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

        sync_create_cells(1)
        with raises_yt_error("Deleting columns is not allowed on a dynamic table"):
            alter_table(self._TABLE_PATH, dynamic=True)

    @authors("orlovorlov")
    def test_drop_dynamic(self):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": "lookup",
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)

        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        with raises_yt_error('Cannot remove column "c" from a strict schema'):
            alter_table(self._TABLE_PATH, schema=schema2, verbose=True)


class TestDeleteColumns(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    _TABLE_PATH = "//tmp/test-alter-table"

    def fetch_node_events(self, event_type):
        events = []
        for k in range(self.NUM_NODES):
            allEvents = [json.loads(line) for line in open(self.path_to_run + '/logs/node-%d.json.log' % k)]
            events = events + [event for event in allEvents if event.get('event_type', '') == event_type]
        return events

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_column(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

        schema3 = get(self._TABLE_PATH + "/@schema")
        assert schema3[2]['stable_name'] == 'c'
        assert schema3[2]['deleted']

        sync_mount_table(self._TABLE_PATH)
        rows = lookup_rows(self._TABLE_PATH, [{"a": 0}])
        assert rows == [{"a": 0, "b": "foo"}]
        sync_unmount_table(self._TABLE_PATH)

        schema4 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
        ], unique_keys=True, strict=True)
        with raises_yt_error('Deleted column "c" must remain in the deleted column list'):
            alter_table(self._TABLE_PATH, schema=schema4, verbose=True)

        schema5 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)
        with raises_yt_error('Deleted column "c" must remain in the deleted column list'):
            alter_table(self._TABLE_PATH, schema=schema5, verbose=True)

        schema6 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("cc", "bool", stable_name="c"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        with raises_yt_error("Duplicate column stable name \"c\""):
            alter_table(self._TABLE_PATH, schema=schema6, verbose=True)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_renamed_column(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("bb", "string", stable_name="b"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

        schema3 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool"),
            make_deleted_column("bb"),
        ], unique_keys=True, strict=True)
        with raises_yt_error("Column \"bb\" (stable name \"b\") is missing in strict schema"):
            alter_table(self._TABLE_PATH, schema=schema3, verbose=True)

        schema4 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema4, verbose=True)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_reintroduce_column(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": 0, "b": "foo", "c": False}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

        schema3 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool"),
            make_column("b", "string"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        with raises_yt_error("Duplicate column stable name \"b\""):
            alter_table(self._TABLE_PATH, schema=schema3, verbose=True)

        schema4 = make_schema([
            make_column("a", "int64", sort_order="ascending"),
            make_column("c", "bool"),
            make_column("b", optional_type("string"), stable_name="bb"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema4, verbose=True)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_key_column_not_allowed(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "string", sort_order="ascending"),
            make_column("c", "int64"),
            make_column("d", "int64"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": "0", "b": "foo", "c": -1, "d": 0}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("c", "int64"),
            make_column("d", "int64"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)

        with raises_yt_error("Key column \"b\" may not be deleted"):
            alter_table(self._TABLE_PATH, schema=schema2, verbose=True)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_multiple_columns(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "int64"),
            make_column("d", "bool"),
            make_column("e", optional_type("int64")),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        def string_key(i):
            return "%04x%04x" % (i, i)

        def random_string():
            return ''.join([chr(ord('a') + random.randint(0, 25)) for i in range(10)])

        n_rows = 1000
        ne = 100
        n_updated = 250

        rows = [
            {
                "a": string_key(i),
                "b": random_string(),
                "c": random.randint(-2**63, 2**63 - 1),
                "d": bool(random.randint(0, 1))
            } for i in range(n_rows)]

        rindex = {}
        for row in rows:
            rindex[row['a']] = row

        dindx = list(range(n_rows))
        random.shuffle(dindx)
        dindx = dindx[:ne]

        for j in dindx:
            rows[j]['e'] = random.randint(0, 100)

        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)

        alter_table(self._TABLE_PATH, dynamic=True)

        schema2 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "string"),
            make_column("c", "int64"),
            make_column("e", optional_type("int64")),
            make_deleted_column("d"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2)

        sync_mount_table(self._TABLE_PATH)
        rowsd = lookup_rows(self._TABLE_PATH, [{"a": string_key(i)} for i in range(n_rows)])
        sync_unmount_table(self._TABLE_PATH)

        for row in rows:
            del row['d']
            if 'e' not in row:
                row['e'] = yson.YsonEntity()

        assert rowsd == rows

        schema3 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("c", "int64"),
            make_column("e", optional_type("int64")),
            make_deleted_column("d"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema3)

        sync_mount_table(self._TABLE_PATH)
        rowsdb = lookup_rows(self._TABLE_PATH, [{"a": string_key(i)} for i in range(n_rows)])
        sync_unmount_table(self._TABLE_PATH)

        for row in rows:
            del row['b']

        assert rowsdb == rows

        schema4 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("c", "int64"),
            make_column("b", optional_type("string"), stable_name="bb"),
            make_column("d", optional_type("string"), stable_name="dd"),
            make_column("e", optional_type("int64")),
            make_deleted_column("d"),
            make_deleted_column("b"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema4)

        updatedb = rows.copy()
        random.shuffle(updatedb)
        updatedb = updatedb[:n_updated]

        rupdated = {}

        for row in updatedb:
            row['b'] = random_string()
            rindex[row['a']]['b'] = row['b']
            rupdated[row['a']] = True

        updatedd = rows.copy()
        random.shuffle(updatedd)
        updatedd = updatedd[:n_updated]

        for row in updatedd:
            row['d'] = random_string()
            rindex[row['a']]['d'] = row['d']
            rupdated[row['a']] = True

        for row in rows:
            if 'b' not in row:
                row['b'] = yson.YsonEntity()
            if 'd' not in row:
                row['d'] = yson.YsonEntity()

        sync_mount_table(self._TABLE_PATH)
        insert_rows(self._TABLE_PATH, [row for row in rows if row['a'] in rupdated])
        rowsu = lookup_rows(self._TABLE_PATH, [{"a": string_key(i)} for i in range(n_rows)])
        sync_unmount_table(self._TABLE_PATH)

        assert rowsu == rows

    def generate_string(self, n):
        return ''.join([chr(ord('a') + random.randint(0, 25)) for _ in range(n)])

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_and_compact(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_column("c", "string"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{"a": "0", "b": 2, "c": self.generate_string(2048)}]
        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)
        sync_mount_table(self._TABLE_PATH)
        rows = [
            {"a": "1", "b": 3, "c": self.generate_string(2048)},
            {"a": "2", "b": 4, "c": self.generate_string(2048)},
        ]

        insert_rows(self._TABLE_PATH, rows)

        sync_flush_table(self._TABLE_PATH)
        sync_compact_table(self._TABLE_PATH)
        initial_compaction = self.fetch_node_events('end_compaction')[-1]
        assert initial_compaction['output_data_weight'] > 1000

        sync_unmount_table(self._TABLE_PATH)

        schema2 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2)

        sync_mount_table(self._TABLE_PATH)
        sync_flush_table(self._TABLE_PATH)
        sync_compact_table(self._TABLE_PATH)
        last_compaction = self.fetch_node_events('end_compaction')[-1]
        assert last_compaction['sequence_number'] > initial_compaction['sequence_number']
        assert last_compaction['output_data_weight'] < 100

    def create_and_populate_table(self, table_name, optimize_for, index):
        schema = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_column("c", "string"),
        ], unique_keys=True, strict=True)

        create("table", table_name, force=True, attributes={
            "schema": schema,
            "optimize_for": optimize_for,
        })

        rows = [{
            "a": "%04x" % (i + index * 10000),
            "b": 2,
            "c": self.generate_string(1024)
        } for i in range(1000)]

        write_table(table_name, rows)

    def make_dynamic(self, table_name):
        sync_create_cells(1)
        alter_table(table_name, dynamic=True)
        sync_mount_table(table_name)

    def alter_drop_column(self, table_name):
        schema = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        alter_table(table_name, schema=schema)

    def alter_drop_column_dynamic(self, table_name):
        sync_unmount_table(table_name)
        self.alter_drop_column(table_name)
        sync_mount_table(table_name)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_no_teleportation(self, optimize_for):
        in1 = "//tmp/merge1"
        in2 = "//tmp/merge2"

        self.create_and_populate_table(in1, optimize_for, 0)
        self.create_and_populate_table(in2, optimize_for, 1)

        m1 = "//tmp/m1"
        create("table", m1)

        merge(in_=[in1, in2], out=m1)

        in1_chunks = get(in1 + "/@chunk_ids")
        in2_chunks = get(in2 + "/@chunk_ids")
        m1_chunks = get(m1 + "/@chunk_ids")

        assert m1_chunks == in1_chunks + in2_chunks

        self.alter_drop_column(in1)
        self.alter_drop_column(in2)

        m2 = "//tmp/m2"
        create("table", m2)

        merge(in_=[in1, in2], out=m2)
        m2_chunks = get(m2 + "/@chunk_ids")
        m2_schema = get(m2 + "/@schema")

        assert not builtins.set(m2_chunks).intersection(builtins.set(in1_chunks + in2_chunks))
        assert len(m2_schema) == 2

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_merge(self, optimize_for):
        in1 = "//tmp/merge1"
        in2 = "//tmp/merge2"
        self.create_and_populate_table(in1, optimize_for, 0)
        self.make_dynamic(in1)
        self.create_and_populate_table(in2, optimize_for, 1)
        self.make_dynamic(in2)

        m1 = "//tmp/m1"
        create("table", m1)

        merge(in_=[in1, in2], out=m1)
        input_chunks = get(in1 + "/@chunk_ids") + get(in2 + "/@chunk_ids")
        output_chunks = get(m1 + "/@chunk_ids")

        assert not builtins.set(input_chunks).intersection(output_chunks)

        m2 = "//tmp/m2"
        create("table", m2)

        self.alter_drop_column_dynamic(in1)
        self.alter_drop_column_dynamic(in2)

        merge(in_=[in1, in2], out=m2)
        output_chunks = get(m1 + "/@chunk_ids")

        assert not builtins.set(input_chunks).intersection(output_chunks)

        merged_schema = get("//tmp/m2/@schema")
        assert not [col for col in merged_schema if col.get('stable_name', '') == 'c' or col.get('name') == 'c']

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_switch_to_static(self, optimize_for):
        self.create_and_populate_table(self._TABLE_PATH, optimize_for, 0)
        self.make_dynamic(self._TABLE_PATH)
        self.alter_drop_column_dynamic(self._TABLE_PATH)

        sync_unmount_table(self._TABLE_PATH)
        with raises_yt_error("Cannot switch mode from dynamic to static: table is sorted"):
            alter_table(self._TABLE_PATH, dynamic=False)

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_ordered_append(self, optimize_for):
        schema = make_schema([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int64"},
            {"name": "c", "type": "string"},
        ], strict=True)
        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema,
            "optimize_for": optimize_for,
        })

        rows = [{
            'a': self.generate_string(1024),
            'b': random.randint(-10**12, 10**12),
            'c': self.generate_string(1024),
        } for i in range(20)]

        write_table(self._TABLE_PATH, rows)
        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)
        sync_mount_table(self._TABLE_PATH)

        rows1 = [{
            'a': self.generate_string(1024),
            'b': random.randint(-10**12, 10**12),
            'c': self.generate_string(1024),
        } for i in range(20)]
        insert_rows(self._TABLE_PATH, rows1)
        sync_unmount_table(self._TABLE_PATH)

        schema1 = make_schema([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int64"},
            {"stable_name": "c", "deleted": True},
        ])

        alter_table(self._TABLE_PATH, schema=schema1)
        sync_mount_table(self._TABLE_PATH)

        out = select_rows("* from [" + self._TABLE_PATH + "]")
        expected = (rows + rows1).copy()
        for i, r in enumerate(expected):
            del r['c']
            r['$row_index'] = i
            r['$tablet_index'] = 0
        assert out == expected

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_drop_static(self, optimize_for):
        schema1 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_column("c", "string"),
        ], unique_keys=True, strict=True)

        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        })

        rows = [{
            "a": "%4x" % (1000 * i),
            "b": random.randint(-1000, 1000),
            "c": "foo"
        } for i in range(20)]

        write_table(self._TABLE_PATH, rows)

        schema2 = make_schema([
            make_column("a", "string", sort_order="ascending"),
            make_column("b", "int64"),
            make_deleted_column("c"),
        ], unique_keys=True, strict=True)
        alter_table(self._TABLE_PATH, schema=schema2)

        rows1 = [{
            "a": "%4x" % (1000 * i + 20000 + 123),
            "b": random.randint(-1000, 1000),
        } for i in range(20)]
        write_table('<append=%true>' + self._TABLE_PATH, rows1)

        out = read_table(self._TABLE_PATH)
        expected = (rows + rows1).copy()
        for r in expected:
            if 'c' in r:
                del r['c']

        assert out == expected

    @authors("orlovorlov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_ordered_change_to_static(self, optimize_for):
        schema = make_schema([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int64"},
            {"name": "c", "type": "boolean"},
        ], strict=True)
        create("table", self._TABLE_PATH, force=True, attributes={
            "schema": schema,
            "optimize_for": optimize_for,
        })

        rows = [{"a": "foo", "b": 123, "c": True}]
        write_table(self._TABLE_PATH, rows)

        sync_create_cells(1)
        alter_table(self._TABLE_PATH, dynamic=True)
        sync_mount_table(self._TABLE_PATH)
        assert not get(self._TABLE_PATH + "/@sorted")
        sync_unmount_table(self._TABLE_PATH)
        schema1 = make_schema([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int64"},
            make_deleted_column("c"),
        ])
        alter_table(self._TABLE_PATH, schema=schema1)
        alter_table(self._TABLE_PATH, dynamic=False)

        updated_schema = get(self._TABLE_PATH + "/@schema")
        assert updated_schema[2]['deleted']
        assert updated_schema[2]['stable_name'] == 'c'

        rows = [{"a": "bar", "b": 456}]
        write_table('<append=%true>' + self._TABLE_PATH, rows)

        data = read_table(self._TABLE_PATH)
        assert data == [
            {"a": "foo", "b": 123},
            {"a": "bar", "b": 456},
        ]
