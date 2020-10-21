from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt_helpers import *

import yt.yson


PARTITIONED_BY = ["foo", "bar"]
SCHEMA = [
    {"name": "foo", "type": "int64", "sort_order": "ascending"},
    {"name": "bar", "type": "string", "sort_order": "ascending"},
    {"name": "baz", "type": "int64", "sort_order": "ascending"},
    {"name": "qux", "type": "boolean"},
]
SCHEMA_NON_STRICT = yt.yson.to_yson_type(SCHEMA, attributes={"strict": False})

PARTITIONS = [
    {"path": "//tmp/t0", "key": [10, "abc"]},
    {"path": "//tmp/t1", "key": [10, "def"]},
    {"path": "//tmp/t2", "key": [20, "abc"]},
    {"path": "//tmp/t3", "key": [20, "abc"]},
]

PARTITION_SCHEMA_BASE = [
    {"name": "baz", "type": "int64", "sort_order": "ascending"},
    {"name": "qux", "type": "boolean"},
]
PARTITION_SCHEMA_EXTRA_COL = [
    {"name": "baz", "type": "int64", "sort_order": "ascending"},
    {"name": "qux", "type": "boolean"},
    {"name": "quux", "type": "string"},
]
PARTITION_SCHEMA_EXTRA_SORT_COL1 = [
    {"name": "baz", "type": "int64", "sort_order": "ascending"},
    {"name": "qux", "type": "boolean", "sort_order": "ascending"},
]
PARTITION_SCHEMA_EXTRA_SORT_COL2 = [
    {"name": "baz", "type": "int64", "sort_order": "ascending"},
    {"name": "quux", "type": "boolean", "sort_order": "ascending"},
    {"name": "qux", "type": "boolean"},
]


def validate_misconfigured_partitions(error, assertions):
    """
    Validate particular errors for given partition indices.
    :param error: compound error.
    :type error: YtError
    :param assertions: dict mapping partition index to int, string or None (int is treated as an error code,
    string is treated as a message substring and None is treated as any error).
    :type assertions: dict
    """
    # Find root error with partition suberrors.
    error = error.find_matching_error(code=MisconfiguredPartitions)
    assert error is not None

    # Build partition to suberror mapping.
    partition_index_to_error = dict()
    for inner_error in error.inner_errors:
        if not isinstance(inner_error, YtError):
            inner_error = YtError(**inner_error)
        partition_index = inner_error.attributes.get("partition_index")
        assert partition_index is not None
        partition_index_to_error[partition_index] = inner_error

    # Validate partition errors.
    for partition_index, assertion in assertions.items():
        inner_error = partition_index_to_error.get(partition_index)
        assert inner_error is not None
        if isinstance(assertion, int):
            assert inner_error.contains_code(assertion)
        elif isinstance(assertion, str):
            assert inner_error._contains_text(assertion)
        else:
            assert assertion is None


class TestPartitionedTables(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    @authors("max42")
    def test_attributes(self):
        # Right now partitioned table node is pretty rudimentary, so there is not much interesting to test here.
        create("partitioned_table", "//tmp/pt")
        get("//tmp/pt/@")

        set("//tmp/pt/@partitioned_by", PARTITIONED_BY)
        assert get("//tmp/pt/@partitioned_by") == PARTITIONED_BY

        set("//tmp/pt/@schema", SCHEMA)
        assert get("//tmp/pt/@schema") == SCHEMA

    @authors("max42")
    @pytest.mark.parametrize("assume_partitioned_table", [False, True])
    def test_simple(self, assume_partitioned_table):
        node_type = "partitioned_table" if not assume_partitioned_table else "map_node"
        create(
            node_type,
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        write_table("//tmp/t0", [{"baz": 5}])
        write_table("//tmp/t1", [{"baz": 4}])
        write_table("//tmp/t2", [{"baz": 1}])
        write_table("//tmp/t3", [{"baz": 10}])

        assert read_table("//tmp/pt", table_reader={"assume_partitioned_table": assume_partitioned_table},) == [
            {"foo": 10, "bar": "abc", "baz": 5, "qux": None},
            {"foo": 10, "bar": "def", "baz": 4, "qux": None},
            {"foo": 20, "bar": "abc", "baz": 1, "qux": None},
            {"foo": 20, "bar": "abc", "baz": 10, "qux": None},
        ]

    @authors("max42")
    def test_partitioned_table_validation(self):
        # Missing table node is handled by table read spec fething routine,
        # so we do not test it explicitly.
        create("partitioned_table", "//tmp/pt")

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        # Attributes partitioned_by and schema not set.
        with raises_yt_error(ResolveErrorCode):
            read_table("//tmp/pt")

        set("//tmp/pt/@schema", SCHEMA)

        # Garbage instead of partitions.
        set("//tmp/pt/@partitions", 42)
        with raises_yt_error():
            read_table("//tmp/pt")

        set("//tmp/pt/@partitions", PARTITIONS)

        # Invalid partitioned_by order.
        set("//tmp/pt/@partitioned_by", PARTITIONED_BY[::-1])
        with raises_yt_error(InvalidPartitionedBy):
            read_table("//tmp/pt")

        set("//tmp/pt/@partitioned_by", PARTITIONED_BY)

        # Computed partitioned column.
        set("//tmp/pt/@schema/0/expression", "farm_hash(bar)")
        with raises_yt_error(InvalidSchemaValue):
            read_table("//tmp/pt")
        remove("//tmp/pt/@schema/0/expression")

        # Aggregated partitioned column.
        set("//tmp/pt/@schema/0/aggregate", "sum")
        with raises_yt_error(InvalidSchemaValue):
            read_table("//tmp/pt")
        remove("//tmp/pt/@schema/0/aggregate")

        # Any and composite types are not allowed.
        set("//tmp/pt/@schema/0/type", "any")
        with raises_yt_error(InvalidSchemaValue):
            read_table("//tmp/pt")
        remove("//tmp/pt/@schema/0/type")
        set("//tmp/pt/@schema/0/type_v3", {"type_name": "list", "item": "int64"})
        with raises_yt_error(InvalidSchemaValue):
            read_table("//tmp/pt")
        remove("//tmp/pt/@schema/0/type_v3")
        set("//tmp/pt/@schema/0/type", "int64")

        # Unique keys.
        set("//tmp/pt/@schema/@unique_keys", True)
        with raises_yt_error(InvalidSchemaValue):
            read_table("//tmp/pt")
        remove("//tmp/pt/@schema/@unique_keys")

        # Check that after fixing all errors table is readable.

        assert read_table("//tmp/pt") == []

    @authors("max42")
    def test_partitions(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS[:1],
            },
        )

        # Partitions missing.
        with raises_yt_error(ResolveErrorCode):
            read_table("//tmp/pt")

        # Wrong type.
        create("map_node", "//tmp/t0")
        with raises_yt_error(InvalidObjectType):
            read_table("//tmp/pt")

        # Dynamic table.
        create_dynamic_table("//tmp/t1", schema=PARTITION_SCHEMA_BASE, force=True)
        with raises_yt_error(InvalidObjectType):
            read_table("//tmp/pt")

    @authors("max42")
    def test_sort_order_validation(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        # Empty partitions.
        assert read_table("//tmp/pt") == []

        write_table("//tmp/t0", [{"baz": 5}])
        write_table("//tmp/t1", [{"baz": 4}])
        write_table("//tmp/t2", [{"baz": 1}])
        write_table("//tmp/t3", [{"baz": 10}])

        # Correct sort order.
        assert read_table("//tmp/pt") == [
            {"foo": 10, "bar": "abc", "baz": 5, "qux": None},
            {"foo": 10, "bar": "def", "baz": 4, "qux": None},
            {"foo": 20, "bar": "abc", "baz": 1, "qux": None},
            {"foo": 20, "bar": "abc", "baz": 10, "qux": None},
        ]

        # Incorrect sort order.
        set("//tmp/pt/@partitions", PARTITIONS[:1] + PARTITIONS[2:0:-1] + PARTITIONS[3:])

        with raises_yt_error(SortOrderViolation):
            read_table("//tmp/pt")

        # Correct partition key sort order, but overlapping two last partitions.
        set("//tmp/pt/@partitions", PARTITIONS)
        write_table("//tmp/t3", [{"baz": 0}])

        with raises_yt_error(SortOrderViolation):
            read_table("//tmp/pt")

    @authors("max42")
    def test_partition_schemas(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA_NON_STRICT,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i, schema in enumerate(
            [
                PARTITION_SCHEMA_BASE,
                PARTITION_SCHEMA_EXTRA_COL,
                PARTITION_SCHEMA_EXTRA_SORT_COL1,
                PARTITION_SCHEMA_EXTRA_SORT_COL2,
            ]
        ):
            create("table", "//tmp/t" + str(i), attributes={"schema": schema})

        assert read_table("//tmp/pt") == []

        # Extra columns cannot be present in strict schema.
        set("//tmp/pt/@schema", SCHEMA)

        with raises_yt_error() as err:
            read_table("//tmp/pt")
        validate_misconfigured_partitions(
            err[0],
            {
                1: IncompatibleSchemas,
                3: IncompatibleSchemas,
            },
        )

    @authors("max42")
    def test_partition_keys(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        set("//tmp/pt/@partitions/0/key", [10, "abc", 3.14])
        set("//tmp/pt/@partitions/1/key", [20])
        set("//tmp/pt/@partitions/2/key", ["xyz", 20])
        # This one is OK
        set("//tmp/pt/@partitions/3/key", [30, None])
        with raises_yt_error() as err:
            read_table("//tmp/pt")
        validate_misconfigured_partitions(
            err[0],
            {
                0: SchemaViolation,
                1: SchemaViolation,
                2: SchemaViolation,
            },
        )

    @authors("max42")
    def test_column_selectors(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        write_table("//tmp/t0", [{"baz": 5}])
        write_table("//tmp/t1", [{"baz": 4, "qux": True}])
        write_table("//tmp/t2", [{"baz": 1, "qux": False}])
        write_table("//tmp/t3", [{"baz": 10}])

        assert read_table("//tmp/pt{foo,qux}") == [
            {"foo": 10, "qux": None},
            {"foo": 10, "qux": True},
            {"foo": 20, "qux": False},
            {"foo": 20, "qux": None},
        ]
        assert read_table("//tmp/pt{baz}") == [
            {"baz": 5},
            {"baz": 4},
            {"baz": 1},
            {"baz": 10},
        ]
        assert read_table("//tmp/pt{}") == [{}] * 4

    @authors("max42")
    def test_ranges(self):
        create(
            "partitioned_table",
            "//tmp/pt",
            attributes={
                "schema": SCHEMA,
                "partitioned_by": PARTITIONED_BY,
                "partitions": PARTITIONS,
            },
        )

        for i in xrange(4):
            create(
                "table",
                "//tmp/t" + str(i),
                attributes={"schema": PARTITION_SCHEMA_BASE},
            )

        with raises_yt_error("Partitioned tables do not support range selectors"):
            read_table("//tmp/pt[#0]")
