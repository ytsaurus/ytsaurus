from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *
import pytest

##################################################################

GOOD_VALUE_LIST = [
    {"i32": 2 ** 31 - 1}, {"i32": 0}, {"i32": - 2 ** 31}, {"i32": None},
    {"i16": 2 ** 15 - 1}, {"i16": 0}, {"i16": - 2 ** 15}, {"i16": None},
    {"i8": 2 ** 7 - 1}, {"i8": 0}, {"i8": - 2 ** 7}, {"i8": None},

    {"ui32": 0}, {"ui32": 2 ** 32 - 1}, {"ui32": None},
    {"ui16": 0}, {"ui16": 2 ** 16 - 1}, {"ui16": None},
    {"ui8": 0}, {"ui8": 2 ** 8 - 1}, {"ui8": None},
]

BAD_VALUE_LIST = [
    {"i32": 2 ** 31}, {"i32": - 2 ** 31 - 1},
    {"i16": 2 ** 15}, {"i16": - 2 ** 15 - 1},
    {"i8": 2 ** 7}, {"i8": - 2 ** 7 - 1},

    {"ui32": 2 ** 32},
    {"ui16": 2 ** 16},
    {"ui8": 2 ** 8}
]

SCHEMA = [
    {
        "type": "uint64",
        "name": "key",
        "sort_order": "ascending",
    },
    {
        "type": "uint32",
        "name": "ui32",
    },
    {
        "type": "uint16",
        "name": "ui16",
    },
    {
        "type": "uint8",
        "name": "ui8",
    },
    {
        "type": "int32",
        "name": "i32",
    },
    {
        "type": "int16",
        "name": "i16",
    },
    {
        "type": "int8",
        "name": "i8",
    },
]


class TestLogicalType(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    def test_static_tables(self):
        create("table", "//test-table", attributes={"schema": SCHEMA})

        # expect no assertions
        write_table("//test-table", GOOD_VALUE_LIST)

        for bad_value in BAD_VALUE_LIST:
            with pytest.raises(YtError):
                write_table("//test-table", [bad_value])

    def test_dynamic_tables(self):
        self.sync_create_cells(1)
        create("table", "//test-dynamic-table", attributes={"schema": SCHEMA, "dynamic": True})

        self.sync_mount_table("//test-dynamic-table")

        key = 0
        for val in GOOD_VALUE_LIST:
            key += 1
            val = val.copy()
            val["key"] = key
            insert_rows("//test-dynamic-table", [val])

        for val in BAD_VALUE_LIST:
            key += 1
            val = val.copy()
            val["key"] = key
            with pytest.raises(YtError):
                insert_rows("//test-dynamic-table", [val])

        self.sync_unmount_table("//test-dynamic-table")

    def test_bad_alter_table(self):
        def single_column_schema(typename):
            return [{"name": "column_name", "type": typename}]

        def expect_error_alter_table(schema_before, schema_after):
            remove("//test-alter-table", force=True)
            create("table", "//test-alter-table", attributes={"schema": schema_before})
            # Make table nonempty, since empty table allows any alter
            write_table("//test-alter-table", [{}])
            with pytest.raises(YtResponseError):
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

    def test_logical_type_column_constrains(self):
        with pytest.raises(YtError):
            create("table", "//test-table",
                   attributes={"schema": [
                       {"name": "key1", "type": "int32", "expression": "100"},
                   ]})

        with pytest.raises(YtError):
            create("table", "//test-table",
                   attributes={"schema": [
                       {"name": "key1", "type": "int32", "aggregate": "sum"},
                   ]})
