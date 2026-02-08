from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, wait, create, exists, get, set, ls, insert_rows, remove, select_rows, trim_rows,
    lookup_rows, delete_rows, remount_table, build_master_snapshots, get_tablet_leader_address, concatenate,
    write_table, alter_table, read_table, map, merge, sync_reshard_table, sync_create_cells, get_operation,
    sync_mount_table, sync_unmount_table, sync_flush_table, sync_compact_table, gc_collect, pull_queue,
    start_transaction, commit_transaction, get_singular_chunk_id, write_file, read_hunks, remote_copy,
    write_journal, create_domestic_medium, update_nodes_dynamic_config, raises_yt_error, copy, move,
    get_account_disk_space_limit, set_account_disk_space_limit, create_dynamic_table, create_user)

from yt_type_helpers import make_schema

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE,
    MASTERS_SERVICE
)

from yt.common import YtError
from yt.test_helpers import assert_items_equal
from operator import itemgetter

import yt_error_codes

import pytest
import yt.yson as yson

from copy import deepcopy
import time
import random

import builtins

################################################################################

HUNK_COMPATIBLE_CHUNK_FORMATS = [
    "table_versioned_simple",
    "table_versioned_columnar",
    "table_versioned_slim",
    "table_versioned_indexed",
]

################################################################################


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesHunks(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 7

    NUM_NODES = 15

    NUM_SCHEDULERS = 1

    SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]

    ROWS_WITH_VARIOUS_ANY_VALUES = [
        {"key": 0, "value": "0"},
        {"key": 1, "value": 123},
        {"key": 2, "value": 3.14},
        {"key": 3, "value": True},
        {"key": 4, "value": [{}, {}]},
        {"key": 5, "value": "0" * 20},
        {"key": 6, "value": yson.YsonEntity()}
    ]

    KEYS_OF_ROWS_WITH_VARIOUS_ANY_VALUES = [{"key": x["key"]} for x in ROWS_WITH_VARIOUS_ANY_VALUES]

    # Do not allow multiple erasure parts per node.
    DELTA_MASTER_CONFIG = {}

    def _get_table_schema(self, schema, max_inline_hunk_size):
        if "sort_order" not in schema[1]:
            schema[1]["max_inline_hunk_size"] = max_inline_hunk_size
        return schema

    def _create_table(self,
                      chunk_format="table_versioned_simple",
                      max_inline_hunk_size=10,
                      hunk_erasure_codec="none",
                      schema=SCHEMA,
                      dynamic=True,
                      enable_dynamic_store_read=False):
        create_table_function = self._create_simple_table if dynamic else self._create_simple_static_table
        schema = self._get_table_schema(schema, max_inline_hunk_size)
        if not dynamic:
            schema = make_schema(
                schema,
                unique_keys=True,
            )

        create_table_function(
            "//tmp/t",
            schema=schema,
            enable_dynamic_store_read=enable_dynamic_store_read,
            hunk_chunk_reader={
                "max_hunk_count_per_read": 2,
                "max_total_hunk_length_per_read": 60,
                "max_inflight_fragment_length": 60,
                "max_inflight_fragment_count": 2,
                "hedging_manager": {
                    "secondary_request_ratio": 0.5,
                    "max_hedging_delay": 1,
                },
            },
            hunk_chunk_writer={
                "desired_block_size": 50,
            },
            max_hunk_compaction_garbage_ratio=0.5,
            enable_lsm_verbose_logging=True,
            chunk_format=chunk_format,
            hunk_erasure_codec=hunk_erasure_codec)

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_flush_inline(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": "value" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 0

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0])) == []

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_timestamped_lookup(self):
        sync_create_cells(1)
        SCHEMA_WITH_VALUES = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 5},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 10},
        ]
        self._create_table(schema=SCHEMA_WITH_VALUES)

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value1": "value1" + str(i) * 10, "value2": "value2" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        result = lookup_rows("//tmp/t", keys, with_timestamps=True)
        timestamp = result[0]["$timestamp:value1"]

        rows1 = [{"key": i, "value1": "value1" + str(i) * 10} for i in range(10)]
        rows2 = [{"key": i, "value2": "value2" + str(i)} for i in range(10)]
        rows3 = [{"key": i, "value1": "value1" + str(i) * 10} for i in range(10)]
        for index in range(10):
            rows[index]["$timestamp:value1"] = timestamp
            rows[index]["$timestamp:value2"] = timestamp
            rows1[index]["$timestamp:value1"] = timestamp
            rows2[index]["$timestamp:value2"] = timestamp
            rows3[index]["$timestamp:value2"] = timestamp

        assert_items_equal(result, rows)

        def _check(column_names, expected_result):
            result = lookup_rows("//tmp/t", keys, with_timestamps=True, column_names=column_names)
            assert_items_equal(result, expected_result)

        _check(["key", "$timestamp:value1", "value1"], rows1)
        _check(["key", "$timestamp:value2", "value2"], rows2)
        _check(["key", "value2", "$timestamp:value2"], rows2)
        _check(["key", "value1", "$timestamp:value2"], rows3)
        _check(["key", "$timestamp:value2", "value1"], rows3)
        for index in range(10):
            rows3[index]["value2"] = "value2" + str(index)
        _check(["key", "value2", "$timestamp:value2", "value1"], rows3)

    @authors("akozhikhov")
    def test_timestamped_select(self):
        sync_create_cells(1)
        SCHEMA_WITH_VALUES = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 5},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 10},
        ]
        self._create_table(schema=SCHEMA_WITH_VALUES)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value1": "value1" + str(i) * 10, "value2": "value2" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        result = lookup_rows("//tmp/t", [{"key": 0}], with_timestamps=True)
        timestamp = result[0]["$timestamp:value1"]

        rows1 = [{"value1": "value1" + str(i) * 10} for i in range(10)]
        rows2 = [{"value2": "value2" + str(i)} for i in range(10)]
        rows3 = [{"key": i, "value1": "value1" + str(i) * 10} for i in range(10)]
        for index in range(10):
            rows[index]["$timestamp:value1"] = timestamp
            rows[index]["$timestamp:value2"] = timestamp
            rows1[index]["$timestamp:value1"] = timestamp
            rows2[index]["$timestamp:value2"] = timestamp
            rows3[index]["$timestamp:value2"] = timestamp

        def _check(columns, expected_result):
            result = select_rows(columns + " from [//tmp/t]", with_timestamps=True)
            assert_items_equal(result, expected_result)
            result = select_rows(columns + " from [//tmp/t] where key in (1, 3, 5, 7, 9)", with_timestamps=True)
            assert_items_equal(result, expected_result[1::2])

        _check("[$timestamp:value1], value1, value2, [$timestamp:value2], key", rows)
        _check("key, [$timestamp:value1], [$timestamp:value2], value1, value2", rows)
        _check("value1, [$timestamp:value1]", rows1)
        _check("value2, [$timestamp:value2]", rows2)
        _check("key, value1, [$timestamp:value2]", rows3)
        _check("key, [$timestamp:value2], value1", rows3)

    @authors("akozhikhov")
    def test_timestamped_operation(self):
        sync_create_cells(1)
        SCHEMA_WITH_VALUES = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 5},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 10},
        ]
        self._create_table(schema=SCHEMA_WITH_VALUES)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value1": "value1" + str(i) * 10, "value2": "value2" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)

        result = lookup_rows("//tmp/t", [{"key": 0}], with_timestamps=True)
        timestamp = result[0]["$timestamp:value1"]
        for index in range(10):
            rows[index]["$timestamp:value1"] = timestamp
            rows[index]["$timestamp:value2"] = timestamp

        sync_unmount_table("//tmp/t")

        create("table", "//tmp/t_out")
        map(
            in_="<versioned_read_options={read_mode=latest_timestamp}>//tmp/t",
            out="//tmp/t_out",
            command="cat",
            spec={
                "input_schema": [
                    {"name": "$timestamp:value2", "type": "uint64"},
                    {"name": "key", "type": "int64"},
                    {"name": "value2", "type": "string"},
                    {"name": "$timestamp:value1", "type": "uint64"},
                    {"name": "value1", "type": "string"},
                ],
                "input_query": "*",
            }
        )
        assert read_table("//tmp/t_out") == rows

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_flush_to_hunk_chunk(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]

        assert get("#{}/@ref_counter".format(store_chunk_id)) == 1

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == [
            {"chunk_id": hunk_chunk_ids[0], "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec}
        ]

        assert get("#{}/@ref_counter".format(hunk_chunk_id)) == 2
        assert get("#{}/@data_weight".format(hunk_chunk_id)) == 260
        assert get("#{}/@uncompressed_data_size".format(hunk_chunk_id)) == 340
        assert get("#{}/@compressed_data_size".format(hunk_chunk_id)) == 340

        assert get("//tmp/t/@chunk_format_statistics/hunk_default/chunk_count") == 1

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 10
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_hunk_length"] == 260
        assert hunk_statistics["total_referenced_hunk_length"] == 260

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(store_chunk_id)) and not exists("#{}".format(hunk_chunk_id)))

    @authors("gritukan")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_flush_nulls_to_hunk_chunk(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)

        SCHEMA_WITH_NULL = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "null_value", "type": "string", "max_inline_hunk_size": 20},
        ]
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec=hunk_erasure_codec, schema=SCHEMA_WITH_NULL)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20, "null_value": yson.YsonEntity()} for i in range(10)]
        insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        remove("//tmp/t")

    @authors("gritukan", "akozhikhov")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    def test_lookup_hunk_chunk_with_repair(self, chunk_format):
        self._separate_tablet_and_data_nodes()
        set("//sys/@config/chunk_manager/enable_chunk_replicator", False)

        sync_create_cells(1)[0]
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec="isa_reed_solomon_3_3")
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        self._set_ban_for_chunk_parts([0, 1, 4], True, hunk_chunk_id)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        self._set_ban_for_chunk_parts([0, 1, 4], False, hunk_chunk_id)

        self._set_ban_for_chunk_parts([0, 1, 2, 4], True, hunk_chunk_id)
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", keys)
        self._set_ban_for_chunk_parts([0, 1, 2, 4], False, hunk_chunk_id)

    @authors("gritukan")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("available", [False, True])
    def test_repair_erasure_hunk_chunk(self, chunk_format, available):
        self._separate_tablet_and_data_nodes()

        sync_create_cells(1)[0]
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec="isa_reed_solomon_6_3")
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        if available:
            self._set_ban_for_chunk_parts([0, 1, 4], True, hunk_chunk_id)
            time.sleep(1)
            wait(lambda: get("//sys/data_missing_chunks/@count") == 0)
            wait(lambda: get("//sys/parity_missing_chunks/@count") == 0)
            assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        else:
            self._set_ban_for_chunk_parts([0, 1, 2, 8], True, hunk_chunk_id)
            time.sleep(2)
            assert hunk_chunk_id in ls("//sys/data_missing_chunks")
            assert hunk_chunk_id in ls("//sys/parity_missing_chunks")

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_compaction(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec=hunk_erasure_codec)
        set("//tmp/t/@max_hunk_compaction_size", 1)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        keys1 = [{"key": i} for i in range(10)]
        insert_rows("//tmp/t", rows1)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1)
        assert_items_equal(lookup_rows("//tmp/t", keys1), rows1)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "y" * 20} for i in range(10, 20)]
        keys2 = [{"key": i} for i in range(10, 20)]
        insert_rows("//tmp/t", rows2)
        select_rows("* from [//tmp/t]")
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        sync_unmount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_count") == 4)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 2
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 2

        store_chunk_id1 = store_chunk_ids[0]
        store_chunk_id2 = store_chunk_ids[1]
        hunk_chunk_id1 = get("#{}/@hunk_chunk_refs/0/chunk_id".format(store_chunk_id1))
        hunk_chunk_id2 = get("#{}/@hunk_chunk_refs/0/chunk_id".format(store_chunk_id2))
        assert_items_equal(hunk_chunk_ids, [hunk_chunk_id1, hunk_chunk_id2])

        if get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id1)) \
                > get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id2)):
            store_chunk_id1, store_chunk_id2 = store_chunk_id2, store_chunk_id1
            hunk_chunk_id1, hunk_chunk_id2 = hunk_chunk_id2, hunk_chunk_id1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id1)) == [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec},
        ]
        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id2)) == [
            {"chunk_id": hunk_chunk_id2, "hunk_count": 10, "total_hunk_length": 270, "erasure_codec": hunk_erasure_codec},
        ]

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 2
        assert hunk_statistics["store_chunk_count"] == 2
        assert hunk_statistics["hunk_count"] == 20
        assert hunk_statistics["referenced_hunk_count"] == 20
        assert hunk_statistics["total_hunk_length"] == 530
        assert hunk_statistics["total_referenced_hunk_length"] == 530

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 3)

        compacted_store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(compacted_store_chunk_ids) == 1
        compacted_store_id = compacted_store_chunk_ids[0]

        assert_items_equal(get("#{}/@hunk_chunk_refs".format(compacted_store_id)), [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec},
            {"chunk_id": hunk_chunk_id2, "hunk_count": 10, "total_hunk_length": 270, "erasure_codec": hunk_erasure_codec},
        ])

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 2
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 20
        assert hunk_statistics["referenced_hunk_count"] == 20
        assert hunk_statistics["total_hunk_length"] == 530
        assert hunk_statistics["total_referenced_hunk_length"] == 530

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

    @authors("babenko")
    def test_hunk_sweep(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        keys1 = [{"key": i} for i in range(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(10, 20)]
        keys2 = [{"key": i} for i in range(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        delete_rows("//tmp/t", keys1)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 2
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)

        set("//tmp/t/@min_data_ttl", 60000)
        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 2)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 1)
        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 0

    @authors("babenko")
    def test_reshard(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        # This chunk will intersect both of new tablets and will produce chunk views.
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in [0, 10, 20, 30, 40, 50]]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id1 = store_chunk_ids[0]

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id1 = hunk_chunk_ids[0]

        sync_mount_table("//tmp/t")
        # This chunk will be fully contained in the first tablet.
        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in [11, 12, 13]]
        insert_rows("//tmp/t", rows2)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 2
        store_chunk_id2 = list(builtins.set(store_chunk_ids) - builtins.set([store_chunk_id1]))[0]

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 2
        hunk_chunk_id2 = list(builtins.set(hunk_chunk_ids) - builtins.set([hunk_chunk_id1]))[0]

        gc_collect()
        assert get("#{}/@ref_counter".format(store_chunk_id1)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id1)) == 2
        assert get("#{}/@ref_counter".format(store_chunk_id2)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id2)) == 2

        sync_reshard_table("//tmp/t", [[], [30]])

        gc_collect()
        assert get("#{}/@ref_counter".format(store_chunk_id1)) == 2
        assert get("#{}/@ref_counter".format(hunk_chunk_id1)) == 3
        assert get("#{}/@ref_counter".format(store_chunk_id2)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id2)) == 2

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

    @authors("ifsmirnov")
    def test_reshard_with_tablet_count(self):
        sync_create_cells(1)
        self._create_table()

        sync_reshard_table("//tmp/t", [[], [30]])
        sync_mount_table("//tmp/t")
        rows = [
            {"key": i, "value": "value" + str(i) + "x" * 20}
            for i in [0, 10, 20, 30, 40, 50]]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        # Expel chunks from Eden.
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", 1)
        assert get("//tmp/t/@tablet_state") == "unmounted"
        assert get("//tmp/t/@tablet_count") == 1

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", 2)
        assert get("//tmp/t/@tablet_state") == "unmounted"
        assert get("//tmp/t/@tablet_count") == 2

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_compaction_writes_hunk_chunk(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, max_inline_hunk_size=1000, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=10))

        chunk_ids_before_compaction = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction) == 1
        chunk_id_before_compaction = chunk_ids_before_compaction[0]
        assert get("#{}/@hunk_chunk_refs".format(chunk_id_before_compaction)) == []

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 0
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 0
        assert hunk_statistics["referenced_hunk_count"] == 0
        assert hunk_statistics["total_hunk_length"] == 0
        assert hunk_statistics["total_referenced_hunk_length"] == 0

        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_ids") != chunk_ids_before_compaction)
        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        compacted_store_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        assert_items_equal(get("#{}/@hunk_chunk_refs".format(compacted_store_id)), [
            {"chunk_id": hunk_chunk_id, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec},
        ])

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 10
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_hunk_length"] == 260
        assert hunk_statistics["total_referenced_hunk_length"] == 260

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_compaction_inlines_hunks(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format,
                           max_inline_hunk_size=10,
                           hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=1000))

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec},
        ])

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 10
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_hunk_length"] == 260
        assert hunk_statistics["total_referenced_hunk_length"] == 260

        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)
        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == []

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 0
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 0
        assert hunk_statistics["referenced_hunk_count"] == 0
        assert hunk_statistics["total_hunk_length"] == 0
        assert hunk_statistics["total_referenced_hunk_length"] == 0

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_compaction_rewrites_hunk_chunk(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, max_inline_hunk_size=10, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        keys = [{"key": i} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id0 = hunk_chunk_ids[0]
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id0, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": hunk_erasure_codec},
        ])

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 10
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_hunk_length"] == 260
        assert hunk_statistics["total_referenced_hunk_length"] == 260

        sync_mount_table("//tmp/t")
        delete_rows("//tmp/t", keys[1:])
        sync_unmount_table("//tmp/t")

        sync_mount_table("//tmp/t")

        chunk_ids_before_compaction1 = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction1) == 3

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 1)
        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check1():
            chunk_ids = get("//tmp/t/@chunk_ids")
            return chunk_ids != chunk_ids_before_compaction1 and len(chunk_ids) == 2
        wait(_check1)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id1 = hunk_chunk_ids[0]
        assert hunk_chunk_id0 == hunk_chunk_id1
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 1, "total_hunk_length": 26, "erasure_codec": hunk_erasure_codec},
        ])

        chunk_ids_before_compaction2 = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction2) == 2

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 10
        assert hunk_statistics["referenced_hunk_count"] == 1
        assert hunk_statistics["total_hunk_length"] == 260
        assert hunk_statistics["total_referenced_hunk_length"] == 26

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check2():
            chunk_ids = get("//tmp/t/@chunk_ids")
            return chunk_ids != chunk_ids_before_compaction2 and len(chunk_ids) == 2
        wait(_check2)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id2 = hunk_chunk_ids[0]
        assert hunk_chunk_id1 != hunk_chunk_id2
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id2, "hunk_count": 1, "total_hunk_length": 26, "erasure_codec": hunk_erasure_codec},
        ])
        assert get("#{}/@hunk_count".format(hunk_chunk_id2)) == 1
        assert get("#{}/@total_hunk_length".format(hunk_chunk_id2)) == 26

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["hunk_chunk_count"] == 1
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["hunk_count"] == 1
        assert hunk_statistics["referenced_hunk_count"] == 1
        assert hunk_statistics["total_hunk_length"] == 26
        assert hunk_statistics["total_referenced_hunk_length"] == 26

    @authors("ifsmirnov")
    @pytest.mark.parametrize("in_memory_mode", ["compressed", "uncompressed"])
    def test_hunks_not_counted_in_tablet_static(self, in_memory_mode):
        def _check_account_resource_usage(expected_memory_usage):
            return \
                get("//sys/accounts/tmp/@resource_usage/tablet_static_memory") == expected_memory_usage and \
                get("//sys/tablet_cell_bundles/default/@resource_usage/tablet_static_memory") == expected_memory_usage

        sync_create_cells(1)
        self._create_table()

        set("//tmp/t/@in_memory_mode", in_memory_mode)
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        compressed_size = get("#{}/@compressed_data_size".format(store_chunk_id))
        uncompressed_size = get("#{}/@uncompressed_data_size".format(store_chunk_id))
        hunk_compressed_size = get("#{}/@compressed_data_size".format(hunk_chunk_id))
        hunk_uncompressed_size = get("#{}/@uncompressed_data_size".format(hunk_chunk_id))

        def _validate_tablet_statistics():
            tablet_statistics = get("//tmp/t/@tablet_statistics")
            return \
                tablet_statistics["compressed_data_size"] == compressed_size + hunk_compressed_size and \
                tablet_statistics["uncompressed_data_size"] == uncompressed_size + hunk_uncompressed_size and \
                tablet_statistics["hunk_compressed_data_size"] == hunk_compressed_size and \
                tablet_statistics["hunk_uncompressed_data_size"] == hunk_uncompressed_size
        wait(_validate_tablet_statistics)

        memory_size = compressed_size if in_memory_mode == "compressed" else uncompressed_size

        assert get("//tmp/t/@tablet_statistics/memory_size") == memory_size

        wait(lambda: _check_account_resource_usage(memory_size))

        sync_unmount_table("//tmp/t")
        wait(lambda: _check_account_resource_usage(0))

    @authors("gritukan")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_type", ["inline", "chunk"])
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_hunks_in_operation(self, chunk_format, hunk_type, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")

        if hunk_type == "inline":
            rows = [{"key": i, "value": "value" + str(i)} for i in range(10)]
        else:
            rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        if hunk_type == "inline":
            assert len(hunk_chunk_ids) == 0
        else:
            assert len(hunk_chunk_ids) == 1

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    def test_lookup_any_value_with_hunks(self, chunk_format):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "max_inline_hunk_size": 10},
        ]

        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, schema=schema)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", self.ROWS_WITH_VARIOUS_ANY_VALUES)
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", self.KEYS_OF_ROWS_WITH_VARIOUS_ANY_VALUES) == self.ROWS_WITH_VARIOUS_ANY_VALUES

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_any_value_with_hunks_from_static(self, optimize_for):
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "any", "max_inline_hunk_size": 10},
            ],
            unique_keys=True,
        )

        create("table", "//tmp/t", attributes={
            "schema": schema,
            "optimize_for": optimize_for,
        })

        write_table("//tmp/t", self.ROWS_WITH_VARIOUS_ANY_VALUES)
        assert read_table("//tmp/t") == self.ROWS_WITH_VARIOUS_ANY_VALUES

        alter_table("//tmp/t", dynamic=True)

        sync_create_cells(1)
        sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", self.KEYS_OF_ROWS_WITH_VARIOUS_ANY_VALUES) == self.ROWS_WITH_VARIOUS_ANY_VALUES
        assert read_table("//tmp/t") == self.ROWS_WITH_VARIOUS_ANY_VALUES

    @authors("gritukan")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_type", ["inline", "chunk"])
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_hunks_in_operation_any_value(self, chunk_format, hunk_type, hunk_erasure_codec):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "max_inline_hunk_size": 10},
        ]

        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, schema=schema, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")

        if hunk_type == "inline":
            rows = [{"key": i, "value": "value" + str(i)} for i in range(10)]
        else:
            rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        if hunk_type == "inline":
            assert len(hunk_chunk_ids) == 0
        else:
            assert len(hunk_chunk_ids) == 1

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("babenko")
    @pytest.mark.parametrize("chunk_format", HUNK_COMPATIBLE_CHUNK_FORMATS)
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_alter_to_hunks(self, chunk_format, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(chunk_format=chunk_format, max_inline_hunk_size=None, hunk_erasure_codec=hunk_erasure_codec)
        if chunk_format == "table_versioned_indexed":
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 0

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=10))

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10, 20)]
        insert_rows("//tmp/t", rows2)

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 2
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

    @authors("babenko")
    def test_alter_must_preserve_hunks(self):
        self._create_table()
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=None))

    @authors("akozhikhov")
    def test_hunks_with_filtered_columns(self):
        sync_create_cells(1)
        self._create_table()
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        def _check(column_names):
            row = [{column: rows[0][column] for column in column_names}]
            assert_items_equal(
                lookup_rows("//tmp/t", [{"key": 0}], column_names=column_names),
                row)
            assert_items_equal(
                select_rows("{} from [//tmp/t] where value = \"{}\"".format(
                    ", ".join(column_names),
                    rows[0]["value"])),
                row)

        _check(["key"])
        _check(["value"])
        _check(["value", "key"])

    @authors("akozhikhov")
    @pytest.mark.skip(reason="Flaky profiling tests")
    @pytest.mark.parametrize("enable_hunk_columnar_profiling", [False, True])
    def test_hunks_profiling_flush(self, enable_hunk_columnar_profiling):
        sync_create_cells(1)
        self._create_table()
        if enable_hunk_columnar_profiling:
            set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/data_weight",
            tags={"method": "store_flush"})
        hunk_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/hunks/data_weight",
            tags={"method": "store_flush"})

        if enable_hunk_columnar_profiling:
            inline_hunk_value_count = self._init_tablet_sensor(
                "//tmp/t",
                "chunk_writer/hunks/inline_value_count",
                tags={"column": "value", "method": "store_flush"})
            ref_hunk_value_count = self._init_tablet_sensor(
                "//tmp/t",
                "chunk_writer/hunks/ref_value_count",
                tags={"column": "value", "method": "store_flush"})

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)

        sync_flush_table("//tmp/t")

        wait(lambda: chunk_data_weight.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        if enable_hunk_columnar_profiling:
            wait(lambda: inline_hunk_value_count.get_delta() == 10)
            wait(lambda: ref_hunk_value_count.get_delta() == 5)

    @authors("akozhikhov")
    @pytest.mark.skip(reason="Flaky profiling tests")
    def test_hunks_profiling_compaction(self):
        sync_create_cells(1)
        self._create_table(max_inline_hunk_size=10)
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        reader_hunk_chunk_transmitted = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_reader/hunks/chunk_reader_statistics/data_bytes_transmitted",
            tags={"method": "compaction"})
        reader_hunk_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_reader/hunks/data_weight",
            tags={"method": "compaction"})

        reader_inline_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_reader/hunks/inline_value_count",
            tags={"column": "value"})
        reader_ref_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_reader/hunks/ref_value_count",
            tags={"column": "value"})

        writer_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/data_weight",
            tags={"method": "compaction"})
        writer_hunk_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/hunks/data_weight",
            tags={"method": "compaction"})

        writer_inline_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/hunks/inline_value_count",
            tags={"column": "value", "method": "compaction"})
        writer_ref_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "chunk_writer/hunks/ref_value_count",
            tags={"column": "value", "method": "compaction"})

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=30))
        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: reader_hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: reader_hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: reader_inline_hunk_value_count.get_delta() == 10)
        wait(lambda: reader_ref_hunk_value_count.get_delta() == 5)

        wait(lambda: writer_chunk_data_weight.get_delta() > 0)
        assert writer_hunk_chunk_data_weight.get_delta() == 0

        wait(lambda: writer_inline_hunk_value_count.get_delta() == 15)
        assert writer_ref_hunk_value_count.get_delta() == 0

    @authors("akozhikhov")
    @pytest.mark.skip(reason="Flaky profiling tests")
    def test_hunks_profiling_lookup(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        hunk_chunk_transmitted = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/chunk_reader_statistics/data_bytes_transmitted")
        hunk_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/data_weight")

        inline_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/inline_value_count")
        ref_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/ref_value_count")

        columnar_inline_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/inline_value_count",
            tags={"column": "value"})
        columnar_ref_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/ref_value_count",
            tags={"column": "value"})

        backend_read_request_count = self._init_tablet_sensor(
            "//tmp/t",
            "lookup/hunks/backend_read_request_count")

        keys1 = [{"key": i} for i in range(10)]
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5)]
        keys2 = [{"key": i} for i in range(10, 20)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_flush_table("//tmp/t")

        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        wait(lambda: hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: inline_hunk_value_count.get_delta() == 20)
        wait(lambda: ref_hunk_value_count.get_delta() == 10)

        wait(lambda: columnar_inline_hunk_value_count.get_delta() == 10)
        wait(lambda: columnar_ref_hunk_value_count.get_delta() == 5)

        # Multiple requests due to max_inflight_fragment_* constraints.
        wait(lambda: backend_read_request_count.get_delta() > 2)

    @authors("akozhikhov")
    @pytest.mark.skip(reason="Flaky profiling tests")
    def test_hunks_profiling_select(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in range(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_flush_table("//tmp/t")

        hunk_chunk_transmitted = self._init_tablet_sensor(
            "//tmp/t",
            "select/hunks/chunk_reader_statistics/data_bytes_transmitted")
        hunk_chunk_data_weight = self._init_tablet_sensor(
            "//tmp/t",
            "select/hunks/data_weight")

        inline_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "select/hunks/inline_value_count",
            tags={"column": "value"})
        ref_hunk_value_count = self._init_tablet_sensor(
            "//tmp/t",
            "select/hunks/ref_value_count",
            tags={"column": "value"})

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        wait(lambda: hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: inline_hunk_value_count.get_delta() == 10)
        wait(lambda: ref_hunk_value_count.get_delta() == 5)

    @authors("ifsmirnov")
    def test_small_hunk_compaction(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@max_hunk_compaction_chunk_count", 2)
        sync_mount_table("//tmp/t")

        for i in range(3):
            insert_rows("//tmp/t", [{"key": i, "value": "verylongvalue" + str(i)}])
            sync_flush_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 1 + 2)
        (store_chunk_id, ) = self._get_store_chunk_ids("//tmp/t")
        assert len(get("#{}/@hunk_chunk_refs".format(store_chunk_id))) == 2

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_count") == 1 + 1)
        (store_chunk_id, ) = self._get_store_chunk_ids("//tmp/t")
        assert len(get("#{}/@hunk_chunk_refs".format(store_chunk_id))) == 1

    @authors("akozhikhov")
    def test_no_small_hunk_compaction(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@max_hunk_compaction_size", 5)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 0, "value": "0" * 20}])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1" * 20}])
        sync_flush_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 2
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 2

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check():
            new_store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
            if len(new_store_chunk_ids) != 1:
                return False
            return new_store_chunk_ids[0] not in store_chunk_ids

        wait(lambda: _check())

        assert hunk_chunk_ids == self._get_hunk_chunk_ids("//tmp/t")

    @authors("akozhikhov")
    def test_hunk_chunks_orchid(self):
        sync_create_cells(1)
        self._create_table()
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 0, "value": "0" * 20}])
        sync_flush_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1

        tablet_info = get("//tmp/t/@tablets/0")
        hunk_chunks_info = get("//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}/hunk_chunks/{}".format(
            tablet_info["cell_leader_address"],
            tablet_info["cell_id"],
            tablet_info["tablet_id"],
            hunk_chunk_ids[0],
        ))

        assert hunk_chunks_info["referenced_total_hunk_length"] == 20
        assert hunk_chunks_info["total_hunk_length"] == 20
        assert hunk_chunks_info["hunk_count"] == 1
        assert hunk_chunks_info["referenced_hunk_count"] == 1
        assert hunk_chunks_info["store_ref_count"] == 1
        assert not hunk_chunks_info["dangling"]

    @authors("babenko")
    def test_hunk_erasure_codec_cannot_be_set_for_nontable(self):
        create("file", "//tmp/f")
        assert not exists("//tmp/f/@hunk_erasure_codec")
        with pytest.raises(YtError):
            set("//tmp/f/@hunk_erasure_codec", "isa_lrc_12_2_2")

    @authors("babenko", "gritukan")
    def test_hunk_erasure_codec_for_table(self):
        create("table", "//tmp/t")

        assert get("//tmp/t/@hunk_erasure_codec") == "none"

        set("//tmp/t/@hunk_erasure_codec", "isa_lrc_12_2_2")
        assert get("//tmp/t/@hunk_erasure_codec") == "isa_lrc_12_2_2"

        tx = start_transaction()
        set("//tmp/t/@hunk_erasure_codec", "reed_solomon_3_3", tx=tx)
        assert get("//tmp/t/@hunk_erasure_codec") == "isa_lrc_12_2_2"
        assert get("//tmp/t/@hunk_erasure_codec", tx=tx) == "reed_solomon_3_3"

        commit_transaction(tx)
        assert get("//tmp/t/@hunk_erasure_codec") == "reed_solomon_3_3"

        set("//tmp/t/@hunk_erasure_codec", "none")
        assert get("//tmp/t/@hunk_erasure_codec") == "none"

    @authors("babenko")
    def test_hunk_erasure_codec_inheritance(self):
        create("map_node", "//tmp/m")
        set("//tmp/m/@hunk_erasure_codec", "isa_lrc_12_2_2")

        create("file", "//tmp/m/f")
        assert not exists("//tmp/m/f/@hunk_erasure_codec")

        create("table", "//tmp/m/t")
        assert get("//tmp/m/t/@hunk_erasure_codec") == "isa_lrc_12_2_2"

    @authors("akozhikhov")
    def test_hedging_manager_sensors(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@hunk_chunk_reader/hedging_manager", {"secondary_request_ratio": 0.5})
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(5)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        request_counter = self._init_tablet_sensor(
            "//tmp/t",
            "hunks/hedging_manager/primary_request_count")

        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        time.sleep(1)

        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        wait(lambda: request_counter.get_delta() > 0)

    BLOB_HUNK_PAYLOAD = [
        {"payload": b"abcdefghijklmnopqrstuvwxyz"}
    ]

    def _write_blob_hunks(self, erasure_codec):
        create("file", "//tmp/f", attributes={
            "erasure_codec": erasure_codec,
            "enable_striped_erasure": True
        })
        assert len(self.BLOB_HUNK_PAYLOAD) == 1
        write_file("//tmp/f", self.BLOB_HUNK_PAYLOAD[0]["payload"])
        return get_singular_chunk_id("//tmp/f")

    def _prepare_hunk_read_requests(self, requests, erasure_codec, payload):
        if erasure_codec != "none":
            for request in requests:
                request["erasure_codec"] = erasure_codec
                if "block_size" not in request:
                    request["block_size"] = len(payload[request["block_index"]]["payload"])

    @authors("babenko")
    @pytest.mark.parametrize("erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_blob_hunk_read(self, erasure_codec):
        chunk_id = self._write_blob_hunks(erasure_codec)
        requests = [
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 0, "length": 26},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 5, "length": 0},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 10, "length": 3}
        ]
        self._prepare_hunk_read_requests(requests, erasure_codec, self.BLOB_HUNK_PAYLOAD)
        responses = read_hunks(requests, parse_header=False)
        assert len(responses) == 3
        assert responses[0]["payload"] == "abcdefghijklmnopqrstuvwxyz"
        assert responses[1]["payload"] == ""
        assert responses[2]["payload"] == "klm"

    @authors("babenko")
    @pytest.mark.parametrize("erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_blob_hunk_read_failed(self, erasure_codec):
        chunk_id = self._write_blob_hunks(erasure_codec)
        chunk_id = get_singular_chunk_id("//tmp/f")
        requests = [
            {"chunk_id": "1-2-3-4", "block_index": 0, "block_offset": 0, "length": 0},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": -100, "length": 0},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 0, "length": -1},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 100, "length": 1},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 0, "length": 100},
            # TODO(babenko,gritukan): consider failing these (zero-length!) requests.
            # {"chunk_id": chunk_id, "block_index": 1, "block_offset": 0, "length": 0},
            # {"chunk_id": chunk_id, "block_index": 0, "block_offset": 100, "length": 0},
        ]
        self._prepare_hunk_read_requests(requests, erasure_codec, self.BLOB_HUNK_PAYLOAD)
        for request in requests:
            with pytest.raises(YtError):
                read_hunks([request], parse_header=False)

    JOURNAL_HUNK_PAYLOAD = [
        {"payload": "abcdefghijklmnopqrstuvwxyz"},
        {"payload": "0123456789"},
        {"payload": "abababababababab"}
    ]

    def _write_journal_hunks(self, erasure_codec, replication_factor, read_quorum, write_quorum):
        create("journal", "//tmp/j", attributes={
            "erasure_codec": erasure_codec,
            "replication_factor": replication_factor,
            "read_quorum": read_quorum,
            "write_quorum": write_quorum
        })
        write_journal("//tmp/j", self.JOURNAL_HUNK_PAYLOAD, enable_chunk_preallocation=False)
        return get_singular_chunk_id("//tmp/j")

    @authors("babenko")
    @pytest.mark.parametrize("erasure_codec,replication_factor,read_quorum,write_quorum",
                             [("none", 3, 2, 2), ("isa_reed_solomon_3_3", 1, 4, 5)])
    def test_journal_hunk_read(self, erasure_codec, replication_factor, read_quorum, write_quorum):
        chunk_id = self._write_journal_hunks(erasure_codec, replication_factor, read_quorum, write_quorum)
        requests = [
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 0, "length": 26},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 3, "length": 4},
            {"chunk_id": chunk_id, "block_index": 1, "block_offset": 2, "length": 5},
            {"chunk_id": chunk_id, "block_index": 2, "block_offset": 1, "length": 1},
            {"chunk_id": chunk_id, "block_index": 1, "block_offset": 7, "length": 0}
        ]
        self._prepare_hunk_read_requests(requests, erasure_codec, self.JOURNAL_HUNK_PAYLOAD)
        responses = read_hunks(requests, parse_header=False)
        assert len(responses) == 5
        assert responses[0]["payload"] == "abcdefghijklmnopqrstuvwxyz"
        assert responses[1]["payload"] == "defg"
        assert responses[2]["payload"] == "23456"
        assert responses[3]["payload"] == "b"
        assert responses[4]["payload"] == ""

    @authors("babenko")
    @pytest.mark.parametrize("erasure_codec,replication_factor,read_quorum,write_quorum",
                             [("none", 3, 2, 2), ("isa_reed_solomon_3_3", 1, 4, 5)])
    def test_journal_hunk_read_failed(self, erasure_codec, replication_factor, read_quorum, write_quorum):
        chunk_id = self._write_journal_hunks(erasure_codec, replication_factor, read_quorum, write_quorum)
        requests = [
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": -1, "length": 1},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 100, "length": 1},
            {"chunk_id": chunk_id, "block_index": 0, "block_offset": 0, "length": -1},
            {"chunk_id": chunk_id, "block_index": 100, "block_offset": 0, "length": 1, "block_size": 100},
            # TODO(babenko,gritukan): consider failing these (zero-length!) requests.
            # {"chunk_id": chunk_id, "block_index": 100, "block_offset": 0, "length": 0},
            # {"chunk_id": chunk_id, "block_index": 0, "block_offset": 100, "length": 0},
        ]
        self._prepare_hunk_read_requests(requests, erasure_codec, self.JOURNAL_HUNK_PAYLOAD)
        for request in requests:
            with pytest.raises(YtError):
                read_hunks([request], parse_header=False)

    @authors("akozhikhov")
    @pytest.mark.parametrize("chunk_format", ["table_unversioned_schemaless_horizontal", "table_unversioned_columnar"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed", "uncompressed"])
    @pytest.mark.parametrize("versioned", [False, True])
    @pytest.mark.parametrize("enable_lookup_hash_table", [False, True])
    def test_unversioned_lookup_all_formats(self, chunk_format, in_memory_mode, versioned, enable_lookup_hash_table):
        if enable_lookup_hash_table and in_memory_mode != "uncompressed":
            return

        self._create_table(chunk_format=chunk_format, max_inline_hunk_size=5, dynamic=False)

        rows = [{"key": 0, "value": "0"}, {"key": 1, "value": "1111111111"}]
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@in_memory_mode", in_memory_mode)
        set("//tmp/t/@enable_lookup_hash_table", enable_lookup_hash_table)
        set("//tmp/t/@optimize_for", "lookup")
        set("//tmp/t/@chunk_format", "table_versioned_simple")

        sync_create_cells(1)
        sync_mount_table("//tmp/t")
        if in_memory_mode != "none":
            wait(lambda: get("//tmp/t/@preload_state") == "complete")

        read_rows = lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}, {"key": 2}], versioned=versioned)
        if not versioned:
            assert read_rows == rows
        else:
            assert len(read_rows) == 2

            def _check_row(actual, expected):
                assert actual["key"] == expected["key"]
                value = actual["value"]
                assert len(value) == 1
                assert str(value[0]) == expected["value"]
            _check_row(read_rows[0], rows[0])

    @authors("akozhikhov")
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_3_3"])
    def test_fragment_prefetcher_and_block_cache(self, hunk_erasure_codec):
        sync_create_cells(1)

        self._create_table(hunk_erasure_codec=hunk_erasure_codec)
        set("//tmp/t/@hunk_chunk_reader/prefetch_whole_blocks", True)
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        update_nodes_dynamic_config({
            "data_node": {
                "block_cache": {
                    "chunk_fragments_data": {
                        "capacity": 10000000,
                    }
                }
            }
        })

        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        keys1 = [{"key": i} for i in range(10, 20)]
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10, 20)]
        insert_rows("//tmp/t", rows1)
        sync_flush_table("//tmp/t")

        assert_items_equal(lookup_rows("//tmp/t", keys + keys1), rows + rows1)
        assert_items_equal(lookup_rows("//tmp/t", keys + keys1), rows + rows1)

    @authors("akozhikhov")
    @pytest.mark.skip(reason="Flaky throttler test")
    def test_fragment_request_cancelation(self):
        sync_create_cells(1)

        self._create_table(max_inline_hunk_size=1)
        set("//tmp/t/@chunk_reader", {
            "enable_local_throttling": True,
            "use_block_cache": False,
            "use_uncompressed_block_cache": False,
            "prefer_local_replicas": False,
        })
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "heavy" + "y" * 10000}, {"key": 1, "value": "light" + "t" * 10}]

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "enable_chunk_fragment_reader_throttling": True,
            },
            # As for now we don't have better way to tune total limit in dynamic config.
            "throttler_free_bandwidth_ratio": 0.9999995,
        })

        # Apply throttler to CFR.
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        with raises_yt_error() as err:
            lookup_rows("//tmp/t", [{"key": 0}], timeout=1000)
        assert err[0].contains_text("Operation timed out") or err[0].contains_text("Request timed out")

        # Bytes throttled within first lookup call have been released by now.
        assert lookup_rows("//tmp/t", [{"key": 1}], timeout=1000) == rows[1:]

    @authors("akozhikhov")
    def test_read_from_sorted_dynamic_store_with_hunks(self):
        sync_create_cells(1)
        self._create_table(enable_dynamic_store_read=True)

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert read_table("//tmp/t") == rows


################################################################################


class TestOrderedDynamicTablesHunks(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 7

    # Need some extra nodes for erasure repair.
    NUM_NODES = 12

    NUM_SCHEDULERS = 1

    SCHEMA = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string"},
    ]

    # Do not allow multiple erasure parts per node.
    DELTA_MASTER_CONFIG = {}

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "tablet_node": {
                "hunk_lock_manager": {
                    "unlock_check_period": 100
                }
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "max_blocks_per_read": 2,
        }
    }

    def _get_table_schema(self, schema, max_inline_hunk_size):
        if "sort_order" not in schema[1]:
            schema[1]["max_inline_hunk_size"] = max_inline_hunk_size
        return schema

    def _create_table(self,
                      path="//tmp/t",
                      optimize_for="lookup",
                      max_inline_hunk_size=10,
                      hunk_erasure_codec="none",
                      schema=SCHEMA,
                      enable_dynamic_store_read=False):
        self._create_simple_table(
            path,
            schema=self._get_table_schema(schema, max_inline_hunk_size),
            enable_dynamic_store_read=enable_dynamic_store_read,
            hunk_chunk_reader={
                "max_hunk_count_per_read": 2,
                "max_total_hunk_length_per_read": 60,
                "hedging_manager": {
                    "secondary_request_ratio": 0.5,
                    "max_hedging_delay": 1,
                },
            },
            hunk_chunk_writer={
                "desired_block_size": 50,
            },
            max_hunk_compaction_garbage_ratio=0.5,
            enable_lsm_verbose_logging=True,
            optimize_for=optimize_for,
            hunk_erasure_codec=hunk_erasure_codec)

    def _get_active_store_id(self, hunk_storage, tablet_index=0):
        tablets = get("{}/@tablets".format(hunk_storage))
        tablet_id = tablets[tablet_index]["tablet_id"]
        wait(lambda: exists("//sys/tablets/{}/orchid/active_store_id".format(tablet_id)))
        return get("//sys/tablets/{}/orchid/active_store_id".format(tablet_id))

    def _get_active_store_orchid(self, hunk_storage, tablet_index=0):
        tablets = get("{}/@tablets".format(hunk_storage))
        tablet_id = tablets[tablet_index]["tablet_id"]
        get("//sys/tablets/{}/orchid".format(tablet_id))

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_flush_inline(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i)} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        get("#{}/@chunk_format".format(chunk_ids[0]))

        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0])) == []

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_flush_to_hunk_chunk(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        hunk_store_id = self._get_active_store_id("//tmp/h")

        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        sync_unmount_table("//tmp/h")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]

        assert get("#{}/@ref_counter".format(store_chunk_id)) == 1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == [
            {"chunk_id": hunk_store_id, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": "none"}
        ]

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_referenced_hunk_length"] == 260

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(store_chunk_id)))

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("enable_dynamic_store_read", [True, False])
    def test_journal_hunk_chunk_parents(self, optimize_for, enable_dynamic_store_read):
        set("//sys/@config/tablet_manager/enable_dynamic_store_read_by_default", enable_dynamic_store_read)

        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)
        set("//tmp/t/@enable_dynamic_store_read", enable_dynamic_store_read)

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "store_rotation_period": 20000,
            "store_removal_grace_period": 4000,
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        hunk_store_id = self._get_active_store_id("//tmp/h")
        update_nodes_dynamic_config({
            "tablet_node": {
                "hunk_lock_manager": {
                    "hunk_store_extra_lifetime": 123,
                    "unlock_check_period": 127
                }
            }
        })

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]

        assert get("#{}/@ref_counter".format(store_chunk_id)) == 1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == [
            {"chunk_id": hunk_store_id, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": "none"}
        ]

        hunk_store_parents = get("#{}/@owning_nodes".format(hunk_store_id))
        assert sorted(hunk_store_parents) == ["//tmp/h", "//tmp/t"]

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        set("//tmp/h/@store_removal_grace_period", 100)
        set("//tmp/h/@store_rotation_period", 500)
        wait(lambda: get("#{}/@owning_nodes".format(hunk_store_id)) == ["//tmp/t"])

        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(store_chunk_id)))

    @authors("akozhikhov")
    def test_trim_with_hunk_chunk(self):
        def _get_journal_hunk_chunk_ids():
            chunk_ids = get("//tmp/t/@chunk_ids")
            store_chunk_ids = builtins.set(self._get_store_chunk_ids("//tmp/t"))
            hunk_chunk_ids = builtins.set([chunk_id for chunk_id in chunk_ids if chunk_id not in store_chunk_ids])
            return list(hunk_chunk_ids)

        sync_create_cells(1)
        self._create_table()

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "store_rotation_period": 2000,
            "store_removal_grace_period": 100,
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        sync_flush_table("//tmp/t")

        hunk_chunk_id = _get_journal_hunk_chunk_ids()
        assert len(hunk_chunk_id) == 1
        hunk_chunk_id = hunk_chunk_id[0]
        assert exists("#{}".format(hunk_chunk_id))

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))
        trim_rows("//tmp/t", 0, 10)
        wait(lambda: get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 0)

        wait(lambda: not exists("#{}".format(hunk_chunk_id)))

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_restart_preserves_locks(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "store_removal_grace_period": 100,
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        hunk_store_id = self._get_active_store_id("//tmp/h")
        update_nodes_dynamic_config({
            "tablet_node": {
                "hunk_lock_manager": {
                    "hunk_store_extra_lifetime": 123,
                    "unlock_check_period": 127
                }
            }
        })

        build_master_snapshots()
        with Restarter(self.Env, NODES_SERVICE):
            pass

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        sync_unmount_table("//tmp/h")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]

        assert get("#{}/@ref_counter".format(store_chunk_id)) == 1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == [
            {"chunk_id": hunk_store_id, "hunk_count": 10, "total_hunk_length": 260, "erasure_codec": "none"}
        ]

        hunk_statistics = get("//tmp/t/@hunk_statistics")
        assert hunk_statistics["store_chunk_count"] == 1
        assert hunk_statistics["referenced_hunk_count"] == 10
        assert hunk_statistics["total_referenced_hunk_length"] == 260

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(store_chunk_id)))

    @authors("akozhikhov")
    def test_read_from_erasure_hunk_storage_with_repair(self):
        self._separate_tablet_and_data_nodes()
        set("//sys/@config/chunk_manager/enable_chunk_replicator", False)

        sync_create_cells(1)
        self._create_table()
        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "store_rotation_period": 2000,
            "store_removal_grace_period": 100,
            "scan_backoff_period": 1000,
            "read_quorum": 4,
            "write_quorum": 5,
            "erasure_codec": "reed_solomon_3_3",
            "replication_factor": 1,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        hunk_store_id = self._get_active_store_id("//tmp/h")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._set_ban_for_chunk_parts([0, 1, 4], True, hunk_store_id)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        self._set_ban_for_chunk_parts([0, 1, 4], False, hunk_store_id)

        self._set_ban_for_chunk_parts([0, 1, 2, 4], True, hunk_store_id)
        with raises_yt_error("exceeded retry count limit"):
            assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        self._set_ban_for_chunk_parts([0, 1, 2, 4], False, hunk_store_id)

    @authors("akozhikhov")
    @pytest.mark.parametrize("available", [False, True])
    def test_repair_erasure_hunk_storage_chunk(self, available):
        set("//sys/@config/chunk_manager/enable_chunk_replicator", False)
        self._separate_tablet_and_data_nodes()

        sync_create_cells(1)
        self._create_table()
        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "store_rotation_period": 2000,
            "store_removal_grace_period": 100,
            "scan_backoff_period": 1000,
            "read_quorum": 4,
            "write_quorum": 5,
            "erasure_codec": "reed_solomon_3_3",
            "replication_factor": 1,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i

        hunk_store_id = self._get_active_store_id("//tmp/h")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        def _check_all_replicas_ok():
            replicas = get("#{}/@stored_replicas".format(hunk_store_id))
            if len(replicas) != 6:
                return False
            if not all(r.attributes["state"] == "sealed" for r in replicas):
                return False
            return True

        wait(_check_all_replicas_ok)

        if available:
            self._set_ban_for_chunk_parts([0, 1, 4], True, hunk_store_id)
            set("//sys/@config/chunk_manager/enable_chunk_replicator", True)
            wait(_check_all_replicas_ok)
            assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        else:
            self._set_ban_for_chunk_parts([0, 1, 2, 4], True, hunk_store_id)
            set("//sys/@config/chunk_manager/enable_chunk_replicator", True)
            time.sleep(5)
            assert not _check_all_replicas_ok()
            with raises_yt_error("exceeded retry count limit"):
                assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("akozhikhov")
    def test_remove_cell_with_attached_hunk_storage(self):
        cell_id = sync_create_cells(1)[0]

        create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })

        sync_mount_table("//tmp/h")

        assert get("//tmp/h/@tablet_state") == "mounted"
        assert get("#{}/@tablet_count".format(cell_id)) == 1
        assert len(get("#{}/@tablet_ids".format(cell_id))) == 1

        assert get("#{}/@total_statistics/tablet_count".format(cell_id)) == 1

        remove("#{}".format(cell_id))
        time.sleep(5)

        assert exists("#{}".format(cell_id))

        sync_unmount_table("//tmp/h")
        wait(lambda: not exists("#{}".format(cell_id)))

    @authors("akozhikhov")
    def test_altering_queue_to_static_is_forbidden(self):
        sync_create_cells(1)
        self._create_table()

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_unmount_table("//tmp/h")

        remove("//tmp/t/@hunk_storage_id")

        with raises_yt_error() as err:
            alter_table("//tmp/t", dynamic=False)

        assert err[0].contains_text("for a table that has hunks") or \
            err[0].contains_text("table contains unsealed hunk chunk")

    @authors("akozhikhov")
    def test_reshard_ordered_table_with_hunks(self):
        sync_create_cells(1)
        self._create_table()
        sync_reshard_table("//tmp/t", 5)

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        for i in range(5):
            self._insert_rows_with_hunk_storage(
                "//tmp/t",
                [{"$tablet_index": i, "key": i, "value": str(i) + "x" * 20}])

        sync_unmount_table("//tmp/t")
        sync_unmount_table("//tmp/h")

        def _get_chunk_ids():
            chunk_ids = get("//tmp/t/@chunk_ids")
            store_chunk_ids = builtins.set(self._get_store_chunk_ids("//tmp/t"))
            hunk_chunk_ids = builtins.set([chunk_id for chunk_id in chunk_ids if chunk_id not in store_chunk_ids])
            return store_chunk_ids, hunk_chunk_ids

        store_chunk_ids, hunk_chunk_ids = _get_chunk_ids()

        sync_reshard_table("//tmp/t", 3)
        new_store_chunk_ids, new_hunk_chunk_ids = _get_chunk_ids()
        assert store_chunk_ids == new_store_chunk_ids
        assert hunk_chunk_ids == new_hunk_chunk_ids

        sync_reshard_table("//tmp/t", 2, first_tablet_index=1, last_tablet_index=1)
        new_store_chunk_ids, new_hunk_chunk_ids = _get_chunk_ids()
        assert store_chunk_ids == new_store_chunk_ids
        assert hunk_chunk_ids == new_hunk_chunk_ids

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [
            {"$tablet_index": 0, "$row_index": 0, "key": 0, "value": str(0) + "x" * 20},
            {"$tablet_index": 1, "$row_index": 0, "key": 1, "value": str(1) + "x" * 20},
            {"$tablet_index": 3, "$row_index": 0, "key": 2, "value": str(2) + "x" * 20},
            {"$tablet_index": 3, "$row_index": 1, "key": 3, "value": str(3) + "x" * 20},
            {"$tablet_index": 3, "$row_index": 2, "key": 4, "value": str(4) + "x" * 20},
        ]

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        for i in range(4):
            self._insert_rows_with_hunk_storage(
                "//tmp/t",
                [{"$tablet_index": i, "key": i, "value": str(i) + "y" * 20}],
            )

        rows.extend([
            {"$tablet_index": 0, "$row_index": 1, "key": 0, "value": str(0) + "y" * 20},
            {"$tablet_index": 1, "$row_index": 1, "key": 1, "value": str(1) + "y" * 20},
            {"$tablet_index": 2, "$row_index": 0, "key": 2, "value": str(2) + "y" * 20},
            {"$tablet_index": 3, "$row_index": 3, "key": 3, "value": str(3) + "y" * 20},
        ])

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

    @authors("akozhikhov", "shakurov")
    def test_hunk_storage_force_unmount(self):
        sync_create_cells(1)
        self._create_table()

        hunk_storage_id = create(
            "hunk_storage",
            "//tmp/h",
            attributes={
                "store_rotation_period": 120000,
                "store_removal_grace_period": 120000,
            })

        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "a" * 100} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_unmount_table("//tmp/h")
        sync_mount_table("//tmp/h")

        sync_unmount_table("//tmp/h", force=True)

        sync_mount_table("//tmp/h")
        sync_unmount_table("//tmp/h", force=True)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

    @authors("akozhikhov")
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_6_3"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_from_ordered_dynamic_store_with_hunks(self, optimize_for, hunk_erasure_codec):
        sync_create_cells(1)
        self._create_table(enable_dynamic_store_read=True, optimize_for=optimize_for, hunk_erasure_codec=hunk_erasure_codec)

        if hunk_erasure_codec != "none":
            hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
                "scan_backoff_period": 1000,
                "read_quorum": 4,
                "write_quorum": 5,
                "erasure_codec": "reed_solomon_3_3",
                "replication_factor": 1,
            })
        else:
            hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
                "scan_backoff_period": 1000,
            })

        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        sync_mount_table("//tmp/h")

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
        assert read_table("//tmp/t") == rows

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("akozhikhov")
    def test_hunk_storage_seal_with_multiple_tables(self):
        sync_create_cells(1)
        self._create_table(path="//tmp/t1")
        self._create_table(path="//tmp/t2")

        hunk_storage_id = create(
            "hunk_storage",
            "//tmp/h",
            attributes={
                "store_rotation_period": 120000,
            })

        set("//tmp/t1/@hunk_storage_id", hunk_storage_id)
        set("//tmp/t2/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t1")
        sync_mount_table("//tmp/t2")

        rows = [{"key": 0, "value": "a" * 100} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t1", rows)
        self._insert_rows_with_hunk_storage("//tmp/t2", rows)

        sync_unmount_table("//tmp/t1")
        sync_unmount_table("//tmp/t2")

        sync_unmount_table("//tmp/h")

        chunk_ids_1 = builtins.set(get("//tmp/t1/@chunk_ids"))
        chunk_ids_2 = builtins.set(get("//tmp/t2/@chunk_ids"))
        journal_hunk_chunk_id = chunk_ids_1.intersection(chunk_ids_2)
        assert len(journal_hunk_chunk_id) == 1
        journal_hunk_chunk_id = journal_hunk_chunk_id.pop()

        wait(lambda: get("#{}/@sealed".format(journal_hunk_chunk_id)))

    @authors("akozhikhov")
    def test_pull_rows_with_hunks(self):
        sync_create_cells(1)

        self._create_table()
        set("//tmp/t/@dynamic_store_auto_flush_period", None)

        hunk_storage_id = create(
            "hunk_storage",
            "//tmp/h",
            attributes={
                "store_rotation_period": 120000,
            })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)
        # Smaller block because block size defines granularity of the underlying reader,
        # so we can actually test limiting by max_data_weight.
        set("//tmp/t/@chunk_writer", {"block_size": 10})

        sync_mount_table("//tmp/t")
        sync_mount_table("//tmp/h")

        rows = [{"key": 0, "value": "a" * 100} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i
        assert rows == pull_queue("//tmp/t", offset=0, partition_index=0)

        assert rows[:2] == pull_queue("//tmp/t", offset=0, partition_index=0, max_row_count=2)
        assert rows[1:3] == pull_queue("//tmp/t", offset=1, partition_index=0, max_row_count=2)

        assert rows[:5] == pull_queue("//tmp/t", offset=0, partition_index=0, max_data_weight=200)

    @authors("akozhikhov")
    def test_seal_after_queue_copy(self):
        sync_create_cells(1)
        self._create_table(path="//tmp/t")
        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "a" * 100} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = [chunk_id for chunk_id in get("//tmp/t/@chunk_ids") if chunk_id != store_chunk_id]
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]
        assert not get("#{}/@sealed".format(hunk_chunk_id))

        copy("//tmp/t", "//tmp/t2")
        assert get("//tmp/t/@chunk_count") == 1
        assert get("//tmp/t/@tablet_statistics/chunk_count") == 1
        assert get("//tmp/t2/@chunk_count") == 1
        assert get("//tmp/t2/@tablet_statistics/chunk_count") == 1

        sync_unmount_table("//tmp/h")

        wait(lambda: get("#{}/@sealed".format(hunk_chunk_id)))
        assert get("//tmp/t/@chunk_count") == 2
        assert get("//tmp/t/@tablet_statistics/chunk_count") == 2
        assert get("//tmp/t2/@chunk_count") == 2
        assert get("//tmp/t2/@tablet_statistics/chunk_count") == 2

    @authors("akozhikhov")
    def test_seal_after_queue_branch(self):
        sync_create_cells(1)
        self._create_table(path="//tmp/t")
        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes={
            "scan_backoff_period": 1000,
        })
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "a" * 100} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = [chunk_id for chunk_id in get("//tmp/t/@chunk_ids") if chunk_id != store_chunk_id]
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]
        assert not get("#{}/@sealed".format(hunk_chunk_id))

        tx = start_transaction()
        set("//tmp/t/@hunk_erasure_codec", "reed_solomon_3_3", tx=tx)
        copy("//tmp/t", "//tmp/t2", tx=tx)

        sync_unmount_table("//tmp/h")

        wait(lambda: get("#{}/@sealed".format(hunk_chunk_id)))
        assert get("//tmp/t/@chunk_count") == 2
        assert get("//tmp/t/@tablet_statistics/chunk_count") == 2

        commit_transaction(tx)

        assert get("//tmp/t/@chunk_count") == 2
        assert get("//tmp/t/@tablet_statistics/chunk_count") == 2

        assert get("//tmp/t2/@chunk_count") == 2
        assert get("//tmp/t2/@tablet_statistics/chunk_count") == 2

    @authors("akozhikhov")
    @pytest.mark.parametrize("hunk_erasure_codec", ["none", "isa_reed_solomon_3_3"])
    def test_prefetch_fragments_on_data_node(self, hunk_erasure_codec):
        sync_create_cells(1)

        self._create_table(path="//tmp/t")
        set("//tmp/t/@hunk_erasure_codec", hunk_erasure_codec)
        set("//tmp/t/@hunk_chunk_reader", {
            "read_and_cache_whole_blocks": True,
            "block_count_to_precache": 1,
            "max_hunk_count_per_read": 10,
            "max_total_hunk_length_per_read": 1000,
        })

        hunk_storage_attributes = {
            "store_rotation_period": 120000,
            "hunk_store_writer": {
                "max_record_size": 10,
            },
        }
        if hunk_erasure_codec != "none":
            hunk_storage_attributes["read_quorum"] = 4
            hunk_storage_attributes["write_quorum"] = 5
            hunk_storage_attributes["erasure_codec"] = hunk_erasure_codec
            hunk_storage_attributes["replication_factor"] = 1

        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes=hunk_storage_attributes)
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_reshard_table("//tmp/t", 2)
        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        assert get("//tmp/t/@tablet_count") == 2

        rows = [{"$tablet_index": random.randint(0, 1), "key": i, "value": "a" * (25 + i)} for i in range(25)]

        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        assert_items_equal(select_rows("key, value, [$tablet_index] from [//tmp/t]"), rows)

        update_nodes_dynamic_config({
            "data_node": {
                "block_cache": {
                    "chunk_fragments_data": {
                        "capacity": 10000000,
                    }
                }
            }
        })

        rows0 = [row for row in rows if row["$tablet_index"] == 0]
        rows1 = [row for row in rows if row["$tablet_index"] == 1]
        assert_items_equal(select_rows("key, value, [$tablet_index] from [//tmp/t] where [$tablet_index] = 0"), rows0)
        assert_items_equal(select_rows("key, value, [$tablet_index] from [//tmp/t]"), rows)
        assert_items_equal(select_rows("key, value, [$tablet_index] from [//tmp/t] where [$tablet_index] = 1"), rows1)
        assert_items_equal(select_rows("key, value, [$tablet_index] from [//tmp/t]"), rows)


################################################################################


class TestDynamicTablesHunkMedia(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_NODES = 10
    STORE_LOCATION_COUNT = 3
    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "tablet_node": {
                "hunk_lock_manager": {
                    "hunk_store_extra_lifetime": 100,
                    "unlock_check_period": 100
                }
            }
        }
    }

    NON_DEFAULT_MEDIUM_1 = "hdd1"
    NON_DEFAULT_MEDIUM_2 = "hdd2"

    SORTED_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string", "max_inline_hunk_size": 10},
    ]

    ORDERED_SCHEMA = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string", "max_inline_hunk_size": 10},
    ]

    @classmethod
    def on_masters_started(cls):
        super(TestDynamicTablesHunkMedia, cls).on_masters_started()
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM_1)
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM_2)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 3

        config["data_node"]["store_locations"][0]["medium_name"] = "default"
        config["data_node"]["store_locations"][1]["medium_name"] = cls.NON_DEFAULT_MEDIUM_1
        config["data_node"]["store_locations"][2]["medium_name"] = cls.NON_DEFAULT_MEDIUM_2

    @classmethod
    def setup_class(cls):
        super(TestDynamicTablesHunkMedia, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, cls.NON_DEFAULT_MEDIUM_1)
        set_account_disk_space_limit("tmp", disk_space_limit, cls.NON_DEFAULT_MEDIUM_2)

        set("//sys/accounts/tmp/@resource_limits/tablet_count", 1000)

    def setup_method(self, method):
        super(TestDynamicTablesHunkMedia, self).setup_method(method)

        sync_create_cells(1)

    def _create_sorted_dynamic_table(self, path, **attributes):
        return create_dynamic_table(
            path,
            schema=self.SORTED_SCHEMA,
            enable_dynamic_store_read=False,
            hunk_chunk_reader={
                "max_hunk_count_per_read": 2,
                "max_total_hunk_length_per_read": 60,
                "hedging_manager": {
                    "secondary_request_ratio": 0.5,
                    "max_hedging_delay": 1,
                },
                "max_inflight_fragment_length": 60,
                "max_inflight_fragment_count": 2,
            },
            hunk_chunk_writer={
                "desired_block_size": 50,
            },
            max_hunk_compaction_garbage_ratio=0.5,
            enable_lsm_verbose_logging=True,
            chunk_format="table_versioned_simple",
            hunk_erasure_codec="none",
            **attributes
        )

    def _create_ordered_dynamic_table(self, path, **attributes):
        return create_dynamic_table(
            path,
            schema=self.ORDERED_SCHEMA,
            enable_dynamic_store_read=False,
            hunk_chunk_reader={
                "max_hunk_count_per_read": 2,
                "max_total_hunk_length_per_read": 60,
                "hedging_manager": {
                    "secondary_request_ratio": 0.5,
                    "max_hedging_delay": 1,
                },
            },
            hunk_chunk_writer={
                "desired_block_size": 50,
            },
            max_hunk_compaction_garbage_ratio=0.5,
            enable_lsm_verbose_logging=True,
            optimize_for="lookup",
            hunk_erasure_codec="none",
            **attributes)

    def _init_sorted_dynamic_table(self, path, **attributes):
        self._create_sorted_dynamic_table(path, **attributes)
        sync_mount_table(path)

    def _init_ordered_dynamic_table(self, table_path, **attributes):
        hunk_storage_path = f"{table_path}_hunk_storage"
        self._create_ordered_dynamic_table(table_path, **attributes)
        hunk_storage_id = create(
            "hunk_storage",
            hunk_storage_path,
            attributes={
                "store_rotation_period": 120000,
                "store_removal_grace_period": 120000,
            })
        set(f"{table_path}/@hunk_storage_id", hunk_storage_id)
        sync_mount_table(hunk_storage_path)
        sync_mount_table(table_path)

    def _speed_up_hunk_storage_store_rotation(self, table_path):
        hunk_storage_path = f"{table_path}_hunk_storage"
        set(f"{hunk_storage_path}/@store_rotation_period", 300)
        set(f"{hunk_storage_path}/@store_removal_grace_period", 300)

    def _get_main_chunk_id(self, table_path):
        for chunk_id in get(f"{table_path}/@chunk_ids"):
            if get(f"#{chunk_id}/@chunk_type") == "table":
                return chunk_id
        return None

    def _get_hunk_chunk_id(self, table_path):
        table_schema = get(f"{table_path}/@schema")
        if table_schema[0].get("sort_order") == "ascending":
            for chunk_id in get(f"{table_path}/@chunk_ids"):
                if get(f"#{chunk_id}/@chunk_type") == "hunk":
                    return chunk_id
            return None
        else:
            hunk_storage_path = f"{table_path}_hunk_storage"
            assert exists(hunk_storage_path)
            tablet_ids = get(f"{hunk_storage_path}/@tablets")
            tablet_id = tablet_ids[0]["tablet_id"]
            tablet_orchid_store_id_path = f"//sys/tablets/{tablet_id}/orchid/active_store_id"
            wait(lambda: exists(tablet_orchid_store_id_path))
            return get(tablet_orchid_store_id_path)

    def _chunk_has_requisition(self, chunk_id, expected_requisition):
        requisition = get(f"#{chunk_id}/@requisition")
        key = itemgetter("account", "medium", "committed")
        return sorted(requisition, key=key) == sorted(expected_requisition, key=key)

    @authors("shakurov")
    @pytest.mark.parametrize("init_node", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_media_attributes_at_creation(self, init_node):
        init_node = getattr(self, init_node)

        init_node("//tmp/t1", hunk_primary_medium=self.NON_DEFAULT_MEDIUM_1)
        assert get("//tmp/t1/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1

        with raises_yt_error("Cannot modify hunk_media since hunk_primary_medium is not set, consider setting it first"):
            init_node("//tmp/t2", hunk_media={"default": {"replication_factor": 5, "data_parts_only": False}})

        with raises_yt_error("Cannot remove primary medium"):
            init_node(
                "//tmp/t2",
                hunk_primary_medium=self.NON_DEFAULT_MEDIUM_1,
                hunk_media={"default": {"replication_factor": 5, "data_parts_only": True}})

        init_node(
            "//tmp/t2",
            hunk_primary_medium=self.NON_DEFAULT_MEDIUM_1,
            hunk_media={
                self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 3, "data_parts_only": False},
                "default": {"replication_factor": 5, "data_parts_only": True}})
        assert get("//tmp/t2/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1
        assert get("//tmp/t2/@hunk_media") == {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 3, "data_parts_only": False},
            "default": {"replication_factor": 5, "data_parts_only": True}}

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_media_attributes_inherited(self, init_table):
        init_table = getattr(self, init_table)

        set("//tmp/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)

        init_table("//tmp/a")
        assert get("//tmp/a/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1

        hunk_media = {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 7, "data_parts_only": False},
            "default": {"replication_factor": 4, "data_parts_only": True}
        }
        set("//tmp/@hunk_media", hunk_media)

        init_table("//tmp/b")
        assert get("//tmp/b/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1
        assert get("//tmp/b/@hunk_media") == hunk_media

    @authors("kvk1920")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_transferrable_hunk_media_attributes(self, init_table):
        init_table = getattr(self, init_table)

        default_medium_id = get("//sys/media/default/@id")
        non_default_medium_id_1 = get(f"//sys/media/{self.NON_DEFAULT_MEDIUM_1}/@id")
        non_default_medium_id_2 = get(f"//sys/media/{self.NON_DEFAULT_MEDIUM_2}/@id")

        set("//tmp/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)

        init_table("//tmp/a")
        assert get("//tmp/a/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1
        assert get("//tmp/a/@hunk_primary_medium_id") == non_default_medium_id_1

        hunk_media = {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 7, "data_parts_only": False},
            "default": {"replication_factor": 4, "data_parts_only": True}
        }
        transferable_hunk_media = {
            f"#{non_default_medium_id_1}": {"replication_factor": 7, "data_parts_only": False},
            f"#{default_medium_id}": {"replication_factor": 4, "data_parts_only": True},
        }
        set("//tmp/@hunk_media", transferable_hunk_media)

        init_table("//tmp/b")
        assert get("//tmp/b/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1
        assert get("//tmp/b/@hunk_primary_medium_id") == non_default_medium_id_1
        assert get("//tmp/b/@hunk_media") == hunk_media
        assert get("//tmp/b/@transferable_hunk_media") == transferable_hunk_media

        explicit_hunk_media = {"default": {"replication_factor": 5, "data_parts_only": False}}
        explicit_transferable_hunk_media = {
            f"#{default_medium_id}": {
                "replication_factor": 5,
                "data_parts_only": False,
            },
        }
        with raises_yt_error(f"Cannot remove primary medium \"{self.NON_DEFAULT_MEDIUM_1}\""):
            init_table("//tmp/c", hunk_media=explicit_hunk_media)
        with raises_yt_error(f"Cannot remove primary medium \"{self.NON_DEFAULT_MEDIUM_1}\""):
            init_table("//tmp/c", hunk_media=explicit_transferable_hunk_media)

        # TODO(kvk1920): YT-15704. Replace with self.NON_DEFAULT_MEDIUM_1.
        with raises_yt_error(f"Cannot remove primary medium \"{self.NON_DEFAULT_MEDIUM_2}\""):
            init_table("//tmp/c", hunk_primary_medium=self.NON_DEFAULT_MEDIUM_2)
        with raises_yt_error(f"Cannot remove primary medium \"{self.NON_DEFAULT_MEDIUM_2}\""):
            init_table("//tmp/c", hunk_primary_medium=f"#{non_default_medium_id_2}")

        expected_attributes = {
            "hunk_primary_medium": "default",
            "hunk_primary_medium_id": default_medium_id,
            "hunk_media": explicit_hunk_media,
            "transferable_hunk_media": explicit_transferable_hunk_media,
        }

        def check_table_attributes(table):
            assert get(f"{table}/@", attributes=[
                "hunk_primary_medium",
                "hunk_primary_medium_id",
                "hunk_media",
                "transferable_hunk_media",
            ]) == expected_attributes

        init_table("//tmp/c", hunk_primary_medium="default", hunk_media=explicit_hunk_media)
        check_table_attributes("//tmp/c")

        init_table(
            "//tmp/d",
            hunk_primary_medium=f"#{default_medium_id}",
            hunk_media=explicit_transferable_hunk_media)
        check_table_attributes("//tmp/d")

        init_table(
            "//tmp/e",
            hunk_primary_medium="default",
            hunk_media=explicit_hunk_media)
        check_table_attributes("//tmp/e")

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_media_attributes_survive_restart(self, init_table):
        init_table = getattr(self, init_table)

        init_table("//tmp/t")
        assert not exists("//tmp/t/@hunk_media")

        sync_unmount_table("//tmp/t")

        hunk_media = {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 5, "data_parts_only": False}
        }
        set("//tmp/t/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)
        set("//tmp/t/@hunk_media", hunk_media)
        sync_mount_table("//tmp/t")

        build_master_snapshots()
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get("//tmp/t/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1
        assert get("//tmp/t/@hunk_media") == hunk_media

    @authors("shakurov")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_primary_medium_attribute(self, init_table):
        init_table = getattr(self, init_table)

        create_user("u")

        init_table("//tmp/q")

        assert not exists("//tmp/q/@hunk_primary_medium")
        assert not exists("//tmp/q/@hunk_media")

        # 1. Set.

        with raises_yt_error("Cannot change storage parameters since not all tablets are unmounted"):
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)

        sync_unmount_table("//tmp/q")

        with raises_yt_error("No such medium"):
            set("//tmp/q/@hunk_primary_medium", "nonexistent")

        with raises_yt_error("Operation cannot be performed in transaction"):
            tx = start_transaction()
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1, tx=tx)

        with raises_yt_error("Access denied for user \"u\""):
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1, authenticated_user="u")

        set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)
        media = {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 5, "data_parts_only": False},
            "default": {"replication_factor": 3, "data_parts_only": True},
        }
        set("//tmp/q/@hunk_media", media)

        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_1

        # 2. Modify.

        with raises_yt_error("Cannot change storage parameters since not all tablets are unmounted"):
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2)

        sync_unmount_table("//tmp/q")

        with raises_yt_error("No such medium"):
            set("//tmp/q/@hunk_primary_medium", "nonexistent")

        with raises_yt_error("Operation cannot be performed in transaction"):
            tx = start_transaction()
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2, tx=tx)

        with raises_yt_error("Access denied for user \"u\""):
            set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2, authenticated_user="u")

        with raises_yt_error("Medium \"default\" stores no parity parts and cannot be made primary"):
            set("//tmp/q/@hunk_primary_medium", "default")

        set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2)

        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@hunk_primary_medium") == self.NON_DEFAULT_MEDIUM_2
        media = get("//tmp/q/@hunk_media")
        # NB: setting @hunk_primary_medium is enough to move from one medium to another.
        assert media[self.NON_DEFAULT_MEDIUM_2]["replication_factor"] == 5
        assert not media[self.NON_DEFAULT_MEDIUM_2]["data_parts_only"]
        assert media["default"]["replication_factor"] == 3
        assert media["default"]["data_parts_only"]
        assert self.NON_DEFAULT_MEDIUM_1 not in media

        # 3. Remove.

        with raises_yt_error("Cannot change storage parameters since not all tablets are unmounted"):
            remove("//tmp/q/@hunk_primary_medium")

        sync_unmount_table("//tmp/q")

        with raises_yt_error("Operation cannot be performed in transaction"):
            tx = start_transaction()
            remove("//tmp/q/@hunk_primary_medium", tx=tx)

        remove("//tmp/q/@hunk_primary_medium", authenticated_user="u")

        sync_mount_table("//tmp/q")

        assert not exists("//tmp/q/@hunk_primary_medium")
        assert not exists("//tmp/q/@hunk_media")

    @authors("shakurov")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_media_attribute(self, init_table):
        init_table = getattr(self, init_table)

        create_user("u")

        init_table("//tmp/q")

        assert not exists("//tmp/q/@hunk_media")

        # 1. Set.

        media = {
            self.NON_DEFAULT_MEDIUM_1: {"replication_factor": 5, "data_parts_only": False},
            "default": {"replication_factor": 3, "data_parts_only": True},
        }
        invalid_media = media.copy()
        invalid_media["nonexistent"] = {"replication_factor": 5, "data_parts_only": False}
        invalid_media2 = media.copy()
        invalid_media2[self.NON_DEFAULT_MEDIUM_1] = {"replication_factor": 5, "data_parts_only": True}

        with raises_yt_error("Cannot change storage parameters since not all tablets are unmounted"):
            set("//tmp/q/@hunk_media", media)

        sync_unmount_table("//tmp/q")

        with raises_yt_error("Cannot modify hunk_media since hunk_primary_medium is not set, consider setting it first"):
            set("//tmp/q/@hunk_media", media)

        set("//tmp/q/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)

        with raises_yt_error("No such medium"):
            set("//tmp/q/@hunk_media", invalid_media)

        with raises_yt_error("At least one medium should store replicas (including parity parts)"):
            set("//tmp/q/@hunk_media", invalid_media2)

        with raises_yt_error("Operation cannot be performed in transaction"):
            tx = start_transaction()
            set("//tmp/q/@hunk_media", media, tx=tx)

        with raises_yt_error("Access denied for user \"u\""):
            set("//tmp/q/@hunk_media", media, authenticated_user="u")

        set("//tmp/q/@hunk_media", media)
        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@hunk_media") == media

        # 2. Modify.

        media[self.NON_DEFAULT_MEDIUM_2] = media[self.NON_DEFAULT_MEDIUM_1]
        invalid_media = media.copy()
        invalid_media["nonexistent"] = {"replication_factor": 5, "data_parts_only": False}
        invalid_media2 = media.copy()
        invalid_media2[self.NON_DEFAULT_MEDIUM_1] = {"replication_factor": 5, "data_parts_only": True}
        invalid_media2[self.NON_DEFAULT_MEDIUM_2] = {"replication_factor": 5, "data_parts_only": True}
        del media[self.NON_DEFAULT_MEDIUM_1]

        with raises_yt_error("Cannot change storage parameters since not all tablets are unmounted"):
            set("//tmp/q/@hunk_media", media)

        sync_unmount_table("//tmp/q")

        with raises_yt_error("No such medium"):
            set("//tmp/q/@hunk_media", invalid_media)

        with raises_yt_error("At least one medium should store replicas (including parity parts)"):
            set("//tmp/q/@hunk_media", invalid_media2)

        with raises_yt_error("Operation cannot be performed in transaction"):
            tx = start_transaction()
            set("//tmp/q/@hunk_media", media, tx=tx)

        with raises_yt_error("Access denied for user \"u\""):
            set("//tmp/q/@hunk_media", media, authenticated_user="u")

        with raises_yt_error("Cannot remove primary medium"):
            set("//tmp/q/@hunk_media", media)

        with raises_yt_error("Attribute \"hunk_media\" cannot be removed"):
            remove("//tmp/q/@hunk_media")

        media[self.NON_DEFAULT_MEDIUM_1] = media[self.NON_DEFAULT_MEDIUM_2]
        set("//tmp/q/@hunk_media", media)

        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@hunk_media") == media

    @authors("shakurov")
    @pytest.mark.parametrize("init_table", ["_init_sorted_dynamic_table", "_init_ordered_dynamic_table"])
    def test_hunk_media_requisitions(self, init_table):
        is_sorted_table = init_table == "_init_sorted_dynamic_table"
        init_table = getattr(self, init_table)

        init_table("//tmp/s")

        sync_unmount_table("//tmp/s")
        set("//tmp/s/@primary_medium", self.NON_DEFAULT_MEDIUM_1)
        set("//tmp/s/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2)
        sync_mount_table("//tmp/s")

        rows = [{"key": 0, "value": "a"*100} for i in range(10)]
        insert_rows("//tmp/s", rows)

        sync_unmount_table("//tmp/s")
        sync_mount_table("//tmp/s")

        hunk_chunk = self._get_hunk_chunk_id("//tmp/s")
        assert hunk_chunk is not None

        main_chunk = self._get_main_chunk_id("//tmp/s")
        assert main_chunk is not None

        requisition_default = [
            {
                "account": "tmp",
                "medium": "default",
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            }
        ]
        requisition_1 = [
            {
                "account": "tmp",
                "medium": self.NON_DEFAULT_MEDIUM_1,
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            }
        ]
        requisition_2 = [
            {
                "account": "tmp",
                "medium": self.NON_DEFAULT_MEDIUM_2,
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            }
        ]

        get("//sys/cluster_nodes/@config")

        assert self._chunk_has_requisition(main_chunk, requisition_1)
        if is_sorted_table:
            assert self._chunk_has_requisition(hunk_chunk, requisition_2)
        else:
            assert self._chunk_has_requisition(hunk_chunk, requisition_2 + requisition_default)
            self._speed_up_hunk_storage_store_rotation("//tmp/s")
            # Unmounting shouldn't really be necessary here, get rid of it.
            sync_unmount_table("//tmp/s")
            sync_unmount_table("//tmp/s_hunk_storage")
            wait(lambda: get("#{}/@owning_nodes".format(hunk_chunk)) == ["//tmp/s"])

            requisition_2[0]["committed"] = True
            wait(lambda: self._chunk_has_requisition(hunk_chunk, requisition_2))

    @authors("shakurov")
    def test_hunk_media_requisitions_multiple_tables(self):
        self._init_sorted_dynamic_table("//tmp/t")

        for i in range(5):
            rows = [{"key": 2 * i, "value": "a"}, {"key": 1 + 2 * i, "value": "b"*100}]
            insert_rows("<append=%true>//tmp/t", rows[:1])
            sync_flush_table("//tmp/t")
            insert_rows("<append=%true>//tmp/t", rows[1:])
            sync_flush_table("//tmp/t")

        sync_unmount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")

        hunk_chunks = []
        for chunk_id in chunk_ids:
            if get("#{}/@chunk_type".format(chunk_id)) == "hunk":
                hunk_chunks.append(chunk_id)

        assert len(hunk_chunks) > 1

        copy("//tmp/t", "//tmp/t2")

        set("//tmp/t/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_1)

        expected_hunk_requisition = [
            {
                "account": "tmp",
                "medium": "default",
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            },
            {
                "account": "tmp",
                "medium": self.NON_DEFAULT_MEDIUM_1,
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            }
        ]

        for hunk_chunk in hunk_chunks:
            wait(lambda: self._chunk_has_requisition(hunk_chunk, expected_hunk_requisition))

        set("//tmp/t2/@hunk_primary_medium", self.NON_DEFAULT_MEDIUM_2)
        expected_hunk_requisition[0]["medium"] = self.NON_DEFAULT_MEDIUM_2

        for hunk_chunk in hunk_chunks:
            wait(lambda: self._chunk_has_requisition(hunk_chunk, expected_hunk_requisition))


################################################################################


@pytest.mark.enabled_multidaemon
class TestHunkValuesDictionaryCompression(TestSortedDynamicTablesHunks):
    ENABLE_MULTIDAEMON = True

    def _setup_for_dictionary_compression(self, path):
        set("{}/@mount_config/enable_lsm_verbose_logging".format(path), True)
        set("{}/@mount_config/value_dictionary_compression".format(path), {
            "enable": True,
            "column_dictionary_size": 256,
            "max_processed_chunk_count": 2,
            "backoff_period": 1000,
        })
        set("{}/@hunk_chunk_reader/max_decompression_blob_size".format(path), 100)

    def _wait_dictionaries_built(self, path, previous_hunk_chunk_count):
        # One dictionary hunk chunk for each of two policies.
        wait(lambda: len(self._get_hunk_chunk_ids(path)) == previous_hunk_chunk_count + 2)

    def _find_data_hunk_chunks(self, path):
        store_chunk_ids = self._get_store_chunk_ids(path)
        data_hunk_chunk_ids = builtins.set()
        for chunk_id in store_chunk_ids:
            hunk_chunk_refs = get("#{}/@hunk_chunk_refs".format(chunk_id))
            for ref in hunk_chunk_refs:
                if ref["hunk_count"] != 0:
                    data_hunk_chunk_ids.add(ref["chunk_id"])
        return list(data_hunk_chunk_ids)

    def _do_find_referenced_dictionary_hunk_chunks(self, path):
        data_hunk_chunk_ids = self._find_data_hunk_chunks(path)
        hunk_chunk_ids = self._get_hunk_chunk_ids(path)
        dictionary_hunk_chunk_ids = [chunk_id for chunk_id in hunk_chunk_ids if chunk_id not in data_hunk_chunk_ids]

        tablet_info = get(f"{path}/@tablets/0")

        def is_referenced_dictionary(chunk_id):
            try:
                hunk_chunk_info = get("//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}/hunk_chunks/{}".format(
                    tablet_info["cell_leader_address"],
                    tablet_info["cell_id"],
                    tablet_info["tablet_id"],
                    chunk_id,
                ))
                return "dictionary_compression_policy" in hunk_chunk_info and not hunk_chunk_info["dangling"]
            except YtError as err:
                if err.contains_code(yt_error_codes.ResolveErrorCode):
                    return False
                raise

        return [chunk_id for chunk_id in dictionary_hunk_chunk_ids if is_referenced_dictionary(chunk_id)]

    def _find_referenced_dictionary_hunk_chunks(self, path, expected_count):
        if expected_count is None:
            return self._do_find_referenced_dictionary_hunk_chunks(path)

        wait(lambda: len(self._do_find_referenced_dictionary_hunk_chunks(path)) == expected_count)
        return self._do_find_referenced_dictionary_hunk_chunks(path)

    def _perform_forced_compaction(self, path, compaction_type):
        chunk_ids_before_compaction = builtins.set(self._get_store_chunk_ids(path))
        set("{}/@forced_{}_revision".format(path, compaction_type), 1)
        remount_table(path)

        def _check_forced_compaction():
            chunk_ids = builtins.set(self._get_store_chunk_ids(path))
            return chunk_ids_before_compaction.isdisjoint(chunk_ids)
        wait(_check_forced_compaction)

    @authors("akozhikhov")
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed", "uncompressed"])
    def test_value_compression_simple(self, in_memory_mode):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@in_memory_mode", in_memory_mode)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_compaction(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        hunk_chunk_ids = self._find_data_hunk_chunks("//tmp/t")
        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(hunk_chunk_ids) == 2
        assert len(dictionary_ids) == 2

        self._perform_forced_compaction("//tmp/t", "compaction")

        new_hunk_chunk_ids = self._find_data_hunk_chunks("//tmp/t")
        new_dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(new_hunk_chunk_ids) == 1
        assert len(new_dictionary_ids) == 2
        assert builtins.set(dictionary_ids) == builtins.set(new_dictionary_ids)

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_no_policy_probation(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@mount_config/value_dictionary_compression/policy_probation_samples_size", 500)
        set("//tmp/t/@mount_config/value_dictionary_compression/max_acceptable_compression_ratio", 1)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_dictionary_refs(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@max_hunk_compaction_size", 1)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)

        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(dictionary_ids) == 2

        def _has_dictionary_ref(chunk_id, expected_ref_count):
            hunk_chunk_refs = get("#{}/@hunk_chunk_refs".format(chunk_id))
            assert expected_ref_count is None or expected_ref_count == len(hunk_chunk_refs)
            has_ref_to_dictionary = False
            for ref in hunk_chunk_refs:
                if ref["chunk_id"] in dictionary_ids:
                    has_ref_to_dictionary = True
            return has_ref_to_dictionary

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        for chunk_id in store_chunk_ids:
            assert not _has_dictionary_ref(chunk_id, 1)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        new_store_chunk_ids_1 = self._get_store_chunk_ids("//tmp/t")
        for chunk_id in new_store_chunk_ids_1:
            has_dictionary_ref = _has_dictionary_ref(chunk_id, None)
            if chunk_id in store_chunk_ids:
                assert not has_dictionary_ref
            else:
                assert has_dictionary_ref

        self._perform_forced_compaction("//tmp/t", "store_compaction")
        new_store_chunk_ids_2 = self._get_store_chunk_ids("//tmp/t")
        assert len(new_store_chunk_ids_2) == 1
        assert new_store_chunk_ids_2[0] not in new_store_chunk_ids_1
        assert _has_dictionary_ref(new_store_chunk_ids_2[0], 3)

        self._perform_forced_compaction("//tmp/t", "compaction")
        new_store_chunk_ids_3 = self._get_store_chunk_ids("//tmp/t")
        assert len(new_store_chunk_ids_3) == 1
        assert new_store_chunk_ids_2 != new_store_chunk_ids_3
        assert _has_dictionary_ref(new_store_chunk_ids_3[0], 2)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=1000))
        sync_mount_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 2)

        self._perform_forced_compaction("//tmp/t", "compaction")
        new_store_chunk_ids_4 = self._get_store_chunk_ids("//tmp/t")
        assert len(new_store_chunk_ids_4) == 1
        assert new_store_chunk_ids_3 != new_store_chunk_ids_4
        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", None)
        assert _has_dictionary_ref(new_store_chunk_ids_4[0], 1)

        set("//tmp/t/@mount_config/value_dictionary_compression/enable", False)
        remount_table("//tmp/t")
        self._perform_forced_compaction("//tmp/t", "compaction")
        new_store_chunk_ids_5 = self._get_store_chunk_ids("//tmp/t")
        assert len(new_store_chunk_ids_5) == 1
        assert new_store_chunk_ids_4 != new_store_chunk_ids_5
        assert not get("#{}/@hunk_chunk_refs".format(new_store_chunk_ids_5[0]))

    @authors("akozhikhov")
    def test_value_compression_inline_hunks(self):
        sync_create_cells(1)
        self._create_table(max_inline_hunk_size=1000)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 0)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        self._perform_forced_compaction("//tmp/t", "store_compaction")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_read_after_disable(self):
        sync_create_cells(1)
        self._create_table(max_inline_hunk_size=1000)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 0)
        self._perform_forced_compaction("//tmp/t", "compaction")
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        set("//tmp/t/@mount_config/value_dictionary_compression/enable", False)
        remount_table("//tmp/t")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        self._perform_forced_compaction("//tmp/t", "store_compaction")
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        self._perform_forced_compaction("//tmp/t", "compaction")
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_read_after_schema_alter(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=1000))
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        self._perform_forced_compaction("//tmp/t", "store_compaction")
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        self._perform_forced_compaction("//tmp/t", "compaction")
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_specify_policy_and_check_attribute(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(dictionary_ids) == 2

        assert get("#{}/@compression_dictionary_id".format(self._get_store_chunk_ids("//tmp/t")[0])) in dictionary_ids
        assert get("#{}/@compression_dictionary_id".format(self._find_data_hunk_chunks("//tmp/t")[0])) in dictionary_ids
        assert not exists("#{}/@compression_dictionary_id".format(dictionary_ids[0]))
        assert not exists("#{}/@compression_dictionary_id".format(dictionary_ids[1]))

        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 3
        set("//tmp/t/@mount_config/value_dictionary_compression/applied_policies", ["large_chunk_first"])
        remount_table("//tmp/t")
        wait(lambda: len(self._get_hunk_chunk_ids("//tmp/t")) == 2)

        set("//tmp/t/@mount_config/value_dictionary_compression/applied_policies", [])
        remount_table("//tmp/t")
        set("//tmp/t/@mount_config/value_dictionary_compression/applied_policies", ["large_chunk_first"])
        remount_table("//tmp/t")
        self._wait_dictionaries_built("//tmp/t", 1)

        self._perform_forced_compaction("//tmp/t", "store_compaction")
        new_dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(new_dictionary_ids) == 2
        old_dictionary_id = dictionary_ids[0] if dictionary_ids[0] in new_dictionary_ids else dictionary_ids[1]
        assert old_dictionary_id in new_dictionary_ids

        assert not exists("#{}/@compression_dictionary_id".format(self._get_store_chunk_ids("//tmp/t")[0]))
        hunk_chunk_dictionary_id = get("#{}/@compression_dictionary_id".format(self._find_data_hunk_chunks("//tmp/t")[0]))
        assert hunk_chunk_dictionary_id == old_dictionary_id
        assert not exists("#{}/@compression_dictionary_id".format(dictionary_ids[0]))
        assert not exists("#{}/@compression_dictionary_id".format(dictionary_ids[1]))

        self._perform_forced_compaction("//tmp/t", "compaction")
        wait(lambda: len(self._find_referenced_dictionary_hunk_chunks("//tmp/t", 1)) == 1)
        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 1)
        assert dictionary_ids[0] in new_dictionary_ids and dictionary_ids[0] != old_dictionary_id

        assert get("#{}/@compression_dictionary_id".format(self._get_store_chunk_ids("//tmp/t")[0])) == dictionary_ids[0]
        assert get("#{}/@compression_dictionary_id".format(self._find_data_hunk_chunks("//tmp/t")[0])) == dictionary_ids[0]
        assert not exists("#{}/@compression_dictionary_id".format(dictionary_ids[0]))

    @authors("akozhikhov")
    def test_value_compression_multiple_columns_1(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 200},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100, "value2": "y" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        rows2 = [{"key": i, "value": "value" + str(i) + "x", "value2": "y"} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        # The second flushed chunk is supposed to remain uncompressed.
        chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(chunk_ids) == 2
        has_dictionary_1 = exists("#{}/@compression_dictionary_id".format(chunk_ids[0]))
        has_dictionary_2 = exists("#{}/@compression_dictionary_id".format(chunk_ids[1]))
        assert has_dictionary_1 ^ has_dictionary_2

        hunk_chunk_ids = self._find_data_hunk_chunks("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        assert exists("#{}/@compression_dictionary_id".format(hunk_chunk_ids[0]))

    @authors("akozhikhov")
    def test_value_compression_multiple_columns_2(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 25},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 25},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS, max_inline_hunk_size=25)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        # Compressor will be created only for first value column.
        rows = [{"key": i, "value": "x" * 100, "value2": yson.YsonEntity()} for i in range(100)]
        rows[0]["value2"] = "y"
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert len(self._find_data_hunk_chunks("//tmp/t")) == 1

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        assert len(self._find_data_hunk_chunks("//tmp/t")) == 0

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        rows2 = [{"key": i, "value": "x", "value2": "y" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        assert len(self._find_data_hunk_chunks("//tmp/t")) == 1

        rows3 = [{"key": i, "value2": "y" * 100} for i in range(200, 300)]
        insert_rows("//tmp/t", rows3)
        sync_flush_table("//tmp/t")

        rows3 = [{**row, "value": yson.YsonEntity()} for row in rows3]
        keys = [{"key": i} for i in range(300)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2 + rows3)

        self._perform_forced_compaction("//tmp/t", "compaction")
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2 + rows3)

    @authors("akozhikhov")
    def test_value_compression_lookup_nonexistent_rows(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys, keep_missing_rows=True), rows + [None] * 100)

    @authors("akozhikhov")
    def test_value_compression_rebuild_dictionary(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")
        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(dictionary_ids) == 2

        set("//tmp/t/@mount_config/value_dictionary_compression/applied_policies", [])
        remount_table("//tmp/t")
        set("//tmp/t/@mount_config/value_dictionary_compression/applied_policies", ["large_chunk_first", "fresh_chunk_first"])
        remount_table("//tmp/t")
        self._wait_dictionaries_built("//tmp/t", 2)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        assert exists("#{}".format(dictionary_ids[0])) ^ exists("#{}".format(dictionary_ids[1]))

        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        wait(lambda: not exists("#{}".format(dictionary_ids[0])))
        wait(lambda: not exists("#{}".format(dictionary_ids[1])))

    @authors("akozhikhov")
    def test_value_compression_alter_from_static(self):
        sync_create_cells(1)
        self._create_table(chunk_format="table_unversioned_schemaless_horizontal", dynamic=False)

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)

        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@chunk_format", "table_versioned_simple")

        sync_mount_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 0)

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        self._perform_forced_compaction("//tmp/t", "store_compaction")

        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_destroy_dictionaries_upon_remount(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        dictionary_ids = self._find_referenced_dictionary_hunk_chunks("//tmp/t", 2)
        assert len(dictionary_ids) == 2

        assert exists("#{}".format(dictionary_ids[0]))
        assert exists("#{}".format(dictionary_ids[1]))

        set("//tmp/t/@mount_config/value_dictionary_compression/enable", False)
        remount_table("//tmp/t")
        self._perform_forced_compaction("//tmp/t", "compaction")

        wait(lambda: not exists("#{}".format(dictionary_ids[0])))
        wait(lambda: not exists("#{}".format(dictionary_ids[1])))

    @authors("akozhikhov")
    def test_value_compression_schema_alter_new_column(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 25},
            {"name": "value2", "type": "string"},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "x" * 100, "value2": "y" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        sync_unmount_table("//tmp/t")
        SCHEMA_WITH_MULTIPLE_COLUMNS.append({"name": "value3", "type": "string", "max_inline_hunk_size": 25})
        alter_table("//tmp/t", schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        sync_mount_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i) + "x" * 100, "value2": "y" * 100, "value3": str(i) + "z" * 100} for i in range(100, 200)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        rows = [{**row, "value3": yson.YsonEntity()} for row in rows]
        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        self._perform_forced_compaction("//tmp/t", "store_compaction")
        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

        self._perform_forced_compaction("//tmp/t", "compaction")
        keys = [{"key": i} for i in range(200)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows + rows2)

    @authors("akozhikhov")
    def test_value_compression_schema_alter_switch_columns(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 25},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 25},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "x" * 100, "value2": "y" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        SCHEMA_WITH_MULTIPLE_COLUMNS[1], SCHEMA_WITH_MULTIPLE_COLUMNS[2] = \
            SCHEMA_WITH_MULTIPLE_COLUMNS[2], SCHEMA_WITH_MULTIPLE_COLUMNS[1]
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    def test_value_compression_dnl_and_hash_index(self, enable_hash_chunk_index, enable_data_node_lookup):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 50},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 50},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100, "value2": str(i) + "y" * 10} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_data_weight_statistics(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 50},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100, "value2": str(i) + "y" * 40} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        def _check_statistics():
            chunk_format_statistics = get("//tmp/t/@chunk_format_statistics")
            if chunk_format_statistics["hunk_default"]["chunk_count"] != 3:
                return False
            assert chunk_format_statistics["table_versioned_simple"]["data_weight"] == 18080
            assert chunk_format_statistics["table_versioned_simple"]["uncompressed_data_size"] == 10016
            assert chunk_format_statistics["table_versioned_simple"]["compressed_data_size"] < 5000
            assert chunk_format_statistics["hunk_default"]["data_weight"] == 11714
            assert chunk_format_statistics["hunk_default"]["uncompressed_data_size"] == 3531
            assert chunk_format_statistics["hunk_default"]["compressed_data_size"] == 3531
            return True

        wait(lambda: _check_statistics())

    @authors("akozhikhov")
    def test_value_compression_dictionary_cache(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "compression_dictionary_cache": {
                    "capacity": 10000000,
                }
            }
        })

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value2", "type": "string"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 25},
        ]
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        sync_mount_table("//tmp/t")

        rows = [{**row, "value2": yson.YsonEntity()} for row in rows]
        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_build_from_multiple_blocks(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 25})
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        set("//tmp/t/@mount_config/value_dictionary_compression/desired_sample_count", 20)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 2)
        self._perform_forced_compaction("//tmp/t", "compaction")

        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_build_after_schema_alter(self):
        SCHEMA_WITH_MULTIPLE_KEY_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        keys = [{"key": i} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=SCHEMA_WITH_MULTIPLE_KEY_COLUMNS)
        sync_mount_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 2)
        self._perform_forced_compaction("//tmp/t", "compaction")

        rows = [{"key": i, "key1": yson.YsonEntity(), "value": "value" + str(i) + "x" * 100} for i in range(100)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("akozhikhov")
    def test_value_compression_read_from_map(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        assert read_table("//tmp/t") == rows

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("akozhikhov")
    def test_value_compression_column_filter(self):
        SCHEMA_WITH_MULTIPLE_COLUMNS = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 50},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 50},
        ]
        sync_create_cells(1)
        self._create_table(schema=SCHEMA_WITH_MULTIPLE_COLUMNS)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "key1": i + 1, "value": "value" + str(i) + "x" * 100, "value1": "y"} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        assert_items_equal(
            lookup_rows("//tmp/t", [{"key": 1, "key1": 2}], column_names=["value"]),
            [{"value": "value1" + "x" * 100}])
        assert_items_equal(
            lookup_rows("//tmp/t", [{"key": 3, "key1": 4}], column_names=["key1", "value1"]),
            [{"key1": 4, "value1": "y"}])

    @authors("akozhikhov")
    def test_value_compression_empty_strings(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(100)]
        rows = [{"key": i, "value": "x" * 100} for i in range(100)]
        insert_rows("//tmp/t", rows + [{"key": 100, "value": ""}])
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        assert_items_equal(lookup_rows("//tmp/t", keys + [{"key": 100}]), rows + [{"key": 100, "value": ""}])

    @authors("akozhikhov")
    def test_value_compression_multiple_chunks_compaction(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@chunk_writer", {"desired_chunk_weight": 750})
        set("//tmp/t/@mount_config/value_dictionary_compression/elect_random_policy", True)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "x" * 100} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        get("//tmp/t/@chunk_ids")

        remove("//tmp/t/@chunk_writer")
        remount_table("//tmp/t")

        self._perform_forced_compaction("//tmp/t", "compaction")

    @authors("akozhikhov")
    def test_value_compression_partitioning(self):
        sync_create_cells(1)
        self._create_table()
        self._setup_for_dictionary_compression("//tmp/t")
        set("//tmp/t/@mount_config/value_dictionary_compression/elect_random_policy", True)
        set("//tmp/t/@chunk_writer", {"block_size": 64, "tesing_delay_before_chunk_close": 1000})
        set("//tmp/t/@compression_codec", "none")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "x" * 100} for i in range(50)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        assert len(self._find_tablet_orchid(address, tablet_id)["partitions"]) == 1

        def _get_eden_chunk_ids():
            chunk_ids = get("//tmp/t/@chunk_ids")
            eden_chunk_ids = []
            for chunk_id in chunk_ids:
                try:
                    if get("#{}/@eden".format(chunk_id)):
                        eden_chunk_ids.append(chunk_id)
                except YtError as err:
                    if err.contains_code(yt_error_codes.ResolveErrorCode):
                        continue
                    raise
            return eden_chunk_ids

        assert len(_get_eden_chunk_ids()) == 1

        set("//tmp/t/@max_partition_data_size", 640)
        set("//tmp/t/@desired_partition_data_size", 512)
        set("//tmp/t/@min_partition_data_size", 256)
        remount_table("//tmp/t")

        wait(lambda: len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

        self._perform_forced_compaction("//tmp/t", "compaction")

        assert len(_get_eden_chunk_ids()) == 0

    @authors("coteeq", "akozhikhov")
    def test_value_compression_merge_with_filter(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 10},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 10},
        ]
        self._create_table(schema=schema)
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value1": "i am huuuunk" * 5, "value2": "i am hunk too" * 5} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        merge(
            in_="//tmp/t",
            out="<create=%true>//tmp/t_static",
            mode="unordered",
            spec={
                "force_transform": True,
            },
        )

        assert read_table("//tmp/t_static") == rows

        def _run_merge_with_column_subset(columns):
            merge(
                in_="<columns=[{}]>//tmp/t".format("; ".join(columns)),
                out="<create=%true>//tmp/t_static",
                mode="unordered",
                spec={
                    "force_transform": True,
                },
            )

            def drop_value(row):
                row = deepcopy(row)
                for column in schema:
                    if column["name"] in columns:
                        continue
                    row[column["name"]] = yson.YsonEntity()
                return row

            assert read_table("//tmp/t_static") == [drop_value(row) for row in rows]

        _run_merge_with_column_subset(["key", "value1"])
        _run_merge_with_column_subset(["value1", "value2"])
        _run_merge_with_column_subset(["key", "value2"])
        _run_merge_with_column_subset(["value2"])

    @authors("akozhikhov")
    def test_value_compression_select_with_key_filter(self):
        sync_create_cells(1)
        self._create_table(schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string", "max_inline_hunk_size": 10},
            {"name": "value2", "type": "string", "max_inline_hunk_size": 10},
        ])
        self._setup_for_dictionary_compression("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value1": "i am huuuunk" * 30, "value2": "i am hunk too" * 30} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._wait_dictionaries_built("//tmp/t", 1)
        self._perform_forced_compaction("//tmp/t", "compaction")

        def _filter_rows(rows, keys):
            new_rows = []
            for row in rows:
                new_rows.append(dict([(key, row[key]) for key in keys]))
            return new_rows

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/t]"),  _filter_rows(rows, ["key", "value1"]))
        assert_items_equal(select_rows("value1, value2 from [//tmp/t]"),  _filter_rows(rows, ["value1", "value2"]))
        assert_items_equal(select_rows("key, value2 from [//tmp/t]"),  _filter_rows(rows, ["key", "value2"]))
        assert_items_equal(select_rows("value2 from [//tmp/t]"),  _filter_rows(rows, ["value2"]))


################################################################################


@pytest.mark.enabled_multidaemon
class TestOrderedMulticellHunks(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    @authors("akozhikhov")
    def test_remove_cell_with_attached_hunk_storage(self):
        cell_id = sync_create_cells(1)[0]

        create("hunk_storage", "//tmp/h", attributes={
            "external_cell_tag": 11,
            "scan_backoff_period": 1000,
        })

        sync_mount_table("//tmp/h")

        assert get("//tmp/h/@tablet_state") == "mounted"
        assert get("#{}/@tablet_count".format(cell_id)) == 1
        assert len(get("#{}/@tablet_ids".format(cell_id))) == 1

        assert get("#{}/@total_statistics/tablet_count".format(cell_id)) == 1

        remove("#{}".format(cell_id))
        time.sleep(5)

        sync_unmount_table("//tmp/h")
        wait(lambda: not exists("#{}".format(cell_id)))


################################################################################

class TestHunksInStaticTable(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2

    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "tablet_node": {
                "hunk_lock_manager": {
                    "hunk_store_extra_lifetime": 100,
                    "unlock_check_period": 100
                }
            }
        }
    }

    SCHEMA = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string", "max_inline_hunk_size": 10},
    ]

    def _create_primary_dynamic_table(self, path, table_attrs={}):
        self._create_simple_table(
            path,
            schema=self.SCHEMA,
            enable_dynamic_store_read=False,
            hunk_chunk_reader={
                "max_hunk_count_per_read": 2,
                "max_total_hunk_length_per_read": 60,
                "hedging_manager": {
                    "secondary_request_ratio": 0.5,
                    "max_hedging_delay": 1,
                },
            },
            hunk_chunk_writer={
                "desired_block_size": 50,
            },
            max_hunk_compaction_garbage_ratio=0.5,
            enable_lsm_verbose_logging=True,
            **table_attrs)

    def _alter_to_static(self):
        def _wait_sealed():
            _, hunk_chunk_ids = self._get_chunk_ids()
            for hunk_chunk_id in hunk_chunk_ids:
                if not get("#{}/@sealed".format(hunk_chunk_id)):
                    return False
            return True

        wait(lambda: _wait_sealed())
        set("//sys/@config/tablet_manager/enable_alter_to_static_with_hunks", True)
        alter_table("//tmp/t", dynamic=False)

    def _generate_static_table_with_hunks(self, do_alter=True, table_attrs={}, hunk_storage_attrs={}):
        self._create_primary_dynamic_table("//tmp/t", table_attrs)

        hunk_storage_attrs_copy = hunk_storage_attrs.copy()
        if "store_rotation_period" not in hunk_storage_attrs_copy:
            hunk_storage_attrs_copy["store_rotation_period"] = 1000
        if "scan_backoff_period" not in hunk_storage_attrs_copy:
            hunk_storage_attrs_copy["scan_backoff_period"] = 1000
        if self.is_multicell():
            if "external_cell_tag" not in hunk_storage_attrs_copy and hunk_storage_attrs_copy.get("external", True):
                external_cell_tag = get("//tmp/t/@external_cell_tag")
                hunk_storage_attrs_copy["external_cell_tag"] = external_cell_tag
        hunk_storage_id = create("hunk_storage", "//tmp/h", attributes=hunk_storage_attrs_copy)
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/h")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows)

        if do_alter:
            sync_unmount_table("//tmp/t")
            self._alter_to_static()

        return rows

    def _get_hunk_storage_store_ids(self):
        tablet_id = get("//tmp/h/@tablets")[0]["tablet_id"]
        wait(lambda: exists("//sys/tablets/{}/orchid/active_store_id".format(tablet_id)))
        return get("//sys/tablets/{}/orchid/stores".format(tablet_id)).keys()

    def _get_chunk_ids(self):
        chunk_ids = get("//tmp/t/@chunk_ids")
        store_chunk_ids = builtins.set(self._get_store_chunk_ids("//tmp/t"))
        hunk_chunk_ids = builtins.set([chunk_id for chunk_id in chunk_ids if chunk_id not in store_chunk_ids])
        return store_chunk_ids, hunk_chunk_ids

    @authors("akozhikhov")
    def test_static_hunks_simple(self):
        sync_create_cells(1)
        rows = self._generate_static_table_with_hunks()

        assert read_table("//tmp/t") == rows

    @authors("akozhikhov")
    def test_static_hunks_chunk_list(self):
        sync_create_cells(1)
        self._generate_static_table_with_hunks()

        store_chunk_ids, hunk_chunk_ids = self._get_chunk_ids()
        assert len(store_chunk_ids) == 1
        assert len(hunk_chunk_ids) == 1
        assert hunk_chunk_ids != store_chunk_ids

    @authors("akozhikhov")
    def test_static_hunks_chunk_lifetime(self):
        sync_create_cells(1)
        self._generate_static_table_with_hunks()

        store_chunk_ids, hunk_chunk_ids = self._get_chunk_ids()

        sync_unmount_table("//tmp/h")

        time.sleep(1)

        assert exists("#{}".format(list(hunk_chunk_ids)[0]))

        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(list(hunk_chunk_ids)[0])))

    @authors("akozhikhov")
    def test_static_hunks_alter_back(self):
        sync_create_cells(1)
        rows = self._generate_static_table_with_hunks()

        store_chunk_ids, hunk_chunk_ids = self._get_chunk_ids()

        alter_table("//tmp/t", dynamic=True)

        assert read_table("//tmp/t") == rows

        store_chunk_ids_1, hunk_chunk_ids_1 = self._get_chunk_ids()

        assert store_chunk_ids == store_chunk_ids_1
        assert hunk_chunk_ids == hunk_chunk_ids_1

    @authors("akozhikhov")
    @pytest.mark.parametrize("alter_immediately", [False, True])
    def test_static_hunks_store_rotated(self, alter_immediately):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks(
            do_alter=alter_immediately,
            hunk_storage_attrs={
                "store_removal_grace_period": 4000
            })

        if not alter_immediately:
            sync_unmount_table("//tmp/t")

        store_ids = builtins.set(self._get_hunk_storage_store_ids())
        wait(lambda: len(store_ids.intersection(builtins.set(self._get_hunk_storage_store_ids()))) == 0)

        if not alter_immediately:
            self._alter_to_static()

        assert read_table("//tmp/t") == rows

        _, hunk_chunk_ids = self._get_chunk_ids()
        hunk_chunk_ids = list(hunk_chunk_ids)
        assert len(hunk_chunk_ids) == 1

        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(hunk_chunk_ids[0])))

    @authors("akozhikhov")
    def test_static_hunks_hunk_storage_link(self):
        sync_create_cells(1)

        rows1 = self._generate_static_table_with_hunks()

        assert get("//tmp/h/@associated_nodes") == []
        assert not exists("//tmp/t/@hunk_storage_id")

        alter_table("//tmp/t", dynamic=True)
        assert get("//tmp/h/@associated_nodes") == []
        assert not exists("//tmp/t/@hunk_storage_id")

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10, 20)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows2)
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows1 + rows2)
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == rows1 + rows2

        hunk_storage_id = get("//tmp/h/@id")
        set("//tmp/t/@hunk_storage_id", hunk_storage_id)

        sync_mount_table("//tmp/t")
        rows3 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(20, 30)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows3)
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows1 + rows2 + rows3)
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == rows1 + rows2 + rows3

        with raises_yt_error():
            remove("//tmp/h")
        self._alter_to_static()
        remove("//tmp/h")

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        rows4 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(20, 30)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows4)
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows1 + rows2 + rows3 + rows4)
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == rows1 + rows2 + rows3 + rows4

    @authors("akozhikhov")
    def test_static_hunks_multiple_tablets(self):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks(
            do_alter=False,
            hunk_storage_attrs={
                "store_removal_grace_period": 4000
            })
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", 3)
        sync_mount_table("//tmp/t")

        for i in range(3):
            new_rows = [{"$tablet_index": i, "key": i, "value": str(i) + "y" * 20}]
            rows += [{"key": i, "value": str(i) + "y" * 20}]
            self._insert_rows_with_hunk_storage("//tmp/t", new_rows)

        sync_unmount_table("//tmp/t")
        store_chunk_ids, hunk_chunk_ids = self._get_chunk_ids()
        self._alter_to_static()

        assert read_table("//tmp/t") == rows
        new_rows = [{"key": 4, "value": str(4) + "a" * 20}]
        rows += new_rows
        write_table("<append=%true>//tmp/t", new_rows)
        assert read_table("//tmp/t") == rows

        store_chunk_ids_1, hunk_chunk_ids_1 = self._get_chunk_ids()
        assert store_chunk_ids.issubset(store_chunk_ids_1)
        assert hunk_chunk_ids == hunk_chunk_ids_1

        store_ids = builtins.set(self._get_hunk_storage_store_ids())
        wait(lambda: len(store_ids.intersection(builtins.set(self._get_hunk_storage_store_ids()))) == 0)

        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@hunk_storage_id", get("//tmp/h/@id"))
        sync_mount_table("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 1
        assert read_table("//tmp/t") == rows
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)

        new_rows = [{"key": 5, "value": str(5) + "z" * 20}]
        rows += new_rows
        self._insert_rows_with_hunk_storage("//tmp/t", new_rows)
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == rows

        store_chunk_ids_2, hunk_chunk_ids_2 = self._get_chunk_ids()
        assert hunk_chunk_ids_1.issubset(hunk_chunk_ids_2)
        assert store_chunk_ids_1.issubset(store_chunk_ids_2)
        assert len(store_chunk_ids_2 - store_chunk_ids_1) == 1
        assert len(hunk_chunk_ids_2 - hunk_chunk_ids_1) == 1

        self._alter_to_static()
        assert read_table("//tmp/t") == rows

        store_chunk_ids_3, hunk_chunk_ids_3 = self._get_chunk_ids()
        assert hunk_chunk_ids_2 == hunk_chunk_ids_3
        assert store_chunk_ids_2 == store_chunk_ids_3

        remove("//tmp/t")

        for chunk_id in store_chunk_ids_3:
            wait(lambda: not exists("#{}".format(chunk_id)))
        for chunk_id in hunk_chunk_ids_3:
            wait(lambda: not exists("#{}".format(chunk_id)))

    @authors("akozhikhov")
    def test_static_hunks_copy_and_move(self):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks()

        copy("//tmp/t", "//tmp/t2")
        move("//tmp/t2", "//tmp/t3")

        chunk_ids = get("//tmp/t/@chunk_ids")
        remove("//tmp/t")

        try:
            assert read_table("//tmp/t2") == rows
        except YtError as err:
            if not err.contains_code(yt_error_codes.ResolveErrorCode):
                raise

        assert read_table("//tmp/t3") == rows
        assert builtins.set(chunk_ids) == builtins.set(get("//tmp/t3/@chunk_ids"))

    @authors("akozhikhov")
    def test_static_hunks_overwrite(self):
        sync_create_cells(1)

        self._generate_static_table_with_hunks()

        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10, 20)]
        write_table("<append=%false>//tmp/t", rows)
        assert read_table("//tmp/t") == rows

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        assert read_table("//tmp/t") == rows
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)

    @authors("akozhikhov")
    @pytest.mark.parametrize("do_alter", [False, True])
    def test_static_hunks_remote_copy(self, do_alter):
        sync_create_cells(1)

        self._generate_static_table_with_hunks(do_alter=do_alter)

        if do_alter:
            create("table", "//tmp/t_out")
        else:
            sync_unmount_table("//tmp/t")
            self._create_primary_dynamic_table("//tmp/t_out")

        with raises_yt_error() as err:
            remote_copy(
                in_="//tmp/t",
                out="//tmp/t_out",
                spec={"cluster_connection": self.__class__.Env.configs["driver"]},
            )

        if do_alter:
            assert "Remote copy for static tables with hunks is not supported" in str(err)
        else:
            assert "Remote copy for tables connected to hunk storage is not supported" in str(err)

    @authors("akozhikhov")
    @pytest.mark.parametrize("do_alter", [False, True])
    def test_static_hunks_map(self, do_alter):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks(do_alter=do_alter)
        if not do_alter:
            sync_unmount_table("//tmp/t")

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("do_alter", [False, True])
    def test_static_hunks_merge(self, do_alter):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks(do_alter=do_alter)
        if not do_alter:
            sync_unmount_table("//tmp/t")

        create("table", "//tmp/t_out")
        merge(
            in_="//tmp/t",
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": "ordered",
            }
        )
        assert read_table("//tmp/t_out") == rows

        create("table", "//tmp/t_out", force=True)
        merge(
            in_="//tmp/t",
            out="//tmp/t_out",
            spec={
                "force_transform": False,
                "mode": "ordered",
            }
        )
        assert read_table("//tmp/t_out") == rows

    @authors("akozhikhov")
    def test_static_hunks_schema_stability(self):
        sync_create_cells(1)

        self._generate_static_table_with_hunks()

        assert not get("//tmp/t/@dynamic")
        schema = get("//tmp/t/@schema")
        assert schema[1]["name"] == "value"
        assert schema[1]["max_inline_hunk_size"] == 10

    @authors("akozhikhov")
    def test_static_hunks_run_select(self):
        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks()

        assert read_table("//tmp/t") == rows
        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        assert read_table("//tmp/t") == rows
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)

        for i in range(len(rows)):
            rows[i]["$tablet_index"] = 0
            rows[i]["$row_index"] = i
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)


@pytest.mark.enabled_multidaemon
class TestHunksInStaticTableMulticell(TestHunksInStaticTable):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    def _generate_input_table_attributes(self, external_cell_tag):
        if external_cell_tag == 10:
            return {"external": False}
        return {"external_cell_tag": external_cell_tag}

    def _generate_output_table_attributes(self, external_cell_tag):
        if external_cell_tag == 10:
            return {"external_cell_tag": 12, "schema": self.SCHEMA}
        elif external_cell_tag == 11:
            return {"external": False, "schema": self.SCHEMA}
        return {"external_cell_tag": 11, "schema": self.SCHEMA}

    @authors("akozhikhov")
    @pytest.mark.parametrize("external_cell_tag", [10, 11, 12])
    def test_static_hunks_multicell(self, external_cell_tag):
        sync_create_cells(1)

        in_table_attributes = self._generate_input_table_attributes(external_cell_tag)

        rows = self._generate_static_table_with_hunks(
            table_attrs=in_table_attributes,
            hunk_storage_attrs=in_table_attributes)

        assert read_table("//tmp/t") == rows

        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(10, 20)]
        write_table("<append=%true>//tmp/t", rows2)
        assert read_table("//tmp/t") == rows + rows2

        rows3 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(20, 30)]
        write_table("<append=%false>//tmp/t", rows3)
        assert read_table("//tmp/t") == rows3

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        rows4 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(30, 40)]
        self._insert_rows_with_hunk_storage("//tmp/t", rows4)
        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows3 + rows4)
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == rows3 + rows4

    @authors("akozhikhov")
    @pytest.mark.parametrize("external_cell_tag", [10, 11, 12])
    def test_static_hunks_no_teleport_in_merge(self, external_cell_tag):
        sync_create_cells(1)

        in_table_attributes = self._generate_input_table_attributes(external_cell_tag)
        out_table_attributes = self._generate_output_table_attributes(external_cell_tag)

        rows = self._generate_static_table_with_hunks(
            table_attrs=in_table_attributes,
            hunk_storage_attrs=in_table_attributes)
        create("table", "//tmp/t_out", attributes=out_table_attributes)

        op = merge(mode="unordered", in_=["//tmp/t"], out="//tmp/t_out")
        op.track()

        data_flow = get_operation(op.id, attributes=["progress"])["progress"]["data_flow"]
        directions = {
            (direction["source_name"], direction["target_name"]) : direction
            for direction in data_flow
        }

        assert len(directions) == 2
        assert directions[("unordered_merge", "output")]["job_data_statistics"]["chunk_count"] == 1
        assert directions[("unordered_merge", "output")]["teleport_data_statistics"]["chunk_count"] == 0

        assert read_table("//tmp/t_out") == rows
        out_chunk_ids = get("//tmp/t_out/@chunk_ids")
        assert len(out_chunk_ids) == 1
        assert out_chunk_ids[0] not in get("//tmp/t/@chunk_ids")

        out_schema = get("//tmp/t_out/@schema")
        assert out_schema[1]["name"] == "value"
        assert out_schema[1]["max_inline_hunk_size"] == 10

    @authors("akozhikhov")
    @pytest.mark.parametrize("external_cell_tag", [10, 11, 12])
    def test_static_hunks_teleport_forbidden(self, external_cell_tag):
        if getattr(self, "ENABLE_TMP_PORTAL", False) and external_cell_tag == 11:
            pytest.skip()

        sync_create_cells(1)

        in_table_attributes = self._generate_input_table_attributes(external_cell_tag)
        out_table_attributes = self._generate_output_table_attributes(external_cell_tag)

        self._generate_static_table_with_hunks(
            table_attrs=in_table_attributes,
            hunk_storage_attrs=in_table_attributes)
        create("table", "//tmp/t_out", attributes=out_table_attributes)

        with raises_yt_error("cannot be exported because it has hunk refs"):
            concatenate(["//tmp/t"], "//tmp/t_out")


@pytest.mark.enabled_multidaemon
class TestHunksInStaticTablePortals(TestHunksInStaticTableMulticell):
    ENABLE_TMP_PORTAL = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["cypress_node_host", "chunk_host"]},
        "12": {"roles": ["cypress_node_host", "chunk_host"]},
    }

    @authors("akozhikhov")
    def test_static_hunks_portals_1(self):
        create("portal_entrance", "//p", attributes={"exit_cell_tag": 11})

        sync_create_cells(1)

        rows = self._generate_static_table_with_hunks(
            table_attrs={"external_cell_tag": 12, "external": True},
            hunk_storage_attrs={"external_cell_tag": 12})

        chunk_ids = get("//tmp/t/@chunk_ids")
        move("//tmp/t", "//p/t")
        sync_unmount_table("//tmp/h")
        remove("//tmp/h")
        assert read_table("//p/t") == rows

        assert chunk_ids == get("//p/t/@chunk_ids")

        assert not exists("//tmp/t")
        assert get("#{}/@sealed".format(chunk_ids[0]))
        assert get("#{}/@sealed".format(chunk_ids[1]))
        assert get("#{}/@owning_nodes".format(chunk_ids[0])) == ["//p/t"]
        assert get("#{}/@owning_nodes".format(chunk_ids[1])) == ["//p/t"]

        remove("//p/t")
        wait(lambda: not exists("#{}".format(chunk_ids[0])))
        wait(lambda: not exists("#{}".format(chunk_ids[1])))

        remove("//p")

    @authors("akozhikhov")
    def test_static_hunks_portals_2(self):
        create("portal_entrance", "//p", attributes={"exit_cell_tag": 12})

        sync_create_cells(1)

        self._generate_static_table_with_hunks(
            table_attrs={"external": False},
            hunk_storage_attrs={"external": False})

        with raises_yt_error("//tmp/t must be external"):
            move("//tmp/t", "//p/t")

        remove("//p")
