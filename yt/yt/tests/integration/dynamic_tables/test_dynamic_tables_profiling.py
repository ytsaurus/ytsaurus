from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, print_debug, wait, create, get, set, create_user,
    create_tablet_cell_bundle, remove_tablet_cell,
    insert_rows, select_rows, lookup_rows, sync_create_cells,
    sync_mount_table, sync_flush_table, generate_uuid, sync_reshard_table)

from yt_helpers import profiler_factory

from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal

from functools import partial

import pytest

import time

##################################################################


class TestDynamicTablesProfiling(TestSortedDynamicTablesBase):
    DELTA_NODE_CONFIG = {"cluster_connection": {"timestamp_provider": {"update_period": 100}}}

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "enable_hunks": True
        }
    }

    @authors("gridem")
    def test_sorted_tablet_node_profiling(self):
        sync_create_cells(1)
        create_user("u")

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        get(table_path + "/@dynamic_store_auto_flush_period")
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path, user="u")

        def get_all_counters(count_name):
            all_counters = (
                tablet_profiling.get_counter("lookup/" + count_name),
                tablet_profiling.get_counter("lookup/unmerged_" + count_name),
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("select/unmerged_" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name),
            )
            print_debug("all_counters = {}".format(all_counters))
            return all_counters

        assert get_all_counters("row_count") == (0, 0, 0, 0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0, 0, 0, 0)
        assert tablet_profiling.get_counter("lookup/cpu_time") == 0
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows, authenticated_user="u")

        wait(
            lambda: get_all_counters("row_count") == (0, 0, 0, 0, 1, 1)
            and get_all_counters("data_weight") == (0, 0, 0, 0, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") == 0
            and tablet_profiling.get_counter("select/cpu_time") == 0
        )

        actual = lookup_rows(table_path, keys, authenticated_user="u")
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 0, 0, 1, 1)
            and get_all_counters("data_weight") == (10, 25, 0, 0, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") > 0
            and tablet_profiling.get_counter("select/cpu_time") == 0
        )

        actual = select_rows("* from [{}]".format(table_path), authenticated_user="u")
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 1, 1, 1, 1)
            and get_all_counters("data_weight") == (10, 25, 10, 25, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") > 0
            and tablet_profiling.get_counter("select/cpu_time") > 0
        )

    @authors("alexelexa")
    def test_validate_resource_time_wall_time_sensor(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path)
        assert tablet_profiling.get_counter("write/row_count") == 0
        assert tablet_profiling.get_counter("write/validate_resource_wall_time") == 0

        insert_rows(table_path, [{"key": 1, "value": "some_str"}])

        wait(lambda: tablet_profiling.get_counter("write/row_count") == 1)
        assert tablet_profiling.get_all_time_max("write/validate_resource_wall_time") > 0

    @authors("gridem")
    def test_sorted_default_enabled_tablet_node_profiling(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        table_profiling = self._get_table_profiling(table_path)

        def get_all_counters(count_name):
            return (
                table_profiling.get_counter("lookup/" + count_name),
                table_profiling.get_counter("write/" + count_name),
                table_profiling.get_counter("commit/" + count_name),
            )

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert table_profiling.get_counter("lookup/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows)

        wait(
            lambda: get_all_counters("row_count") == (0, 1, 1)
            and get_all_counters("data_weight") == (0, 10, 10)
            and table_profiling.get_counter("lookup/cpu_time") == 0
        )

        actual = lookup_rows(table_path, keys)
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 1)
            and get_all_counters("data_weight") == (10, 10, 10)
            and table_profiling.get_counter("lookup/cpu_time") > 0
        )

    @authors("iskhakovt", "alexelexa")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed"])
    def test_data_weight_performance_counters(self, optimize_for, in_memory_mode):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            in_memory_mode=in_memory_mode,
            dynamic_store_auto_flush_period=YsonEntity(),
        )
        sync_mount_table("//tmp/t")

        path = "//tmp/t/@tablets/0/performance_counters"

        insert_rows("//tmp/t", [{"key": 0, "value": "hello"}])

        wait(lambda: get(path + "/dynamic_row_write_data_weight_count") > 0)

        select_rows("* from [//tmp/t]")

        # Dynamic read must change, lookup must not change
        wait(lambda: get(path + "/dynamic_row_read_data_weight_count") > 0)
        assert get(path + "/dynamic_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Dynamic read lookup change, read must not change
        wait(lambda: get(path + "/dynamic_row_lookup_data_weight_count") > 0)
        assert get(path + "/dynamic_row_read_data_weight_count") == get(path + "/dynamic_row_lookup_data_weight_count")

        # Static read/lookup must not change
        assert get(path + "/static_chunk_row_read_data_weight_count") == 0
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        sync_flush_table("//tmp/t")

        select_rows("* from [//tmp/t]")

        # Static read must change, lookup must not change
        wait(lambda: get(path + "/static_chunk_row_read_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Static lookup must change, read must not change
        wait(lambda: get(path + "/static_chunk_row_lookup_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_read_data_weight_count") == get(
            path + "/static_chunk_row_lookup_data_weight_count"
        )

    @authors("prime")
    def test_bundle_solomon_tag(self):
        default_cell = sync_create_cells(1)[0]

        def get_solomon_tags(cell_id):
            node_address = get("#%s/@peers/0/address" % cell_id)
            return get("//sys/cluster_nodes/%s/orchid/monitoring/solomon/dynamic_tags" % node_address)

        wait(lambda: get_solomon_tags(default_cell) == {"tablet_cell_bundle": "default"})

        create_tablet_cell_bundle("b1", attributes={})
        bundle_cells = sync_create_cells(20, tablet_cell_bundle="b1")

        wait(lambda: get_solomon_tags(default_cell) == {})
        remove_tablet_cell(default_cell)

        for cell_id in bundle_cells:
            wait(lambda: get_solomon_tags(cell_id) == {"tablet_cell_bundle": "b1"})

        set("//sys/tablet_cell_bundles/b1/@dynamic_options/solomon_tag", "tag1")

        for cell_id in bundle_cells:
            wait(lambda: get_solomon_tags(cell_id) == {"tablet_cell_bundle": "tag1"})

    @authors("prime")
    def test_profiling_path_letters(self):
        sync_create_cells(1)

        create("map_node", "//tmp/path_letters")
        set("//tmp/path_letters/@profiling_mode", "path_letters")

        self._create_simple_table("//tmp/path_letters/t0")
        self._create_simple_table("//tmp/path_letters/t1")
        sync_mount_table("//tmp/path_letters/t0")
        sync_mount_table("//tmp/path_letters/t1")

        insert_rows("//tmp/path_letters/t0", [{"key": 0, "value": "hello"}])
        time.sleep(2)

        profiler = profiler_factory().at_tablet_node("//tmp/path_letters/t0")

        counter = profiler.get("write/row_count", {"table_path": "//tmp/path_letters/t_"})
        assert counter == 1

    @authors("dave11ar")
    def test_tablet_counters(self):
        def _create_table(creator, **attributes):
            creator(
                **attributes,
                enable_dynamic_store_read=False,
                hunk_chunk_reader={
                    "max_hunk_count_per_read": 2,
                    "max_total_hunk_length_per_read": 60,
                    "fragment_read_hedging_delay": 1,
                    "max_inflight_fragment_length": 60,
                    "max_inflight_fragment_count": 2,
                },
                hunk_chunk_writer={
                    "desired_block_size": 50,
                },
            )

        sync_create_cells(1)

        def _wait_counters(
                profiling,
                overlapping_store_count,
                eden_store_count,
                data_weight,
                uncompressed_data_size,
                compressed_data_size,
                row_count,
                chunk_count,
                hunk_count,
                total_hunk_length,
                hunk_chunk_count):
            wait(
                lambda: profiling.get_counter("tablet/overlapping_store_count") == overlapping_store_count
                and profiling.get_counter("tablet/eden_store_count") == eden_store_count
                and profiling.get_counter("tablet/data_weight") == data_weight
                and profiling.get_counter("tablet/uncompressed_data_size") == uncompressed_data_size
                and profiling.get_counter("tablet/compressed_data_size") == compressed_data_size
                and profiling.get_counter("tablet/row_count") == row_count
                and profiling.get_counter("tablet/chunk_count") == chunk_count
                and profiling.get_counter("tablet/hunk_count") == hunk_count
                and profiling.get_counter("tablet/total_hunk_length") == total_hunk_length
                and profiling.get_counter("tablet/hunk_chunk_count") == hunk_chunk_count
                and profiling.get_counter("tablet/tablet_count") == 2
            )

        table_sorted = "//tmp/t_sorted"
        _create_table(
            self._create_simple_table,
            path=table_sorted,
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string", "max_inline_hunk_size": 1},
            ],
        )
        sync_reshard_table(table_sorted, [[], [1]])
        sync_mount_table(table_sorted)

        wait_sorted = partial(_wait_counters, self._get_table_profiling(table_sorted), 2, 1)

        insert_rows(table_sorted, [{"key": 0, "value": "a" * 100}])
        sync_flush_table(table_sorted)
        wait_sorted(data_weight=29,
                    uncompressed_data_size=77,
                    compressed_data_size=121,
                    row_count=1,
                    chunk_count=1,
                    hunk_count=1,
                    total_hunk_length=100,
                    hunk_chunk_count=1)

        insert_rows(table_sorted, [{"key": 2, "value": "c" * 100}, {"key": 3, "value": "b" * 100}])
        sync_flush_table(table_sorted)
        wait_sorted(data_weight=87,
                    uncompressed_data_size=215,
                    compressed_data_size=285,
                    row_count=3,
                    chunk_count=2,
                    hunk_count=3,
                    total_hunk_length=300,
                    hunk_chunk_count=2)

        # TODO(dave11ar): add hunk test when hunk_chunk_count in hunk_storage become determined
        table_ordered = "//tmp/t_ordered"
        _create_table(
            self._create_ordered_table,
            path=table_ordered,
            tablet_count=2,
            schema=[
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string", "max_inline_hunk_size": 1}
            ],
        )

        sync_mount_table(table_ordered)

        wait_ordered = partial(_wait_counters, self._get_table_profiling(table_ordered), 0, 0)

        insert_rows(table_ordered, [{"key": 0, "value": "a" * 100}])
        sync_flush_table(table_ordered)
        wait_ordered(data_weight=109,
                     uncompressed_data_size=112,
                     compressed_data_size=48,
                     row_count=1,
                     chunk_count=1,
                     hunk_count=0,
                     total_hunk_length=0,
                     hunk_chunk_count=0)

        insert_rows(table_ordered, [{"key": 2, "value": "b" * 100}])
        sync_flush_table(table_ordered)
        wait_ordered(data_weight=218,
                     uncompressed_data_size=224,
                     compressed_data_size=96,
                     row_count=2,
                     chunk_count=2,
                     hunk_count=0,
                     total_hunk_length=0,
                     hunk_chunk_count=0)
