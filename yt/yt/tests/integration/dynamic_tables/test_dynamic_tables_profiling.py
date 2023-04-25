from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, print_debug, wait, create, get, set, create_user,
    create_tablet_cell_bundle, remove_tablet_cell,
    insert_rows, select_rows, lookup_rows, sync_create_cells, sync_mount_table, sync_flush_table, generate_uuid)

from yt_helpers import profiler_factory

from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal

import pytest

import time

##################################################################


class TestDynamicTablesProfiling(TestSortedDynamicTablesBase):
    DELTA_NODE_CONFIG = {"cluster_connection": {"timestamp_provider": {"update_period": 100}}}

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
