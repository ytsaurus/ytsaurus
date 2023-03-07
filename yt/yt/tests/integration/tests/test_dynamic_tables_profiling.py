import pytest
import __builtin__

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait, skip_if_rpc_driver_backend, parametrize_external, Restarter, NODES_SERVICE
from yt_commands import *
from yt.yson import YsonEntity, loads, dumps

from time import sleep
from random import randint, choice, sample
from string import ascii_lowercase

import random

from yt.environment.helpers import assert_items_equal

##################################################################

class TestDynamicTablesProfiling(TestSortedDynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "cluster_connection" : {
            "timestamp_provider" : {
                "update_period": 100
            }
        }
    }

    @authors("gridem")
    def test_sorted_tablet_node_profiling(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        get(table_path + "/@dynamic_store_auto_flush_period")
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path)

        def get_all_counters(count_name):
            return (
                tablet_profiling.get_counter("lookup/" + count_name),
                tablet_profiling.get_counter("lookup/unmerged_" + count_name),
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("select/unmerged_" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0, 0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0, 0, 0, 0)
        assert tablet_profiling.get_counter("lookup/cpu_time") == 0
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows)

        wait(lambda: get_all_counters("row_count") == (0, 0, 0, 0, 1, 1) and \
                     get_all_counters("data_weight") == (0, 0, 0, 0, 10, 10) and \
                     tablet_profiling.get_counter("lookup/cpu_time") == 0 and \
                     tablet_profiling.get_counter("select/cpu_time") == 0)

        actual = lookup_rows(table_path, keys)
        assert_items_equal(actual, rows)

        wait(lambda: get_all_counters("row_count") == (1, 1, 0, 0, 1, 1) and \
                     get_all_counters("data_weight") == (10, 25, 0, 0, 10, 10) and \
                     tablet_profiling.get_counter("lookup/cpu_time") > 0 and \
                     tablet_profiling.get_counter("select/cpu_time") == 0)

        actual = select_rows("* from [{}]".format(table_path))
        assert_items_equal(actual, rows)

        wait(lambda: get_all_counters("row_count") == (1, 1, 1, 1, 1, 1) and \
                     get_all_counters("data_weight") == (10, 25, 10, 25, 10, 10) and \
                     tablet_profiling.get_counter("lookup/cpu_time") > 0 and \
                     tablet_profiling.get_counter("select/cpu_time") > 0)

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
                table_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert table_profiling.get_counter("lookup/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows)

        wait(lambda: get_all_counters("row_count") == (0, 1, 1) and \
                     get_all_counters("data_weight") == (0, 10, 10) and \
                     table_profiling.get_counter("lookup/cpu_time") == 0)

        actual = lookup_rows(table_path, keys)
        assert_items_equal(actual, rows)

        wait(lambda: get_all_counters("row_count") == (1, 1, 1) and \
                     get_all_counters("data_weight") == (10, 10, 10) and \
                     table_profiling.get_counter("lookup/cpu_time") > 0)

    @authors("iskhakovt")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed"])
    def test_data_weight_performance_counters(self, optimize_for, in_memory_mode):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, in_memory_mode=in_memory_mode, dynamic_store_auto_flush_period=YsonEntity())
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
        assert get(path + "/static_chunk_row_read_data_weight_count") == get(path + "/static_chunk_row_lookup_data_weight_count")
