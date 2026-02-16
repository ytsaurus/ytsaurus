from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from .test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_commands import (
    authors, print_debug, wait, create, get, set, create_user, remount_table,
    create_tablet_cell_bundle, remove_tablet_cell, update_nodes_dynamic_config,
    insert_rows, select_rows, lookup_rows, sync_create_cells, pull_queue,
    sync_mount_table, sync_unmount_table, sync_flush_table, generate_uuid, sync_reshard_table)

from yt_helpers import profiler_factory

from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal

from flaky import flaky

from functools import partial

import builtins

import pytest

from concurrent.futures import ThreadPoolExecutor
import time

##################################################################


class TestDynamicTablesProfiling(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = False  # Checks profiling.
    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "timestamp_provider": {"update_period": 100}
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
    @flaky(max_runs=3)
    def test_validate_resource_time_wall_time_sensor(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path)
        assert tablet_profiling.get_counter("write/row_count") == 0
        assert tablet_profiling.get_counter("write/validate_resource_wall_time") == 0

        rows = [
            {"key": 1, "value": "some_str"},
            {"key": 2, "value": "another_one"}
        ]
        for i in range(len(rows)):
            insert_rows(table_path, [rows[i]])
            wait(lambda: tablet_profiling.get_counter("write/row_count") == i + 1)

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

        before = get(path + "/dynamic_row_read_data_weight_count")
        select_rows("* from [//tmp/t] where key in (0, 1, 2, 3)")

        # Dynamic read must change, lookup must not change
        wait(lambda: get(path + "/dynamic_row_read_data_weight_count") > before)
        assert get(path + "/dynamic_row_lookup_data_weight_count") == before

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

        before = get(path + "/static_chunk_row_read_data_weight_count")
        select_rows("* from [//tmp/t] where key in (0, 1, 2, 3)")

        # Static read must change, lookup must not change
        wait(lambda: get(path + "/static_chunk_row_read_data_weight_count") > before)
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == before

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
                and profiling.get_counter("tablet/compressed_data_size") == uncompressed_data_size
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
            compression_codec="none",
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
                    uncompressed_data_size=101,
                    row_count=1,
                    chunk_count=1,
                    hunk_count=1,
                    total_hunk_length=100,
                    hunk_chunk_count=1)

        insert_rows(table_sorted, [{"key": 2, "value": "c" * 100}, {"key": 3, "value": "b" * 100}])
        sync_flush_table(table_sorted)
        wait_sorted(data_weight=87,
                    uncompressed_data_size=215,
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
            compression_codec="none",
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
                     uncompressed_data_size=133,
                     row_count=1,
                     chunk_count=1,
                     hunk_count=0,
                     total_hunk_length=0,
                     hunk_chunk_count=0)

        insert_rows(table_ordered, [{"key": 2, "value": "b" * 100}])
        sync_flush_table(table_ordered)
        wait_ordered(data_weight=218,
                     uncompressed_data_size=266,
                     row_count=2,
                     chunk_count=2,
                     hunk_count=0,
                     total_hunk_length=0,
                     hunk_chunk_count=0)

    @authors("atalmenev")
    def test_max_block_size(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        profiler = profiler_factory().at_tablet_node("//tmp/t")
        max_block_size_summary = profiler.summary("lookup/chunk_reader_statistics/max_block_size")

        insert_rows("//tmp/t", [{"key": 1, "value": "F" * 10}])
        sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 2, "value": "F" * 20}])
        sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 3, "value": "F" * 30}])
        sync_flush_table("//tmp/t")

        lookup_rows("//tmp/t", [{"key": i} for i in range(1, 4)], raw=False)
        wait(lambda: max_block_size_summary.get_max() == 126.0)

        insert_rows("//tmp/t", [{"key": 4, "value": "F" * 40}])
        sync_flush_table("//tmp/t")

        lookup_rows("//tmp/t", [{"key": i} for i in range(1, 4)], raw=False)
        wait(lambda: max_block_size_summary.get_max() == 136.0)


@pytest.mark.enabled_multidaemon
class TestOrderedDynamicTablesProfiling(TestOrderedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_NODE_CONFIG = {"cluster_connection": {"timestamp_provider": {"update_period": 100}}}

    @authors("nadya73")
    def test_queue_profiling(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        table_profiling = self._get_table_profiling(table_path)

        assert table_profiling.get_counter("fetch_table_rows/row_count") == 0
        assert table_profiling.get_counter("fetch_table_rows/data_weight") == 0

        assert table_profiling.get_counter("write/row_count") == 0
        assert table_profiling.get_counter("write/data_weight") == 0

        rows = [{"a": 1}, {"a": 2}]
        insert_rows(table_path, rows)

        wait(
            lambda: table_profiling.get_counter("write/row_count") == 2
        )
        wait(
            lambda: table_profiling.get_counter("write/data_weight") == 18
        )

        read_rows = pull_queue(table_path, offset=0, partition_index=0)
        assert len(read_rows) == 2

        wait(
            lambda: table_profiling.get_counter("fetch_table_rows/row_count") == 2
        )
        wait(
            lambda: table_profiling.get_counter("fetch_table_rows/data_weight") == 50
        )


##################################################################


class TestStatisticsReporterBase:
    @staticmethod
    def _create_table_for_statistics_reporter(table_path, driver=None, bundle="default"):
        def make_struct(name):
            return {
                "name": name,
                "type_v3": {
                    "type_name": "struct",
                    "members": [
                        {"name": "count", "type": "int64"},
                        {"name": "rate", "type": "double"},
                        {"name": "rate_10m", "type": "double"},
                        {"name": "rate_1h", "type": "double"},
                    ],
                }
            }

        create(
            "table",
            table_path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "table_id", "type_v3": "string", "sort_order": "ascending"},
                    {"name": "tablet_id", "type_v3": "string", "sort_order": "ascending"},
                    make_struct("dynamic_row_read"),
                    make_struct("dynamic_row_read_data_weight"),
                    make_struct("dynamic_row_lookup"),
                    make_struct("dynamic_row_lookup_data_weight"),
                    make_struct("dynamic_row_write"),
                    make_struct("dynamic_row_write_data_weight"),
                    make_struct("dynamic_row_delete"),
                    make_struct("static_chunk_row_read"),
                    make_struct("static_chunk_row_read_data_weight"),
                    make_struct("static_hunk_chunk_row_read_data_weight"),
                    make_struct("static_chunk_row_lookup"),
                    make_struct("static_chunk_row_lookup_data_weight"),
                    make_struct("static_hunk_chunk_row_lookup_data_weight"),
                    make_struct("compaction_data_weight"),
                    make_struct("partitioning_data_weight"),
                    make_struct("lookup_error"),
                    make_struct("write_error"),
                    make_struct("lookup_cpu_time"),
                    make_struct("select_cpu_time"),
                    {"name": "uncompressed_data_size", "type_v3": "int64"},
                    {"name": "compressed_data_size", "type_v3": "int64"},
                ],
                "mount_config": {
                    "min_data_ttl": 0,
                    "max_data_ttl": 86400000,
                    "min_data_versions": 0,
                    "max_data_versions": 1,
                    "merge_rows_on_flush": True,
                },
                "tablet_cell_bundle": bundle,
            },
            driver=driver)

    @classmethod
    def _setup_statistics_reporter(cls, path, driver=None, tablet_cell_bundle="default"):
        update_nodes_dynamic_config({
            "tablet_node" : {
                "statistics_reporter" : {
                    "enable" : True,
                    "table_path": path,
                    "report_backoff_time": 1,
                    "periodic_options": {
                        "period": 1,
                        "splay": 0,
                        "jitter": 0,
                    }
                }
            }
        }, driver=driver)

        if tablet_cell_bundle != "default":
            create_tablet_cell_bundle(tablet_cell_bundle, driver=driver)
        sync_create_cells(1, tablet_cell_bundle=tablet_cell_bundle, driver=driver)

        cls._create_table_for_statistics_reporter(path, driver=driver, bundle=tablet_cell_bundle)
        set(f"{path}/@tablet_balancer_config", {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False
        }, driver=driver)
        sync_mount_table(path, driver=driver)


class TestStatisticsReporter(TestStatisticsReporterBase, TestSortedDynamicTablesBase):
    def _get_counter(self, statistics_path, table_id, tablet_id, name, counter):
        response = lookup_rows(statistics_path, [{"table_id": table_id, "tablet_id": tablet_id}])
        if len(response) == 0:
            return None
        return response[0][name][counter]

    @authors("dave11ar")
    def test_statistics_reporter(self):
        statistics_path = "//tmp/statistics_reporter_table"
        self._setup_statistics_reporter(statistics_path)

        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        wait(lambda: len(lookup_rows(statistics_path, [{"table_id": table_id, "tablet_id": tablet_id}])) == 1)

    @authors("atalmenev")
    def test_update_statistics_in_statistics_reporter(self):
        statistics_path = "//tmp/statistics_reporter_table"
        self._setup_statistics_reporter(statistics_path)

        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _check_dynamic_row_write_counter_after_unmount(expected_value, rows=None):
            sync_unmount_table("//tmp/t")
            sync_mount_table("//tmp/t")

            if rows is None:
                self._create_sorted_table("//tmp/t1")
                sync_mount_table("//tmp/t1")
                wait(lambda: self._get_counter(
                    statistics_path,
                    get("//tmp/t1/@id"),
                    get("//tmp/t1/@tablets/0/tablet_id"),
                    "dynamic_row_write", "count") == 0)
            else:
                insert_rows("//tmp/t", rows)

            wait(lambda: self._get_counter(
                statistics_path,
                table_id,
                tablet_id,
                "dynamic_row_write", "count") == expected_value)

        insert_rows("//tmp/t", [{"key": 1, "value": "F"}])
        wait(lambda: self._get_counter(
            statistics_path,
            table_id,
            tablet_id,
            "dynamic_row_write", "count") == 1)
        _check_dynamic_row_write_counter_after_unmount(expected_value=1)

        _check_dynamic_row_write_counter_after_unmount(
            expected_value=2,
            rows=[{"key": 2, "value": "F"}])

        _check_dynamic_row_write_counter_after_unmount(
            expected_value=9,
            rows=[{"key": i, "value": "F"} for i in range(3, 10)])

    @authors("sabdenovch")
    def test_select_cpu_performance_counters(self):
        statistics_path = "//tmp/statistics_reporter_table"
        self._setup_statistics_reporter(statistics_path)

        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [100], [200]])

        sync_mount_table("//tmp/t")
        values = ("aaa", "bbb", "ccc")
        insert_rows("//tmp/t", [{"key": i, "value": values[i // 100]} for i in range(300)])

        table_id = get("//tmp/t/@id")
        tablet_ids = [tablet["tablet_id"] for tablet in get("//tmp/t/@tablets")]

        with ThreadPoolExecutor(max_workers=48) as executor:
            for _ in range(480):
                executor.submit(select_rows, "max(value) from [//tmp/t] where value = \"bbb\" group by 1")

        def _check_greater_second_tablet_load():
            response = lookup_rows(statistics_path, [{"table_id": table_id, "tablet_id": tablet_id} for tablet_id in tablet_ids])
            if len(response) != len(tablet_ids):
                return False

            def _get_cpu(index):
                return response[index]["select_cpu_time"]["count"]
            return _get_cpu(1) > ((_get_cpu(0) + _get_cpu(2)) / 2) * 1.3

        wait(_check_greater_second_tablet_load)

    @authors("akozhikhov")
    def test_hunks_performance_counters_1(self):
        statistics_path = "//tmp/statistics_reporter_table"
        self._setup_statistics_reporter(statistics_path)

        SCHEMA = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        self._create_simple_table(
            "//tmp/t",
            schema=SCHEMA,
            chunk_format="table_versioned_simple")

        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i)} for i in range(5)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5, 10)])
        sync_flush_table("//tmp/t")

        lookup_rows("//tmp/t", [{"key": i} for i in range(10)])

        def _get_lookup_counter():
            return self._get_counter(statistics_path, table_id, tablet_id, "static_chunk_row_lookup_data_weight", "count")

        def _get_hunk_counter(request_type):
            return self._get_counter(statistics_path, table_id, tablet_id, f"static_hunk_chunk_row_{request_type}_data_weight", "count")

        wait(lambda: _get_lookup_counter() > 0)
        assert _get_hunk_counter("lookup") > 0

        prev_value = _get_lookup_counter()
        hunk_prev_value = _get_hunk_counter("lookup")
        lookup_rows("//tmp/t", [{"key": i} for i in range(5)])
        wait(lambda: _get_lookup_counter() > prev_value)
        assert _get_hunk_counter("lookup") == hunk_prev_value

        hunk_prev_value = _get_hunk_counter("read")
        select_rows("key, value from [//tmp/t]")
        wait(lambda: _get_hunk_counter("read") > hunk_prev_value)

        hunk_prev_value = _get_hunk_counter("read")
        select_rows("key, value from [//tmp/t] where key in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
        wait(lambda: _get_hunk_counter("read") > hunk_prev_value)

        hunk_prev_value = _get_hunk_counter("read")
        select_rows("key, value from [//tmp/t] where key in (0, 1, 2, 3, 4)")
        time.sleep(3)
        assert _get_hunk_counter("read") == hunk_prev_value

    @authors("akozhikhov")
    def test_hunks_performance_counters_2(self):
        statistics_path = "//tmp/statistics_reporter_table"
        self._setup_statistics_reporter(statistics_path)

        SCHEMA = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        self._create_simple_table(
            "//tmp/t",
            schema=SCHEMA,
            chunk_format="table_versioned_simple")

        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i)} for i in range(5)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5, 10)])
        sync_flush_table("//tmp/t")

        lookup_rows("//tmp/t", [{"key": i} for i in range(10)])
        select_rows("key, value from [//tmp/t]")

        def _get_hunk_counter(request_type):
            return self._get_counter(statistics_path, table_id, tablet_id, f"static_hunk_chunk_row_{request_type}_data_weight", "count")

        wait(lambda: _get_hunk_counter("lookup") > 0)
        wait(lambda: _get_hunk_counter("read") > 0)

        lookup_data_weight_value = _get_hunk_counter("lookup")
        select_data_weight_value = _get_hunk_counter("read")

        chunk_ids_before_compaction = builtins.set(self._get_store_chunk_ids("//tmp/t"))
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check_forced_compaction():
            chunk_ids = builtins.set(self._get_store_chunk_ids("//tmp/t"))
            return chunk_ids_before_compaction.isdisjoint(chunk_ids)

        wait(_check_forced_compaction)

        time.sleep(3)
        assert lookup_data_weight_value == _get_hunk_counter("lookup")
        assert select_data_weight_value == _get_hunk_counter("read")
