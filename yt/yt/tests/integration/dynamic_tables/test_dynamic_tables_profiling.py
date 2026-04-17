from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from .test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_commands import (
    authors, print_debug, wait, create, get, set, create_user, remount_table,
    create_tablet_cell_bundle, remove_tablet_cell, update_nodes_dynamic_config,
    insert_rows, select_rows, lookup_rows, sync_create_cells, pull_queue,
    sync_mount_table, sync_unmount_table, sync_flush_table, generate_uuid, sync_reshard_table,
    disable_write_sessions_on_node, sync_compact_table, exists, ls, WaitFailed
)

from yt_helpers import profiler_factory

from yt.common import YtError
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
                lambda: print(profiling.get_counter("tablet/uncompressed_data_size")) is None
                and profiling.get_counter("tablet/overlapping_store_count") == overlapping_store_count
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
                    uncompressed_data_size=277,
                    row_count=1,
                    chunk_count=1,
                    hunk_count=1,
                    total_hunk_length=100,
                    hunk_chunk_count=1)

        insert_rows(table_sorted, [{"key": 2, "value": "c" * 100}, {"key": 3, "value": "b" * 100}])
        sync_flush_table(table_sorted)
        wait_sorted(data_weight=87,
                    uncompressed_data_size=567,
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
        # Use dedicated table name to avoid sensor clash with other tests.
        table_path = "//tmp/large_blocks"
        self._create_simple_table(
            table_path,
            mount_config={
                "enable_compaction_and_partitioning": False,
            })
        sync_mount_table(table_path)

        profiler = profiler_factory().at_tablet_node(table_path)
        max_block_size_summary = profiler.summary("lookup/chunk_reader_statistics/max_block_size")

        insert_rows(table_path, [{"key": 1, "value": "F" * 10}])
        sync_flush_table(table_path)

        insert_rows(table_path, [{"key": 2, "value": "F" * 20}])
        sync_flush_table(table_path)

        insert_rows(table_path, [{"key": 3, "value": "F" * 30}])
        sync_flush_table(table_path)

        lookup_rows(table_path, [{"key": i} for i in range(1, 4)], raw=False)
        wait(lambda: max_block_size_summary.get_max() == 302.0)

        insert_rows(table_path, [{"key": 4, "value": "F" * 40}])
        sync_flush_table(table_path)

        lookup_rows(table_path, [{"key": i} for i in range(1, 4)], raw=False)
        wait(lambda: max_block_size_summary.get_max() == 312.0)


@pytest.mark.enabled_multidaemon
class TestOrderedDynamicTablesProfiling(TestOrderedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_NODE_CONFIG = {"cluster_connection": {"timestamp_provider": {"update_period": 100}}}
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

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

    @authors("nadya73")
    def test_detailed_pull_queue_profiling(self):
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t", enable_detailed_profiling=True)
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", rows)

        rpc_proxy = ls("//sys/rpc_proxies")[0]

        proxy_pull_queue_duration_histogram = profiler_factory().at_rpc_proxy(rpc_proxy).histogram(
            name="rpc_proxy/detailed_table_statistics/pull_queue_duration",
            fixed_tags={"table_path": "//tmp/t", "user": "root"})

        def _remove_system_columns(rows):
            return [{"key": row["key"], "value": row["value"]} for row in rows]

        def _check():
            def _check_histogram(histogram):
                try:
                    bins = histogram.get_bins(verbose=True)
                    bin_counters = [bin["count"] for bin in bins]
                    if sum(bin_counters) != 1:
                        return False
                    if len(bin_counters) < 20:
                        return False
                    return True
                except YtError as e:
                    if "No sensors have been collected so far" not in str(e):
                        raise e
                    return False

            assert _remove_system_columns(pull_queue("//tmp/t", offset=0, partition_index=0)) == rows

            try:
                wait(lambda: _check_histogram(proxy_pull_queue_duration_histogram), iter=5, sleep_backoff=0.5)
                return True
            except WaitFailed:
                return False

        wait(lambda: _check())
        assert profiler_factory().at_rpc_proxy(rpc_proxy).get(
            name="rpc_proxy/detailed_table_statistics/pull_queue_mount_cache_wait_time",
            tags={"table_path": "//tmp/t"},
            postprocessor=lambda data: data.get('all_time_max'),
            summary_as_max_for_all_time=True,
            export_summary_as_max=True,
            verbose=False,
            default=0) > 0


##################################################################


class TestStatisticsReporterBase:
    STATISTICS_TABLE_PATH = "//tmp/statistics_reporter_table"

    @staticmethod
    def _create_table_for_statistics_reporter(
        table_path,
        driver=None,
        bundle="default"
    ):
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
                    make_struct("user_data_bytes_transmitted"),
                    make_struct("system_data_bytes_transmitted"),
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
    def _setup_statistics_reporter(
        cls,
        table_path=None,
        driver=None,
        bundle="default"
    ):
        if table_path is None:
            table_path = cls.STATISTICS_TABLE_PATH

        update_nodes_dynamic_config({
            "tablet_node" : {
                "statistics_reporter" : {
                    "enable" : True,
                    "table_path": table_path,
                    "report_backoff_time": 1,
                    "periodic_options": {
                        "period": 1,
                        "splay": 0,
                        "jitter": 0,
                    }
                }
            }
        }, driver=driver)

        if not exists(f"//sys/tablet_cell_bundles/{bundle}", driver=driver):
            create_tablet_cell_bundle(bundle, driver=driver)
        if get(f"//sys/tablet_cell_bundles/{bundle}/@tablet_cell_count", driver=driver) == 0:
            sync_create_cells(1, tablet_cell_bundle=bundle, driver=driver)

        cls._create_table_for_statistics_reporter(table_path, driver=driver, bundle=bundle)
        set(f"{table_path}/@tablet_balancer_config", {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }, driver=driver)
        sync_mount_table(table_path, driver=driver)

    def _get_counter(
        self, table_id, tablet_id, name, counter="count",
        statistics_table_path=None
    ):
        if statistics_table_path is None:
            statistics_table_path = self.STATISTICS_TABLE_PATH

        response = lookup_rows(
            statistics_table_path, [{"table_id": table_id, "tablet_id": tablet_id}],
            verbose=False)
        result = response[0][name][counter] if response else None
        print_debug(f"Got counter {name}.{counter} for table {table_id}, tablet {tablet_id}: {result}")
        return result


class TestStatisticsReporter(TestStatisticsReporterBase, TestSortedDynamicTablesBase):
    def setup_method(self, method):
        super().setup_method(method)
        self._setup_statistics_reporter()

    @authors("dave11ar")
    def test_statistics_reporter(self):
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        wait(lambda: len(lookup_rows(
            self.STATISTICS_TABLE_PATH,
            [{"table_id": table_id, "tablet_id": tablet_id}])) == 1)

    @authors("atalmenev")
    def test_update_statistics_in_statistics_reporter(self):
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
                    get("//tmp/t1/@id"),
                    get("//tmp/t1/@tablets/0/tablet_id"),
                    "dynamic_row_write") == 0)
            else:
                insert_rows("//tmp/t", rows)

            wait(lambda: self._get_counter(
                table_id,
                tablet_id,
                "dynamic_row_write") == expected_value)

        insert_rows("//tmp/t", [{"key": 1, "value": "F"}])
        wait(lambda: self._get_counter(
            table_id,
            tablet_id,
            "dynamic_row_write") == 1)
        _check_dynamic_row_write_counter_after_unmount(expected_value=1)

        _check_dynamic_row_write_counter_after_unmount(
            expected_value=2,
            rows=[{"key": 2, "value": "F"}])

        _check_dynamic_row_write_counter_after_unmount(
            expected_value=9,
            rows=[{"key": i, "value": "F"} for i in range(3, 10)])

    @authors("sabdenovch")
    def test_select_cpu_performance_counters(self):
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
            response = lookup_rows(
                self.STATISTICS_TABLE_PATH,
                [{"table_id": table_id, "tablet_id": tablet_id} for tablet_id in tablet_ids])
            if len(response) != len(tablet_ids):
                return False

            def _get_cpu(index):
                return response[index]["select_cpu_time"]["count"]
            return _get_cpu(1) > ((_get_cpu(0) + _get_cpu(2)) / 2) * 1.3

        wait(_check_greater_second_tablet_load)

    @authors("akozhikhov")
    def test_hunks_performance_counters_1(self):
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
            return self._get_counter(table_id, tablet_id, "static_chunk_row_lookup_data_weight")

        def _get_hunk_counter(request_type):
            return self._get_counter(table_id, tablet_id, f"static_hunk_chunk_row_{request_type}_data_weight")

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
            return self._get_counter(table_id, tablet_id, f"static_hunk_chunk_row_{request_type}_data_weight")

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

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("lookup_cache", [True, False])
    def test_data_bytes_transmitted_sorted(self, optimize_for, lookup_cache):
        SCHEMA = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        self._create_simple_table(
            "//tmp/t",
            schema=SCHEMA,
            optimize_for=optimize_for,
            pivot_keys=[[], [20]],
            mount_config={
                "dynamic_store_auto_flush_period": YsonEntity(),
            })
        if lookup_cache:
            set("//tmp/t/@mount_config/lookup_cache_rows_per_tablet", 50)

        update_nodes_dynamic_config({
            "data_node": {
                "block_cache": {
                    "compressed_data": {
                        "capacity": 0,
                    },
                    "uncompressed_data": {
                        "capacity": 0,
                    },
                    "chunk_fragments_data": {
                        "capacity": 0,
                    },
                }
            }
        })

        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_ids = [t["tablet_id"] for t in get("//tmp/t/@tablets")]
        leader_address = get("//tmp/t/@tablets/0/cell_leader_address")
        disable_write_sessions_on_node(leader_address)

        # Insert two chunks into the first tablet and one chunk into the second tablet
        # to detect any double-counting.
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i)} for i in range(5)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "x" * 20} for i in range(5, 10)])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i)} for i in range(10, 15)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "y" * 20} for i in range(15, 20)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i)} for i in range(20, 25)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "z" * 20} for i in range(25, 30)])
        sync_unmount_table("//tmp/t")

        sync_mount_table("//tmp/t")

        profiler = profiler_factory().at_tablet_node("//tmp/t", fixed_tags={
            "table_path": "//tmp/t",
        })

        # These values are hard-coded to test collection of data_bytes_transmitted as well.
        # Feel free to update them if you modify the chunk format.
        # If it happens that these numbers are modified too frequently then we may rewrite
        # the code and compare performance counters against profiled values without any
        # canonical numbers.
        # Also note that data_bytes_transmitted is not stable between runs.
        expected = {
            "lookup": (1660, 1710),
            # NB: This is strange, the value is always 360 but rarely 440 or 334. Maybe some miscounting happens.
            "lookup_hunks": (334, 440),
            "select": (3320, 3420),
            "select_hunks": (1180, 1210),
            "compaction": (1660, 1710),
            "compaction_hunks": (760, 800),
        }

        def _in_range(actual, range):
            if isinstance(range, int):
                return actual == range
            else:
                return range[0] <= actual <= range[1]

        # Check that performance counters match profiled values for lookup and select.
        tags = {
            "medium": "default",
            "user": "root",
        }
        time.sleep(1)
        lookup_transmitted = profiler.counter("lookup/chunk_reader_statistics/data_bytes_transmitted", tags)
        lookup_transmitted_hunks = profiler.counter("lookup/hunks/chunk_reader_statistics/data_bytes_transmitted", tags)
        select_transmitted = profiler.counter("select/chunk_reader_statistics/data_bytes_transmitted", tags)
        select_transmitted_hunks = profiler.counter("select/hunks/chunk_reader_statistics/data_bytes_transmitted", tags)

        select_rows("* from [//tmp/t]")
        lookup_rows("//tmp/t", [{"key": i} for i in range(0, 30, 5)], use_lookup_cache=True)

        wait(lambda: _in_range(lookup_transmitted.get_delta(), expected["lookup"]))
        wait(lambda: _in_range(lookup_transmitted_hunks.get_delta(), expected["lookup_hunks"]))
        wait(lambda: _in_range(select_transmitted.get_delta(), expected["select"]))
        wait(lambda: _in_range(select_transmitted_hunks.get_delta(), expected["select_hunks"]))

        total = (
            lookup_transmitted.get_delta() +
            lookup_transmitted_hunks.get_delta() +
            select_transmitted.get_delta() +
            select_transmitted_hunks.get_delta()
        )

        user_tablet_counters = [
            self._get_counter(table_id, tablet_id, "user_data_bytes_transmitted")
            for tablet_id in tablet_ids]
        assert sum(user_tablet_counters) == total

        # Check that performance counters match profiled values for compaction.
        tags = {
            "medium": "default",
            "account": "tmp",
            "method": "compaction",
        }
        compaction = profiler.counter("chunk_reader_statistics/data_bytes_transmitted", tags)
        compaction_hunks = profiler.counter("chunk_reader/hunks/chunk_reader_statistics/data_bytes_transmitted", tags)

        sync_compact_table("//tmp/t")
        wait(lambda: _in_range(compaction.get_delta(), expected["compaction"]))
        wait(lambda: _in_range(compaction_hunks.get_delta(), expected["compaction_hunks"]))

        total = compaction.get_delta() + compaction_hunks.get_delta()

        system_tablet_counters = [
            self._get_counter(table_id, tablet_id, "system_data_bytes_transmitted")
            for tablet_id in tablet_ids]
        assert sum(system_tablet_counters) == total

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_data_bytes_transmitted_ordered(self, optimize_for):
        SCHEMA = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        self._create_simple_table(
            "//tmp/t",
            schema=SCHEMA,
            optimize_for=optimize_for,
            tablet_count=2,
            mount_config={
                "dynamic_store_auto_flush_period": YsonEntity(),
            })

        update_nodes_dynamic_config({
            "data_node": {
                "block_cache": {
                    "compressed_data": {
                        "capacity": 0,
                    },
                    "uncompressed_data": {
                        "capacity": 0,
                    },
                    "chunk_fragments_data": {
                        "capacity": 0,
                    },
                }
            }
        })

        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")
        tablet_ids = [t["tablet_id"] for t in get("//tmp/t/@tablets")]
        leader_address = get("//tmp/t/@tablets/0/cell_leader_address")
        disable_write_sessions_on_node(leader_address)

        # Insert two chunks into the first tablet and one chunk into the second tablet
        # to detect any double-counting.
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i), "$tablet_index": 0} for i in range(5)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "x" * 20, "$tablet_index": 0} for i in range(5, 10)])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i), "$tablet_index": 0} for i in range(10, 15)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "y" * 20, "$tablet_index": 0} for i in range(15, 20)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i), "$tablet_index": 1} for i in range(20, 25)])
        insert_rows("//tmp/t", [{"key": i, "value": "value" + str(i) + "z" * 20, "$tablet_index": 1} for i in range(25, 30)])
        sync_unmount_table("//tmp/t")

        sync_mount_table("//tmp/t")

        profiler = profiler_factory().at_tablet_node("//tmp/t", fixed_tags={
            "table_path": "//tmp/t",
        })

        # These values are hard-coded to test collection of data_bytes_transmitted as well.
        # Feel free to update them if you modify the chunk format.
        # If it happens that these numbers are modified too frequently then we may rewrite
        # the code and compare performance counters against profiled values without any
        # canonical numbers.
        # Also note that data_bytes_transmitted is not stable between runs.
        if optimize_for == "scan":
            expected = {
                "select": (1300, 1550),
            }
        else:
            expected = {
                "select": (950, 1050)
            }

        expected["select_hunks"] = 0

        def _in_range(actual, range):
            if isinstance(range, int):
                return actual == range
            else:
                return range[0] <= actual <= range[1]

        # Check that performance counters match profiled values for lookup and select.
        tags = {
            "medium": "default",
            "user": "root",
        }
        time.sleep(1)
        select_transmitted = profiler.counter("select/chunk_reader_statistics/data_bytes_transmitted", tags)
        select_transmitted_hunks = profiler.counter("select/hunks/chunk_reader_statistics/data_bytes_transmitted", tags)

        select_rows("* from [//tmp/t]")

        wait(lambda: _in_range(select_transmitted.get_delta(), expected["select"]))
        wait(lambda: _in_range(select_transmitted_hunks.get_delta(), expected["select_hunks"]))

        total = select_transmitted.get_delta() + select_transmitted_hunks.get_delta()

        user_tablet_counters = [
            self._get_counter(table_id, tablet_id, "user_data_bytes_transmitted")
            for tablet_id in tablet_ids]
        assert sum(user_tablet_counters) == total
