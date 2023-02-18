from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    alter_table, authors, wait, create, ls, get, set, remove, exists,
    insert_rows, delete_rows,
    write_table, map, merge, vanilla, remote_copy, get_table_columnar_statistics,
    sync_create_cells, sync_mount_table, sync_flush_table, sync_compact_table,
    raises_yt_error)

from yt_type_helpers import (
    make_schema, make_column,
    optional_type,
)

from yt_helpers import skip_if_renaming_disabled

from yt.common import YtError

import pytest


class _TestColumnarStatisticsBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "user_file_limits": {
                "max_table_data_weight": 2000,
            },
            "operation_options": {
                "spec_template": {
                    "use_columnar_statistics": True,
                },
            },
            "tagged_memory_statistics_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "testing_options": {
                "columnar_statistics_chunk_meta_fetch_max_delay": 5000
            },
            "columnar_statistics_read_timeout_fraction": 0.1,
        }
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 10000
        },
    }

    @staticmethod
    def _expect_statistics(
        lower_row_index,
        upper_row_index,
        columns,
        expected_data_weights,
        expected_timestamp_weight=None,
        expected_legacy_data_weight=0,
        fetcher_mode="from_nodes",
        table="//tmp/t",
        enable_early_finish=False
    ):
        path = '["{0}{{{1}}}[{2}:{3}]";]'.format(
            table,
            columns,
            "#" + str(lower_row_index) if lower_row_index is not None else "",
            "#" + str(upper_row_index) if upper_row_index is not None else "",
        )
        statistics = get_table_columnar_statistics(path,
                                                   fetcher_mode=fetcher_mode,
                                                   enable_early_finish=enable_early_finish)[0]
        assert statistics["legacy_chunks_data_weight"] == expected_legacy_data_weight
        assert statistics["column_data_weights"] == dict(zip(columns.split(","), expected_data_weights))
        if expected_timestamp_weight is not None:
            assert statistics["timestamp_total_weight"] == expected_timestamp_weight

    def _expect_multi_statistics(
        self,
        paths,
        lower_row_indices,
        upper_row_indices,
        all_columns,
        all_expected_data_weights,
        all_expected_timestamp_weight=None,
        schema_columns=None,
    ):
        assert len(paths) == len(all_columns)
        for index in range(len(paths)):
            paths[index] = "{0}{1}[{2}:{3}]".format(
                paths[index],
                "{" + all_columns[index] + "}" if all_columns[index] is not None else "",
                "#" + str(lower_row_indices[index]) if lower_row_indices[index] is not None else "",
                "#" + str(upper_row_indices[index]) if upper_row_indices[index] is not None else "",
            )
        yson_paths = "["
        for path in paths:
            yson_paths += '"' + path + '";'
        yson_paths += "]"
        all_statistics = get_table_columnar_statistics(yson_paths)
        assert len(all_statistics) == len(all_expected_data_weights)
        for index in range(len(all_statistics)):
            assert all_statistics[index]["legacy_chunks_data_weight"] == 0
            expected_columns = all_columns[index] if all_columns[index] is not None else schema_columns[index]
            assert all_statistics[index]["column_data_weights"] == dict(
                zip(expected_columns.split(","), all_expected_data_weights[index])
            )
            if all_expected_timestamp_weight is not None:
                assert all_statistics[index] == all_expected_timestamp_weight[index]

    def _create_simple_dynamic_table(self, path, optimize_for="lookup"):
        create(
            "table",
            path,
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "optimize_for": optimize_for,
            },
        )


class TestColumnarStatistics(_TestColumnarStatisticsBase):
    @authors("max42")
    def test_get_table_columnar_statistics(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])
        with pytest.raises(YtError):
            get_table_columnar_statistics('["//tmp/t";]')
        self._expect_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_statistics(0, 6, "a,b,c", [1300, 8, 17])
        self._expect_statistics(0, 6, "a,c,x", [1300, 17, 0])
        self._expect_statistics(1, 5, "a,b,c", [1300, 8, 17])
        self._expect_statistics(2, 5, "a", [1200])
        self._expect_statistics(1, 4, "", [])

    @authors("gritukan")
    def test_get_table_approximate_statistics(self):
        def make_table(column_weights):
            if exists("//tmp/t"):
                remove("//tmp/t")
            create("table", "//tmp/t")
            if column_weights:
                write_table(
                    "//tmp/t",
                    [{"x{}".format(i): "a" * column_weights[i] for i in range(len(column_weights))}],
                )

        make_table([255, 12, 45, 1, 0])
        self._expect_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
        )

        create("table", "//tmp/t2")
        remote_copy(
            in_="//tmp/t",
            out="//tmp/t2",
            spec={"cluster_connection": self.__class__.Env.configs["driver"]},
        )
        self._expect_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
            table="//tmp/t2",
        )

        make_table([510, 12, 13, 1, 0])
        self._expect_statistics(0, 1, "x0,x1,x2,x3,x4", [510, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([256, 12, 13, 1, 0])
        self._expect_statistics(0, 1, "x0,x1,x2,x3,x4", [256, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([1])
        self._expect_statistics(0, 1, "", [], fetcher_mode="from_master")

        make_table([])
        self._expect_statistics(0, 1, "x", [0], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 1)
        make_table([255, 42])
        self._expect_statistics(0, 1, "x0,x1,zzz", [255, 255, 255], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 0)
        make_table([256, 42])
        self._expect_statistics(
            0,
            1,
            "x0,x1,zzz",
            [0, 0, 0],
            fetcher_mode="from_master",
            expected_legacy_data_weight=299,
        )
        self._expect_statistics(0, 1, "x0,x1,zzz", [256, 42, 0], fetcher_mode="fallback")

    @authors("dakovalkov")
    def test_get_table_columnar_statistics_multi(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 10, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 20}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 100}])

        create("table", "//tmp/t2")
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t2", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        paths = []
        lower_row_indices = []
        upper_row_indices = []
        all_columns = []
        all_expected_data_weights = []

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(2)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([0, 0, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,c,x")
        all_expected_data_weights.append([130, 17, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(5)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(5)
        all_columns.append("a")
        all_expected_data_weights.append([120])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(4)
        all_columns.append("")
        all_expected_data_weights.append([])

        self._expect_multi_statistics(
            paths,
            lower_row_indices,
            upper_row_indices,
            all_columns,
            all_expected_data_weights,
        )

    @authors("achulkov2")
    def test_get_table_columnar_statistics_schema_fetching(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 10, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 20}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 100}])

        create("table", "//tmp/t2", attributes={
            "schema": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "any"}
            ]
        })
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t2", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        paths = []
        lower_row_indices = []
        upper_row_indices = []
        all_columns = []
        all_expected_data_weights = []
        schema_columns = []

        paths.append("//tmp/t")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("a,b")
        schema_columns.append(None)
        all_expected_data_weights.append([130, 8])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append(None)
        schema_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("c,a,x")
        schema_columns.append(None)
        all_expected_data_weights.append([17, 1300, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("a,b,c")
        schema_columns.append(None)
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(5)
        all_columns.append("a")
        schema_columns.append(None)
        all_expected_data_weights.append([120])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append(None)
        schema_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        self._expect_multi_statistics(
            paths,
            lower_row_indices,
            upper_row_indices,
            all_columns,
            all_expected_data_weights,
            schema_columns=schema_columns
        )

    @authors("max42")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_dynamic_tables(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@optimize_for", optimize_for)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 10080], expected_timestamp_weight=(8 * 10))

        rows = [{"key": i, "value": str(i // 2) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 20))

        sync_compact_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 20))

        rows = [{"key": i} for i in range(10)]
        delete_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 30))

        sync_compact_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 30))

    @authors("prime")
    def test_max_node_per_fetch(self):
        create("table", "//tmp/t")

        for _ in range(100):
            write_table("<append=%true>//tmp/t", [{"a": "x" * 100}, {"c": 1.2}])

        statistics0 = get_table_columnar_statistics('["//tmp/t{a}";]')
        statistics1 = get_table_columnar_statistics('["//tmp/t{a}";]', max_chunks_per_node_fetch=1)
        assert statistics0 == statistics1


##################################################################

class TestColumnarStatisticsOperations(_TestColumnarStatisticsBase):
    @authors("max42")
    def test_map_thin_column(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = map(
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1000},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_sorted_merge_thin_column(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "sort_order": "ascending", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
            },
        )
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = merge(in_="//tmp/t{b}", out="//tmp/d", spec={"data_weight_per_job": 1000})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_map_thin_column_dynamic(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        set("//tmp/t/@optimize_for", "scan")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        for i in range(10):
            insert_rows(
                "//tmp/t",
                [{"key": j, "value": "y" * 80} for j in range(i * 100, (i + 1) * 100)],
            )
            sync_flush_table("//tmp/t")
        create("table", "//tmp/d")
        wait(lambda: get("//tmp/t/@chunk_count") == 12)
        assert get("//tmp/t/@data_weight") == (8 + (80 + 8) + 8) * 10 ** 3
        self._expect_statistics(
            None,
            None,
            "key,value",
            [8 * 10 ** 3, (80 + 8) * 10 ** 3],
            expected_timestamp_weight=(8 * 1000),
        )
        op = map(
            in_="//tmp/t{key}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1600},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_empty_column_selector(self):
        create("table", "//tmp/t")
        create("table", "//tmp/d")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100}] * 100)
        op = merge(in_="//tmp/t{}", out="//tmp/d", spec={"data_weight_per_job": 10})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_table_file_in_sandbox(self):
        create("table", "//tmp/t")
        s = "x" * 100
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"a": s, "b": s, "c": s, "d": s, "e": s}])
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task": {
                            "job_count": 1,
                            "command": "exit",
                            "file_paths": ["<format=dsv>//tmp/t"],
                        },
                    },
                }
            )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t[#1:#4]"],
                    },
                },
            }
        )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t{b,c,d}"],
                    },
                },
            }
        )

    @authors("max42")
    def test_fetch_cancellation(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        # Restart controller agent to ensure our operation taking memory tagged statistics slot 0.
        with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE]):
            pass

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agents[0]
        )

        op = map(
            track=False,
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={
                "data_weight_per_job": 1000,
                "testing": {"cancellation_stage": "columnar_statistics_fetch"},
            },
            command="echo '{a=1}'",
        )

        with raises_yt_error("Test operation failure"):
            op.track()

        def operation_disposed():
            entry = get(controller_agent_orchid + "/tagged_memory_statistics/0")
            if entry["operation_id"] != op.id:
                return False
            return not entry["alive"]

        wait(operation_disposed)

    @authors("gritukan")
    def test_estimated_input_statistics(self):
        create(
            "table",
            "//tmp/in",
            attributes={"optimize_for": "scan", "compression_codec": "none"},
        )
        create("table", "//tmp/out")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/in",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        op = map(in_="//tmp/in{b}", out="//tmp/out", command="echo '{a=1}'")
        op.track()

        statistics = get(op.get_path() + "/@progress/estimated_input_statistics")
        assert 10000 <= statistics["uncompressed_data_size"] <= 12000
        assert 10000 <= statistics["compressed_data_size"] <= 12000


##################################################################


class TestColumnarStatisticsOperationsEarlyFinish(TestColumnarStatisticsOperations):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "user_file_limits": {
                "max_table_data_weight": 2000,
            },
            "operation_options": {
                "spec_template": {
                    "use_columnar_statistics": True,
                },
            },
            "tagged_memory_statistics_update_period": 100,
            "enable_columnar_statistics_early_finish": True,
            "fetcher": {
                "node_rpc_timeout": 3000
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "testing_options": {
                "columnar_statistics_chunk_meta_fetch_max_delay": 6000
            },
            "columnar_statistics_read_timeout_fraction": 0.3,
        }
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
    }

    @classmethod
    def _expect_statistics(cls, *args, **kwargs):
        if "enable_early_finish" not in kwargs:
            kwargs["enable_early_finish"] = True
        super(TestColumnarStatisticsOperationsEarlyFinish, cls)._expect_statistics(*args, **kwargs)


##################################################################


class TestColumnarStatisticsCommandEarlyFinish(_TestColumnarStatisticsBase):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "data_node": {
            "testing_options": {
                "columnar_statistics_chunk_meta_fetch_max_delay": 6000
            },
            "columnar_statistics_read_timeout_fraction": 0.3,
        }
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
    }

    @authors("achulkov2")
    def test_get_table_columnar_statistics_with_early_finish(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        with pytest.raises(YtError):
            self._expect_statistics(None, None, "a,b,c", [1900, 56, 65], enable_early_finish=False)

        self._expect_statistics(None, None, "a,b,c", [1900, 56, 65], enable_early_finish=True)


##################################################################


class TestColumnarStatisticsCommandEarlyFinishRpcProxy(TestColumnarStatisticsCommandEarlyFinish):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
    }


##################################################################


class TestColumnarStatisticsRenamedColumns(_TestColumnarStatisticsBase):
    @authors("levysotsky")
    def test_get_table_columnar_statistics(self):
        skip_if_renaming_disabled(self.Env)

        schema1 = make_schema([
            make_column("a", optional_type("string")),
            make_column("b", optional_type("int64")),
            make_column("c", optional_type("double")),
        ])
        schema2 = make_schema([
            make_column("a_new", optional_type("string"), stable_name="a"),
            make_column("c_new", optional_type("double"), stable_name="c"),
            make_column("b", optional_type("int64")),
        ])

        create("table", "//tmp/t", attributes={"schema": schema1})

        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])

        alter_table("//tmp/t", schema=schema2)

        write_table("<append=%true>//tmp/t", [{"a_new": "x" * 200}, {"c_new": 6.5}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c_new": 0.0}, {"a_new": "x" * 1000}])

        self._expect_statistics(2, 2, "a,b,c", [0, 0, 0])  # Note the wrong names.
        self._expect_statistics(2, 2, "a_new,b,c_new", [0, 0, 0])
        self._expect_statistics(0, 6, "a,b,c", [0, 8, 0])  # Note the wrong names.
        self._expect_statistics(0, 6, "a_new,b,c_new", [1300, 8, 24])
        self._expect_statistics(0, 6, "a_new,c_new,x", [1300, 24, 0])
        self._expect_statistics(1, 5, "a,b,c", [0, 8, 0])  # Note the wrong names.
        self._expect_statistics(1, 5, "a_new,b,c_new", [1300, 8, 24])
        self._expect_statistics(2, 5, "a_new", [1200])
        self._expect_statistics(1, 4, "", [])

        alter_table("//tmp/t", schema=schema1)

        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0.0}, {"a": "x" * 1000}])

        self._expect_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_statistics(0, 6, "a,b,c", [1300, 8, 24])
        self._expect_statistics(0, 6, "a,c,x", [1300, 24, 0])
        self._expect_statistics(1, 5, "a,b,c", [1300, 8, 24])
        self._expect_statistics(2, 5, "a", [1200])
        self._expect_statistics(1, 4, "", [])

    @authors("levysotsky")
    def test_map_thin_column(self):
        schema1 = make_schema([
            make_column("a", "string"),
            make_column("b", "string"),
        ])
        schema2 = make_schema([
            make_column("b_new", "string", stable_name="b"),
            make_column("a_new", "string", stable_name="a"),
        ])
        create("table", "//tmp/t", attributes={"optimize_for": "scan", "schema": schema1})
        create("table", "//tmp/d")
        for i in range(10):
            a_name = ["a", "a_new"][i % 2]
            b_name = ["b", "b_new"][i % 2]
            schema = [schema1, schema2][i % 2]
            alter_table("//tmp/t", schema=schema)
            write_table(
                "<append=%true>//tmp/t",
                [{a_name: "x" * 90, b_name: "y" * 10} for _ in range(100)],
            )

        for a_name, b_name, schema in [("a", "b", schema1), ("a_new", "b_new", schema2)]:
            alter_table("//tmp/t", schema=schema)
            assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
            self._expect_statistics(
                0, 1000,
                "{},{}".format(a_name, b_name),
                [90 * 10 ** 3, 10 * 10 ** 3],
            )
            op = map(
                in_="//tmp/t{{{}}}".format(b_name),
                out="//tmp/d",
                spec={"data_weight_per_job": 1000},
                command="echo '{a=1}'",
            )
            op.track()
            assert 9 <= get("//tmp/d/@chunk_count") <= 11


##################################################################


class TestColumnarStatisticsRpcProxy(TestColumnarStatistics):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
