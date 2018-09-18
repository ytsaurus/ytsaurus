from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

import pytest

class TestColumnarStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "max_user_file_table_data_weight": 2000,
            "operation_options" : {
                "spec_template" : {
                    "use_columnar_statistics" : True,
                },
            },
        },
    }

    def _expect_statistics(self, lower_row_index, upper_row_index, columns, expected_data_weights, expected_timestamp_weight=None):
        path = '["//tmp/t{{{0}}}[{1}:{2}]";]'.format(columns,
                                                "#" + str(lower_row_index) if lower_row_index is not None else "",
                                                "#" + str(upper_row_index) if upper_row_index is not None else "")
        statistics = get_table_columnar_statistics(path)[0]
        assert statistics["legacy_chunks_data_weight"] == 0
        assert statistics["column_data_weights"] == dict(zip(columns.split(','), expected_data_weights))
        if expected_timestamp_weight is not None:
            assert statistics["timestamp_total_weight"] == expected_timestamp_weight

    def _expect_multi_statistics(self, paths, lower_row_indices, upper_row_indices, all_columns, all_expected_data_weights, all_expected_timestamp_weight=None):
        assert len(paths) == len(all_columns)
        for index in range(len(paths)):
            paths[index] = '{0}{{{1}}}[{2}:{3}]'.format(paths[index], all_columns[index],
                                                "#" + str(lower_row_indices[index]) if lower_row_indices[index] is not None else "",
                                                "#" + str(upper_row_indices[index]) if upper_row_indices[index] is not None else "")
        yson_paths = "["
        for path in paths:
            yson_paths += '"' + path + '";'
        yson_paths += "]"
        allStatistics = get_table_columnar_statistics(yson_paths)
        assert len(allStatistics) == len(all_expected_data_weights)
        for index in range(len(allStatistics)):
            assert allStatistics[index]["legacy_chunks_data_weight"] == 0
            assert allStatistics[index]["column_data_weights"] == dict(zip(all_columns[index].split(','), all_expected_data_weights[index]))
            if all_expected_timestamp_weight is not None:
                assert allStatistics[index] == all_expected_timestamp_weight[index]

    def _create_simple_dynamic_table(self, path, optimize_for="lookup"):
        create("table", path,
               attributes = {
                   "schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}, {"name": "value", "type": "string"}],
                   "dynamic": True,
                   "optimize_for": optimize_for
               })

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

        self._expect_multi_statistics(paths, lower_row_indices, upper_row_indices, all_columns, all_expected_data_weights)

    def test_map_thin_column(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table("<append=%true>//tmp/t", [{"a": 'x' * 90, "b": 'y' * 10} for j in range(100)])
        assert get("//tmp/t/@data_weight") == 101 * 10**3
        self._expect_statistics(0, 1000, "a,b", [90 * 10**3, 10 * 10**3])
        op = map(in_="//tmp/t{b}",
                 out="//tmp/d",
                 spec={"data_weight_per_job": 1000},
                 command="echo '{a=1}'")
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    def test_sorted_merge_thin_column(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan", "schema": [{"name": "a", "sort_order": "ascending", "type": "string"},
                                                                                  {"name": "b", "type": "string"}]})
        create("table", "//tmp/d")
        for i in range(10):
            write_table("<append=%true>//tmp/t", [{"a": 'x' * 90, "b": 'y' * 10} for j in range(100)])
        assert get("//tmp/t/@data_weight") == 101 * 10**3
        self._expect_statistics(0, 1000, "a,b", [90 * 10**3, 10 * 10**3])
        op = merge(in_="//tmp/t{b}",
                   out="//tmp/d",
                   spec={"data_weight_per_job": 1000})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    def test_map_thin_column_dynamic(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@optimize_for", "scan")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        for i in range(10):
            insert_rows("//tmp/t", [{"key": j, "value": 'y' * 80} for j in range(i * 100, (i + 1) * 100)])
            sync_flush_table("//tmp/t")
        create("table", "//tmp/d")
        assert get("//tmp/t/@chunk_count") == 10
        assert get("//tmp/t/@data_weight") == (8 + (80 + 8) + 8) * 10**3
        self._expect_statistics(None, None, "key,value", [8 * 10**3, (80 + 8) * 10**3], expected_timestamp_weight=(8 * 1000))
        op = map(in_="//tmp/t{key}",
                 out="//tmp/d",
                 spec={"data_weight_per_job": 1600},
                 command="echo '{a=1}'")
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    def test_empty_column_selector(self):
        create("table", "//tmp/t")
        create("table", "//tmp/d")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100}] * 100)
        op = merge(in_="//tmp/t{}",
                   out="//tmp/d",
                   spec={"data_weight_per_job": 10})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    def test_table_file_in_sandbox(self):
        create("table", "//tmp/t")
        s = "x" * 100
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"a": s, "b": s, "c": s, "d": s, "e": s}])
        with pytest.raises(YtError):
            op = vanilla(
                dont_track=False,
                spec={
                    "tasks": {
                        "task": {
                            "job_count": 1,
                            "command": 'exit',
                            "file_paths": ["<format=dsv>//tmp/t"]
                        },
                    },
                })

        op = vanilla(
            dont_track=False,
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": 'exit',
                        "file_paths": ["<format=dsv>//tmp/t[#1:#4]"]
                    },
                },
            })

        op = vanilla(
            dont_track=False,
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": 'exit',
                        "file_paths": ["<format=dsv>//tmp/t{b,c,d}"]
                    },
                },
            })

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

