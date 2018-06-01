from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

import __builtin__
import pytest

class TestColumnarStatistics(YTEnvSetup):
    NUM_MASTERS = 1 
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "max_user_file_table_data_weight": 2000,
        }
    }

    def _expect_statistics(self, lower_row_index, upper_row_index, columns, expected_data_weights):
        path = "//tmp/t{{{0}}}[#{1}:#{2}]".format(columns, lower_row_index, upper_row_index)
        statistics = get_table_columnar_statistics(path)
        assert statistics["legacy_chunks_data_weight"] == 0
        assert statistics["column_data_weights"] == dict(zip(columns.split(','), expected_data_weights))

    def test_get_table_columnar_statistics(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])
        with pytest.raises(YtError):
            get_table_columnar_statistics("//tmp/t")
        self._expect_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_statistics(0, 6, "a,b,c", [1300, 8, 17])
        self._expect_statistics(0, 6, "a,c,x", [1300, 17, 0])
        self._expect_statistics(1, 5, "a,b,c", [1300, 8, 17])
        self._expect_statistics(2, 5, "a", [1200])
        self._expect_statistics(1, 4, "", [])

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
