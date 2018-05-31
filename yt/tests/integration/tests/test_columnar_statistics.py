from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

import __builtin__
import pytest

class TestColumnarStatistics(YTEnvSetup):
    NUM_MASTERS = 1 
    NUM_NODES = 3
    NUM_SCHEDULERS = 1 

    def _expect_statistics(self, lower_row_index, upper_row_index, columns, expected_data_weights):
        path = "//tmp/t{{{0}}}[#{1}:#{2}]".format(columns, lower_row_index, upper_row_index)
        statistics = get_columnar_statistics(path)
        assert statistics["legacy_chunks_data_weight"] == 0
        assert statistics["column_data_weights"] == dict(zip(columns.split(','), expected_data_weights))

    def test_get_columnar_statistics(self):
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

