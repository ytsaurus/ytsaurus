
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestSchedulerEraseCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_empty_in(self):
        create("table", "//tmp/table")
        erase("//tmp/table[#0:#10]")
        assert read("//tmp/table") == []

###############################################################

    def _prepare_table(self):
        self.table = "//tmp/t_in"
        create("table", self.table)

        self.v = \
        [
            {"key": 0, "value": 10},
            {"key": -5, "value": 150},
            {"key": -1, "value": 2},
            {"key": 20, "value": 4},
            {"key": 15, "value": -1}
        ]

        for row in self.v:
            write("<append=true>" + self.table, row)

    def test_no_selectors(self):
        self._prepare_table()
        erase(self.table)
        assert read(self.table) == []

    def test_by_index(self):
        self._prepare_table()
        erase(self.table + "[#10:]")
        assert read(self.table) == self.v
        assert get(self.table + "/@chunk_count") == 5

        erase(self.table + "[#0:#2]")
        assert read(self.table) == self.v[2:]
        assert get(self.table + "/@chunk_count") == 3

        erase(self.table + "[#0:#1]")
        assert read(self.table) == self.v[3:]
        assert get(self.table + "/@chunk_count") == 2
                
        erase(self.table + "[#1:]")
        assert read(self.table) == [self.v[3]]
        assert get(self.table + "/@chunk_count") == 1

        with pytest.raises(YtError):
            erase(self.table + "[#1:#2,#3:#4]")

    # test combine when actually no data is removed
    def test_combine_without_remove(self):
        self._prepare_table()
        erase(self.table + "[#10:]", combine_chunks=True)
        assert read(self.table) == self.v
        assert get(self.table + "/@chunk_count") == 1

    # test combine when data is removed from the middle
    def test_combine_remove_from_middle(self):
        self._prepare_table()
        erase(self.table + "[#2:#4]", combine_chunks=True)
        assert read(self.table) == self.v[:2] + self.v[4:]
        assert get(self.table + "/@chunk_count") == 1

###############################################################

    def test_by_key_from_non_sorted(self):
        create("table", "//tmp/table")
        write("//tmp/table", {"v" : 42})

        with pytest.raises(YtError):
            erase("//tmp/table[:42]")

###############################################################

    @pytest.mark.xfail(run = False, reason = "Issue #151")
    def test_by_column(self):
        create("table", "//tmp/table")
        write("//tmp/table", {"v" : 42})
        
        with pytest.raises(YtError):
            erase("//tmp/table{v}")

        with pytest.raises(YtError):
            erase("//tmp/table{non_v}")

        with pytest.raises(YtError):
            erase("//tmp/table{}")

###############################################################

    def _prepare_medium_chunks(self):
        self.table = "//tmp/table"
        create("table", self.table)
        self.v = \
        [
            {"key" : 1},
            {"key" : 0},
            {"key" : 5},
            {"key" : 8},
            {"key" : 10},
            {"key" : -3},
            {"key" : -1},
            {"key" : 7}
        ]
        write("<append=true>" + self.table, self.v[0:2]) 
        write("<append=true>" + self.table, self.v[2:4]) 
        write("<append=true>" + self.table, self.v[4:6]) 
        write("<append=true>" + self.table, self.v[6:8]) 

        assert get(self.table + "/@chunk_count") == 4
    
    def test_one_side_chunk(self):
        self._prepare_medium_chunks()
        erase(self.table + "[#1:#4]")
        assert read(self.table) == [self.v[0]] + self.v[4:]
        assert get(self.table + "/@chunk_count") == 3 # side chunks are not united

    def test_two_side_chunks(self):
        self._prepare_medium_chunks()
        erase(self.table + "[#1:#3]")
        assert read(self.table) == [self.v[0]] + self.v[3:]
        assert get(self.table + "/@chunk_count") == 3 # side chunks are united

###############################################################

    def test_by_key(self):
        v = \
        [
            {"key": -100, "value": 20},
            {"key": -5, "value": 1},
            {"key": 0, "value": 76},
            {"key": 10, "value": 10},
            {"key": 42, "value": 124},
            {"key": 100500, "value": -20},
        ]
        create("table", "//tmp/table")
        write("//tmp/table", v, sorted_by="key")

        erase("//tmp/table[0:42]")
        assert read("//tmp/table") == v[0:2] + v[4:6]
        assert get("//tmp/table/@sorted") # check that table is still sorted

        erase("//tmp/table[1000:]")
        assert read("//tmp/table") == v[0:2] + v[4:5]
        assert get("//tmp/table/@sorted") # check that table is still sorted

        erase("//tmp/table[:0]")
        assert read("//tmp/table") == v[4:5]
        assert get("//tmp/table/@sorted") # check that table is still sorted



