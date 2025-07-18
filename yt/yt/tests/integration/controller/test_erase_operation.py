from yt_env_setup import YTEnvSetup
from yt_commands import authors, create, get, read_table, write_table, erase

from yt_type_helpers import make_schema

from yt.common import YtError

import pytest


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerEraseCommands(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("panin", "ignat")
    def test_empty_in(self):
        create("table", "//tmp/table")
        erase("//tmp/table[#0:#10]")
        assert read_table("//tmp/table") == []

    def _prepare_table(self):
        self.table = "//tmp/t_in"
        create("table", self.table)

        self.v = [
            {"key": 0, "value": 10},
            {"key": -5, "value": 150},
            {"key": -1, "value": 2},
            {"key": 20, "value": 4},
            {"key": 15, "value": -1},
        ]

        for row in self.v:
            write_table("<append=true>" + self.table, row)

    @authors("panin")
    def test_no_selectors(self):
        self._prepare_table()
        erase(self.table)
        assert read_table(self.table) == []

    @authors("ignat")
    def test_by_index(self):
        self._prepare_table()
        erase(self.table + "[#10:]")
        assert read_table(self.table) == self.v
        assert get(self.table + "/@chunk_count") == 5

        erase(self.table + "[#0:#2]")
        assert read_table(self.table) == self.v[2:]
        assert get(self.table + "/@chunk_count") == 3

        erase(self.table + "[#0:#1]")
        assert read_table(self.table) == self.v[3:]
        assert get(self.table + "/@chunk_count") == 2

        erase(self.table + "[#1:]")
        assert read_table(self.table) == [self.v[3]]
        assert get(self.table + "/@chunk_count") == 1

        with pytest.raises(YtError):
            erase(self.table + "[#1:#2,#3:#4]")

    # test combine when actually no data is removed
    @authors("panin", "ignat")
    def test_combine_without_remove(self):
        self._prepare_table()
        erase(self.table + "[#10:]", combine_chunks=True)
        assert read_table(self.table) == self.v
        assert get(self.table + "/@chunk_count") == 1

    # test combine when data is removed from the middle
    @authors("panin", "ignat")
    def test_combine_remove_from_middle(self):
        self._prepare_table()
        erase(self.table + "[#2:#4]", combine_chunks=True)
        assert read_table(self.table) == self.v[:2] + self.v[4:]
        assert get(self.table + "/@chunk_count") == 1

    @authors("panin", "ignat")
    def test_by_key_from_non_sorted(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", {"v": 42})

        with pytest.raises(YtError):
            erase("//tmp/table[:42]")

    @authors("ignat")
    def test_by_column(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", {"v": 42})

        with pytest.raises(YtError):
            erase("//tmp/table{v}")

        with pytest.raises(YtError):
            erase("//tmp/table{non_v}")

        with pytest.raises(YtError):
            erase("//tmp/table{}")

    def _prepare_medium_chunks(self):
        self.table = "//tmp/table"
        create("table", self.table)
        self.v = [
            {"key": 1},
            {"key": 0},
            {"key": 5},
            {"key": 8},
            {"key": 10},
            {"key": -3},
            {"key": -1},
            {"key": 7},
        ]

        write_table("<append=true>" + self.table, self.v[0:2])
        write_table("<append=true>" + self.table, self.v[2:4])
        write_table("<append=true>" + self.table, self.v[4:6])
        write_table("<append=true>" + self.table, self.v[6:8])

        assert get(self.table + "/@chunk_count") == 4

    @authors("panin", "ignat")
    def test_one_side_chunk(self):
        self._prepare_medium_chunks()
        erase(self.table + "[#1:#4]")
        assert read_table(self.table) == [self.v[0]] + self.v[4:]
        assert get(self.table + "/@chunk_count") == 3  # side chunks are not united

    @authors("panin", "ignat")
    def test_two_side_chunks(self):
        self._prepare_medium_chunks()
        erase(self.table + "[#1:#3]")
        assert read_table(self.table) == [self.v[0]] + self.v[3:]
        assert get(self.table + "/@chunk_count") == 3  # side chunks are united

    @authors("ignat")
    def test_by_key(self):
        v1 = [
            {"key": -100, "value": 20},
            {"key": -5, "value": 1},
            {"key": 0, "value": 76},
            {"key": 10, "value": 10},
            {"key": 42, "value": 124},
        ]

        v2 = [{"key": 100500, "value": -20}]
        v = v1 + v2

        create("table", "//tmp/table")
        write_table("//tmp/table", v1, sorted_by="key")
        write_table("<append=%true>//tmp/table", v2, sorted_by="key")

        erase("//tmp/table[0:42]")
        assert read_table("//tmp/table") == v[0:2] + v[4:6]
        assert get("//tmp/table/@sorted")  # check that table is still sorted

        erase("//tmp/table[1000:]")
        assert read_table("//tmp/table") == v[0:2] + v[4:5]
        assert get("//tmp/table/@sorted")  # check that table is still sorted

        erase("//tmp/table[:0]")
        assert read_table("//tmp/table") == v[4:5]
        assert get("//tmp/table/@sorted")  # check that table is still sorted

    @authors("babenko")
    def test_schema_validation(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ]
                )
            },
        )

        write_table("//tmp/table", [{"key": i, "value": "foo"} for i in range(10)])

        erase("//tmp/table[5:]")

        assert get("//tmp/table/@schema/@strict")
        assert get("//tmp/table/@schema_mode") == "strong"
        assert read_table("//tmp/table") == [{"key": i, "value": "foo"} for i in range(5)]

##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerEraseCommandsMulticell(TestSchedulerEraseCommands):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
