from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, print_debug, wait, create, get, set,
    copy, remove,
    exists, concatenate, create_user, start_transaction, abort_transaction, commit_transaction, lock, alter_table, write_file, read_table,
    write_table, read_blob_table, map, map_reduce, merge,
    sort, remote_copy, get_first_chunk_id,
    get_singular_chunk_id, get_chunk_replication_factor, set_all_nodes_banned,
    get_recursive_disk_space, get_chunk_owner_disk_space, raises_yt_error, sorted_dicts,
)

from yt_helpers import skip_if_no_descending
from yt_type_helpers import make_schema, normalize_schema, list_type
import yt_error_codes

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import yt.wrapper

import pytest

import math
import random

##################################################################


class TestTables(YTEnvSetup):
    NUM_TEST_PARTITIONS = 10

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    def _wait_until_unlocked(self, path):
        wait(lambda: get(path + "/@lock_count") == 0)

    def _wait_until_no_nested_tx(self, tx_id):
        wait(lambda: get("#{}/@nested_transaction_ids".format(tx_id)) == [])

    @authors("ignat")
    def test_invalid_type(self):
        with pytest.raises(YtError):
            read_table("//tmp")
        with pytest.raises(YtError):
            write_table("//tmp", [])

    @authors("savrus", "ignat")
    def test_simple(self):
        create("table", "//tmp/table")

        assert read_table("//tmp/table") == []
        assert get("//tmp/table/@row_count") == 0
        assert get("//tmp/table/@chunk_count") == 0
        assert get("//tmp/table/@uncompressed_data_size") == 0
        assert get("//tmp/table/@compressed_data_size") == 0
        assert get("//tmp/table/@data_weight") == 0

        write_table("//tmp/table", {"b": "hello"})
        assert read_table("//tmp/table") == [{"b": "hello"}]
        assert get("//tmp/table/@row_count") == 1
        assert get("//tmp/table/@chunk_count") == 1
        assert get("//tmp/table/@uncompressed_data_size") == 13
        assert get("//tmp/table/@compressed_data_size") == 39
        assert get("//tmp/table/@data_weight") == 6

        write_table(
            "<append=true>//tmp/table",
            [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}],
        )
        assert read_table("//tmp/table") == [
            {"b": "hello"},
            {"a": "1", "b": "2"},
            {"a": "30", "x": "10", "y": "20"},
        ]
        assert get("//tmp/table/@row_count") == 3
        assert get("//tmp/table/@chunk_count") == 2
        assert get("//tmp/table/@uncompressed_data_size") == 46
        assert get("//tmp/table/@compressed_data_size") == 99
        assert get("//tmp/table/@data_weight") == 16

    @authors("psushin")
    def test_unavailable(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}])

        set_all_nodes_banned(True)

        with pytest.raises(YtError):
            read_table("//tmp/table")

        set_all_nodes_banned(False)

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_write_table(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        rows = [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/table",
            rows,
            sorted_by={"name": "key", "sort_order": sort_order},
        )

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["key"]
        assert get("//tmp/table/@row_count") == 4

        # sorted flag is discarded when writing unsorted data to sorted table
        next_key = 4 if sort_order == "ascending" else -1
        write_table("<append=true>//tmp/table", {"key": next_key})
        assert not get("//tmp/table/@sorted")
        with pytest.raises(YtError):
            get("//tmp/table/@sorted_by")

    @authors("monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_simple(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        first_chunk = [{"a": 0, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0}]
        second_chunk = [{"a": 1, "b": 0}, {"a": 2, "b": 0}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        sorted_by = [{"name": "a", "sort_order": sort_order}, {"name": "b", "sort_order": sort_order}]
        write_table(
            "//tmp/table",
            first_chunk,
            sorted_by=sorted_by,
        )
        write_table(
            "<append=true>//tmp/table",
            second_chunk,
            sorted_by=sorted_by,
        )

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["a", "b"]
        assert get("//tmp/table/@row_count") == 5

    @authors("shakurov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_simple_with_transaction(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        first_chunk = [{"a": 0, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0}]
        second_chunk = [{"a": 1, "b": 0}, {"a": 2, "b": 0}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        sorted_by = [{"name": "a", "sort_order": sort_order}]

        write_table("//tmp/table", first_chunk, sorted_by=sorted_by)

        tx_b1 = start_transaction()
        tx_b2 = start_transaction(tx=tx_b1)

        lock("//tmp/table", mode="exclusive", tx=tx_b2)
        abort_transaction(tx=tx_b2)

        write_table("<append=true>//tmp/table", second_chunk, sorted_by=sorted_by)

        tx_b3 = start_transaction(tx=tx_b1)

        with pytest.raises(YtError):
            write_table(
                "<append=true>//tmp/table",
                second_chunk,
                sorted_by=sorted_by,
                tx=tx_b3,
            )
        self._wait_until_unlocked("//tmp/table")
        self._wait_until_no_nested_tx(tx_b3)

        commit_transaction(tx_b3)
        commit_transaction(tx_b1)

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["a"]
        assert get("//tmp/table/@row_count") == 5

    @authors("monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_with_less_key_columns(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        first_chunk = [{"a": 0, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0}]
        second_chunk = [{"a": 1, "b": 0}, {"a": 2, "b": 0}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        write_table(
            "//tmp/table",
            first_chunk,
            sorted_by=[{"name": "a", "sort_order": sort_order}, {"name": "b", "sort_order": sort_order}],
        )
        write_table(
            "<append=true>//tmp/table",
            second_chunk,
            sorted_by=[{"name": "a", "sort_order": sort_order}],
        )

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["a"]
        assert get("//tmp/table/@row_count") == 5

    @authors("ignat", "monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_order_violated(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        first_chunk = [{"a": 1}, {"a": 2}]
        second_chunk = [{"a": 0}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        sorted_by = [{"name": "a", "sort_order": sort_order}]
        write_table("//tmp/table", first_chunk, sorted_by=sorted_by)
        with raises_yt_error(yt_error_codes.SortOrderViolation):
            write_table("<append=true>//tmp/table", second_chunk, sorted_by=sorted_by)
        self._wait_until_unlocked("//tmp/table")

    @authors("ignat", "monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_to_unsorted(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        first_chunk = [{"a": 2}, {"a": 1}, {"a": 0}]
        second_chunk = [{"a": 2}, {"a": 3}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        sorted_by = [{"name": "a", "sort_order": sort_order}]
        write_table("//tmp/table", first_chunk)
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", second_chunk, sorted_by=sorted_by)
        self._wait_until_unlocked("//tmp/table")

    @authors("ignat", "monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_with_more_key_columns(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")

        sorted_by_a = [{"name": "a", "sort_order": sort_order}]
        sorted_by_ab = [{"name": "a", "sort_order": sort_order}, {"name": "b", "sort_order": sort_order}]
        write_table("//tmp/table", [{"a": 0}], sorted_by=sorted_by_a)
        next_key = 1 if sort_order == "ascending" else -1
        with pytest.raises(YtError):
            write_table(
                "<append=true>//tmp/table",
                [{"a": next_key, "b": 0}],
                sorted_by=sorted_by_ab,
            )
        self._wait_until_unlocked("//tmp/table")

    @authors("ignat", "monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_with_different_key_columns(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")
        rows = [{"a": 0}, {"a": 1}, {"a": 2}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/table",
            rows,
            sorted_by=[{"name": "a", "sort_order": sort_order}])

        rows = [{"b": 0}, {"b": 1}]
        if sort_order == "descending":
            rows = rows[::-1]
        with pytest.raises(YtError):
            write_table(
                "<append=true>//tmp/table",
                [{"b": 0}, {"b": 1}],
                sorted_by=[{"name": "b", "sort_order": sort_order}])
        self._wait_until_unlocked("//tmp/table")

    @authors("monster")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_concurrently(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/table")
        tx1 = start_transaction()
        tx2 = start_transaction()
        first_chunk = [{"a": 0}, {"a": 1}]
        second_chunk = [{"a": 1}, {"a": 2}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
            second_chunk = second_chunk[::-1]
            first_chunk, second_chunk = second_chunk, first_chunk
        sorted_by = [{"name": "a", "sort_order": sort_order}]
        write_table("<append=true>//tmp/table", first_chunk, sorted_by=sorted_by, tx=tx1)
        with pytest.raises(YtError):
            write_table(
                "<append=true>//tmp/table",
                second_chunk,
                sorted_by=sorted_by,
                tx=tx2,
            )

        # Make sure tx2 does not hold a lock.
        locks = get("//tmp/table/@locks")
        assert len(locks) == 1
        assert locks[0]["transaction_id"] == tx1

    @authors("gritukan")
    def test_append_sorted_different_sort_order(self):
        skip_if_no_descending(self.Env)

        create("table", "//tmp/table")
        write_table(
            "//tmp/table",
            [{"a": 0}],
            sorted_by=[{"name": "a", "sort_order": "ascending"}])
        with pytest.raises(YtError):
            write_table(
                "<append=true>//tmp/table",
                [{"a": 0}],
                sorted_by=[{"name": "a", "sort_order": "descending"}])

    @authors("ignat")
    def test_append_overwrite_write_table(self):
        # Default (overwrite)
        create("table", "//tmp/table1")
        assert get("//tmp/table1/@row_count") == 0
        write_table("//tmp/table1", {"a": 0})
        assert get("//tmp/table1/@row_count") == 1
        write_table("//tmp/table1", {"a": 1})
        assert get("//tmp/table1/@row_count") == 1

        # Append
        create("table", "//tmp/table2")
        assert get("//tmp/table2/@row_count") == 0
        write_table("<append=true>//tmp/table2", {"a": 0})
        assert get("//tmp/table2/@row_count") == 1
        write_table("<append=true>//tmp/table2", {"a": 1})
        assert get("//tmp/table2/@row_count") == 2

        # Overwrite
        create("table", "//tmp/table3")
        assert get("//tmp/table3/@row_count") == 0
        write_table("<append=false>//tmp/table3", {"a": 0})
        assert get("//tmp/table3/@row_count") == 1
        write_table("<append=false>//tmp/table3", {"a": 1})
        assert get("//tmp/table3/@row_count") == 1

    @authors("psushin")
    def test_malformed_table_data(self):
        create("table", "//tmp/table")

        # we can write only list fragments
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", yson.loads(b"string"))
        self._wait_until_unlocked("//tmp/table")

        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", yson.loads(b"100"))
        self._wait_until_unlocked("//tmp/table")

        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", yson.loads(b"3.14"))
        self._wait_until_unlocked("//tmp/table")

        # check max_row_weight limit
        with pytest.raises(YtError):
            write_table("//tmp/table", {"a": "long_string"}, table_writer={"max_row_weight": 2})
        self._wait_until_unlocked("//tmp/table")

        # check max_key_weight limit
        with pytest.raises(YtError):
            write_table(
                "//tmp/table",
                {"a": "long_string"},
                sorted_by=["a"],
                table_writer={"max_key_weight": 2},
            )
        self._wait_until_unlocked("//tmp/table")

        # check duplicate ids
        with pytest.raises(YtError):
            write_table("//tmp/table", b"{a=version1; a=version2}", is_raw=True)
        self._wait_until_unlocked("//tmp/table")

    @authors("psushin")
    def test_cannot_read_file_as_table(self):
        content = b"some_data"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        with pytest.raises(YtError):
            read_table("//tmp/file")

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schemaful_write(self, optimize_for):
        create("table", "//tmp/table", attributes={"optimize_for": optimize_for})

        assert get("//tmp/table/@optimize_for") == optimize_for
        assert get("//tmp/table/@schema_mode") == "weak"

        with pytest.raises(YtError):
            # append and schema are not compatible
            write_table(
                "<append=true; schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
                [{"key": 1}],
            )
        self._wait_until_unlocked("//tmp/table")

        with pytest.raises(YtError):
            # sorted_by and schema are not compatible
            write_table(
                "<sorted_by=[a]; schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
                [{"key": 2}],
            )
        self._wait_until_unlocked("//tmp/table")

        with pytest.raises(YtError):
            # invalid schema - duplicate columns
            write_table(
                "<schema=[{name=key; type=int64};{name=key; type=string}]>//tmp/table",
                [],
            )
        self._wait_until_unlocked("//tmp/table")

        write_table(
            "<schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
            [{"key": 0}, {"key": 1}],
        )

        assert get("//tmp/table/@schema_mode") == "strong"
        assert read_table("//tmp/table") == [{"key": i} for i in range(2)]

        # schemas are equal so this must work; data is overwritten
        write_table(
            "<schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
            [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}],
        )
        assert read_table("//tmp/table") == [{"key": i} for i in range(4)]

        write_table("<append=true>//tmp/table", [{"key": 4}, {"key": 5}])
        assert get("//tmp/table/@schema_mode") == "strong"
        assert read_table("//tmp/table") == [{"key": i} for i in range(6)]

        # data is overwritten, schema is reset
        write_table("<schema=[{name=key; type=any}]>//tmp/table", [{"key": 4}, {"key": 5}])
        assert get("//tmp/table/@row_count") == 2

    @authors("prime")
    def test_write_with_optimize_for(self):
        create("table", "//tmp/table")

        write_table("<optimize_for=lookup>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@optimize_for") == "lookup"

        write_table("<optimize_for=scan>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@optimize_for") == "scan"

        with pytest.raises(YtError):
            write_table("<append=true;optimize_for=scan>//tmp/table", [{"key": 0}])
        self._wait_until_unlocked("//tmp/table")

    @authors("prime")
    def test_write_with_compression(self):
        create("table", "//tmp/table")

        write_table("<compression_codec=none>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@compression_codec") == "none"

        write_table("<compression_codec=lz4>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@compression_codec") == "lz4"

        with pytest.raises(YtError):
            write_table("<append=true;compression_codec=lz4>//tmp/table", [{"key": 0}])
        self._wait_until_unlocked("//tmp/table")

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_unique(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/table",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": sort_order}],
                    unique_keys=True,
                ),
            },
        )

        assert get("//tmp/table/@schema_mode") == "strong"

        rows = [{"key": 0}, {"key": 1}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/table", rows)

        key = 1 if sort_order == "ascending" else 0
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"key": key}])
        self._wait_until_unlocked("//tmp/table")

        key = 2 if sort_order == "ascending" else -1
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"key": key}, {"key": key}])
        self._wait_until_unlocked("//tmp/table")

        write_table("<append=true>//tmp/table", [{"key": key}])

        assert get("//tmp/table/@schema_mode") == "strong"
        assert get("//tmp/table/@schema/@unique_keys")
        if sort_order == "ascending":
            expected = [{"key": 0}, {"key": 1}, {"key": 2}]
        else:
            expected = [{"key": 1}, {"key": 0}, {"key": -1}]
        assert read_table("//tmp/table") == expected

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_empty_strict_schema(self, optimize_for):
        create("table", "//tmp/table")

        write_table(
            "<optimize_for={}; schema=<strict=%true>[]>//tmp/table".format(optimize_for),
            [{}, {}],
        )
        assert read_table("//tmp/table") == [{}, {}]

    @authors("savrus")
    def test_computed_columns(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "schema": [
                    {
                        "name": "k1",
                        "type": "int64",
                        "expression": "k2 * 2",
                        "sort_order": "ascending",
                    },
                    {"name": "k2", "type": "int64"},
                ]
            },
        )

        assert get("//tmp/table/@schema_mode") == "strong"

        with pytest.raises(YtError):
            write_table("//tmp/table", [{"k2": 1}, {"k2": 0}])
        self._wait_until_unlocked("//tmp/table")

        write_table("//tmp/table", [{"k2": 0}, {"k2": 1}])
        assert read_table("//tmp/table") == [{"k1": i * 2, "k2": i} for i in range(2)]

    @authors("panin", "ignat")
    def test_row_index_selector(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 0}, {"b": 1}, {"c": 2}, {"d": 3}])

        # closed ranges
        assert read_table("//tmp/table[#0:#2]") == [{"a": 0}, {"b": 1}]  # simple
        assert read_table("//tmp/table[#-1:#1]") == [{"a": 0}]  # left < min
        assert read_table("//tmp/table[#2:#5]") == [{"c": 2}, {"d": 3}]  # right > max
        assert read_table("//tmp/table[#-10:#-5]") == []  # negative indexes

        assert read_table("//tmp/table[#1:#1]") == []  # left = right
        assert read_table("//tmp/table[#3:#1]") == []  # left > right

        # open ranges
        assert read_table("//tmp/table[:]") == [{"a": 0}, {"b": 1}, {"c": 2}, {"d": 3}]
        assert read_table("//tmp/table[:#3]") == [{"a": 0}, {"b": 1}, {"c": 2}]
        assert read_table("//tmp/table[#2:]") == [{"c": 2}, {"d": 3}]

        # multiple ranges
        assert read_table("//tmp/table[:,:]") == [{"a": 0}, {"b": 1}, {"c": 2}, {"d": 3}] * 2
        assert read_table("//tmp/table[#1:#2,#3:#4]") == [{"b": 1}, {"d": 3}]
        assert read_table("//tmp/table[#0]") == [{"a": 0}]
        assert read_table("//tmp/table[#1]") == [{"b": 1}]

        # reading key selectors from unsorted table
        with pytest.raises(YtError):
            read_table("//tmp/table[:a]")

    @authors("psushin")
    def test_chunk_index_selector(self):
        create("table", "//tmp/table")

        write_table("<append=true>//tmp/table", [{"a": 0}])
        write_table("<append=true>//tmp/table", [{"b": 1}])
        write_table("<append=true>//tmp/table", [{"c": 2}])
        write_table("<append=true>//tmp/table", [{"d": 3}])
        write_table("<append=true>//tmp/table", [{"e": 4}])
        write_table("<append=true>//tmp/table", [{"f": 5}])
        write_table("<append=true>//tmp/table", [{"g": 6}])
        write_table("<append=true>//tmp/table", [{"h": 7}])

        assert len(get("//tmp/table/@chunk_ids")) == 8

        assert read_table("<upper_limit={chunk_index=1}>//tmp/table") == [{"a": 0}]
        assert read_table("<lower_limit={chunk_index=2}>//tmp/table") == [
            {"c": 2},
            {"d": 3},
            {"e": 4},
            {"f": 5},
            {"g": 6},
            {"h": 7},
        ]
        assert read_table("<lower_limit={chunk_index=1};upper_limit={chunk_index=2}>//tmp/table") == [{"b": 1}]
        assert read_table("<ranges=[{exact={chunk_index=1}}]>//tmp/table") == [{"b": 1}]

        rows = read_table("//tmp/table", unordered=True)
        d = dict()
        for r in rows:
            d.update(r)

        assert d == {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4, "f": 5, "g": 6, "h": 7}

    @authors("panin", "ignat")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_key_selector(self, optimize_for):
        create("table", "//tmp/table", attributes={"optimize_for": optimize_for})

        v1 = {"s": "a", "i": 0, "d": 15.5}
        v2 = {"s": "a", "i": 10, "d": 15.2}
        v3 = {"s": "b", "i": 5, "d": 20.0}
        v4 = {"s": "b", "i": 20, "d": 20.0}
        v5 = {"s": "c", "i": -100, "d": 10.0}

        values = [v1, v2, v3, v4, v5]
        write_table("//tmp/table", values, sorted_by=["s", "i", "d"])

        # possible empty ranges
        assert read_table("//tmp/table[a : a]") == []
        assert read_table("//tmp/table[(a, 1) : (a, 10)]") == []
        assert read_table("//tmp/table[b : a]") == []
        assert read_table("//tmp/table[(c, 0) : (a, 10)]") == []
        assert read_table("//tmp/table[(a, 10, 1e7) : (b, )]") == []

        # some typical cases
        assert read_table("//tmp/table[(a, 4) : (b, 20, 18.)]") == [v2, v3]
        assert read_table("//tmp/table[c:]") == [v5]
        assert read_table("//tmp/table[:(a, 10)]") == [v1]
        assert read_table("//tmp/table[:(a, 10),:(a, 10)]") == [v1, v1]
        assert read_table("//tmp/table[:(a, 11)]") == [v1, v2]
        assert read_table("//tmp/table[:]") == [v1, v2, v3, v4, v5]
        assert read_table("//tmp/table[a : b , b : c]") == [v1, v2, v3, v4]
        assert read_table("//tmp/table[a]") == [v1, v2]
        assert read_table("//tmp/table[(a,10)]") == [v2]
        assert read_table("//tmp/table[a,c]") == [v1, v2, v5]

        # combination of row and key selectors
        assert read_table("//tmp/table{s, d}[aa: (b, 10)]") == [{"s": "b", "d": 20.0}]

        # limits of different types
        assert read_table("//tmp/table[#0:c]") == [v1, v2, v3, v4]

    @authors("panin", "ignat", "gritukan", "gepardo")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_key_selector_descending(self, optimize_for):
        skip_if_no_descending(self.Env)

        create("table", "//tmp/table", attributes={
            "optimize_for": optimize_for,
            "schema": make_schema([
                {"name": "s", "type": "string", "sort_order": "descending"},
                {"name": "i", "type": "int64", "sort_order": "descending"},
                {"name": "d", "type": "double", "sort_order": "descending"},
            ])})

        v1 = {"s": "a", "i": 0, "d": 15.5}
        v2 = {"s": "a", "i": 10, "d": 15.2}
        v3 = {"s": "b", "i": 5, "d": 20.0}
        v4 = {"s": "b", "i": 20, "d": 20.0}
        v5 = {"s": "c", "i": -100, "d": 10.0}

        values = [v5, v4, v3, v2, v1]
        write_table("//tmp/table", values)

        # possible empty ranges
        assert read_table("//tmp/table[a : a]") == []
        assert read_table("//tmp/table[(a, 9) : (a, 0)]") == []
        assert read_table("//tmp/table[a : b]") == []
        assert read_table("//tmp/table[(a, 9) : (c, 10)]") == []

        # type conversion: double <-> int
        assert read_table("//tmp/table[(a, 10, 16) : (a, 0)]") == [v2]

        # some typical cases
        assert read_table("//tmp/table[:b]") == [v5]
        assert read_table("//tmp/table[(a, 10):]") == [v2, v1]
        assert read_table("//tmp/table[(a, 11):]") == [v2, v1]
        assert read_table("//tmp/table[:]") == [v5, v4, v3, v2, v1]
        assert read_table("//tmp/table[c : b , b : a]") == [v5, v4, v3]
        assert read_table("//tmp/table[a]") == [v2, v1]
        assert read_table("//tmp/table[(a,10)]") == [v2]
        assert read_table("//tmp/table[a,c]") == [v2, v1, v5]

        # combination of row and key selectors
        assert read_table("//tmp/table{s, d}[(b, 10):aa]") == [{"s": "b", "d": 20.0}]

        # limits of different types
        assert read_table("//tmp/table[#0:b]") == [v5]

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_key_selector_types(self, optimize_for):
        create("table", "//tmp/table", attributes={"optimize_for": optimize_for})

        v1 = {"s": None, "i": 0}
        v2 = {"s": None, "i": 1}
        v3 = {"s": 1, "i": 2}
        v4 = {"s": 2, "i": 3}
        v5 = {"s": yson.YsonUint64(3), "i": 4}
        v6 = {"s": yson.YsonUint64(4), "i": 5}
        v7 = {"s": 2.0, "i": 6}
        v8 = {"s": 4.0, "i": 7}
        v9 = {"s": False, "i": 8}
        v10 = {"s": True, "i": 9}
        v11 = {"s": "", "i": 10}
        v12 = {"s": "b", "i": 11}

        values = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12]
        write_table("//tmp/table", values, sorted_by=["s", "i"])

        assert read_table("//tmp/table[(#,0):(#,100)]") == [v1, v2]
        assert read_table("//tmp/table[(1,0):(2,100)]") == [v3, v4]
        assert read_table("//tmp/table[(3u,0):(4u,100)]") == [v5, v6]
        assert read_table("//tmp/table[(2.0,0):(4.0,100)]") == [v7, v8]
        assert read_table("//tmp/table[(%false,0):(%true,100)]") == [v9, v10]
        assert read_table('//tmp/table[("",0):("b",100)]') == [v11, v12]

    @authors("panin", "ignat")
    def test_column_selector(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1, "aa": 2, "b": 3, "bb": 4, "c": 5})
        # empty columns
        assert read_table("//tmp/table{}") == [{}]

        # single columms
        assert read_table("//tmp/table{a}") == [{"a": 1}]
        assert read_table("//tmp/table{a, }") == [{"a": 1}]  # extra comma
        assert read_table("//tmp/table{a, a}") == [{"a": 1}]
        assert read_table("//tmp/table{c, b}") == [{"b": 3, "c": 5}]
        assert read_table("//tmp/table{zzzzz}") == [{}]  # non existent column

        assert read_table("//tmp/table{a}") == [{"a": 1}]
        assert read_table("//tmp/table{a, }") == [{"a": 1}]  # extra comma
        assert read_table("//tmp/table{a, a}") == [{"a": 1}]
        assert read_table("//tmp/table{c, b}") == [{"b": 3, "c": 5}]
        assert read_table("//tmp/table{zzzzz}") == [{}]  # non existent column

    @authors("monster")
    def test_range_and_row_index(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}])

        v1 = yson.to_yson_type(None, attributes={"range_index": 0})
        v2 = yson.to_yson_type(None, attributes={"row_index": 0})
        v3 = {"a": 0}
        v4 = {"a": 1}
        v5 = {"a": 2}
        v6 = yson.to_yson_type(None, attributes={"range_index": 1})
        v7 = yson.to_yson_type(None, attributes={"row_index": 2})
        v8 = {"a": 2}
        v9 = {"a": 3}

        control_attributes = {"enable_range_index": True, "enable_row_index": True}
        result = read_table("//tmp/table[#0:#3, #2:#4]", control_attributes=control_attributes)
        assert result == [v1, v2, v3, v4, v5, v6, v7, v8, v9]

        # Test row_index without range index.
        control_attributes = {"enable_row_index": True}
        result = read_table("//tmp/table[#0:#3, #2:#4]", control_attributes=control_attributes)
        assert result == [v2, v3, v4, v5, v7, v8, v9]

    @authors("monster")
    def test_range_and_row_index2(self):
        create("table", "//tmp/table")

        write_table(
            "//tmp/table",
            [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}],
            sorted_by="a",
        )

        v1 = yson.to_yson_type(None, attributes={"range_index": 0})
        v2 = yson.to_yson_type(None, attributes={"row_index": 2})
        v3 = {"a": 2}
        v4 = {"a": 3}
        v5 = {"a": 4}

        control_attributes = {"enable_range_index": True, "enable_row_index": True}
        result = read_table("//tmp/table[2:5]", control_attributes=control_attributes)
        assert result == [v1, v2, v3, v4, v5]

    @authors("babenko")
    def test_row_key_selector_yt_4840(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        write_table(
            "<append=true>//tmp/table",
            [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}],
            sorted_by="a",
            tx=tx,
        )
        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        write_table(
            "<append=true>//tmp/table",
            [{"a": 3}, {"a": 4}, {"a": 5}],
            sorted_by="a",
            tx=tx,
        )
        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        assert read_table("//tmp/table[1:5]", tx=tx) == [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 3},
            {"a": 4},
        ]

    @authors("panin", "ignat")
    def test_shared_locks_two_chunks(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", {"a": 1}, tx=tx)
        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx)

        assert read_table("//tmp/table") == []
        assert read_table("//tmp/table", tx=tx) == [{"a": 1}, {"b": 2}]

        commit_transaction(tx)
        assert read_table("//tmp/table") == [{"a": 1}, {"b": 2}]

    @authors("ignat")
    def test_shared_locks_three_chunks(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", {"a": 1}, tx=tx)
        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx)
        write_table("<append=true>//tmp/table", {"c": 3}, tx=tx)

        assert read_table("//tmp/table") == []
        assert read_table("//tmp/table", tx=tx) == [{"a": 1}, {"b": 2}, {"c": 3}]

        commit_transaction(tx)
        assert read_table("//tmp/table") == [{"a": 1}, {"b": 2}, {"c": 3}]

    @authors("panin", "ignat")
    def test_shared_locks_parallel_tx(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1})

        tx1 = start_transaction()
        tx2 = start_transaction()

        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx1)

        write_table("<append=true>//tmp/table", {"c": 3}, tx=tx2)
        write_table("<append=true>//tmp/table", {"d": 4}, tx=tx2)

        # check which records are seen from different transactions
        assert read_table("//tmp/table") == [{"a": 1}]
        assert read_table("//tmp/table", tx=tx1) == [{"a": 1}, {"b": 2}]
        assert read_table("//tmp/table", tx=tx2) == [{"a": 1}, {"c": 3}, {"d": 4}]

        commit_transaction(tx2)
        assert read_table("//tmp/table") == [{"a": 1}, {"c": 3}, {"d": 4}]
        assert read_table("//tmp/table", tx=tx1) == [{"a": 1}, {"b": 2}]

        # now all records are in table in specific order
        commit_transaction(tx1)
        assert read_table("//tmp/table") == [{"a": 1}, {"c": 3}, {"d": 4}, {"b": 2}]

    @authors("savrus", "psushin")
    def test_set_schema_in_tx(self):
        create("table", "//tmp/table")

        tx1 = start_transaction()
        tx2 = start_transaction()

        schema = get("//tmp/table/@schema")
        schema1 = make_schema(
            [{"name": "a", "type": "string", "required": False}],
            strict=False,
            unique_keys=False,
        )
        schema2 = make_schema(
            [{"name": "b", "type": "string", "required": False}],
            strict=False,
            unique_keys=False,
        )

        alter_table("//tmp/table", schema=schema1, tx=tx1)

        with pytest.raises(YtError):
            alter_table("//tmp/table", schema=schema2, tx=tx2)

        assert normalize_schema(get("//tmp/table/@schema")) == schema
        assert normalize_schema(get("//tmp/table/@schema", tx=tx1)) == schema1
        assert normalize_schema(get("//tmp/table/@schema", tx=tx2)) == schema

        commit_transaction(tx1)
        abort_transaction(tx2)
        assert normalize_schema(get("//tmp/table/@schema")) == schema1

    @authors("panin", "ignat")
    def test_shared_locks_nested_tx(self):
        create("table", "//tmp/table")

        v1 = {"k": 1}
        v2 = {"k": 2}
        v3 = {"k": 3}
        v4 = {"k": 4}

        outer_tx = start_transaction()

        write_table("//tmp/table", v1, tx=outer_tx)

        inner_tx = start_transaction(tx=outer_tx)

        write_table("<append=true>//tmp/table", v2, tx=inner_tx)
        assert read_table("//tmp/table", tx=outer_tx) == [v1]
        assert read_table("//tmp/table", tx=inner_tx) == [v1, v2]

        # this won"t be seen from inner
        write_table("<append=true>//tmp/table", v3, tx=outer_tx)
        assert read_table("//tmp/table", tx=outer_tx) == [v1, v3]
        assert read_table("//tmp/table", tx=inner_tx) == [v1, v2]

        write_table("<append=true>//tmp/table", v4, tx=inner_tx)
        assert read_table("//tmp/table", tx=outer_tx) == [v1, v3]
        assert read_table("//tmp/table", tx=inner_tx) == [v1, v2, v4]

        commit_transaction(inner_tx)
        # order is not specified
        assert_items_equal(read_table("//tmp/table", tx=outer_tx), [v1, v2, v4, v3])

        commit_transaction(outer_tx)

    @authors("ignat")
    def test_codec_in_writer(self):
        create("table", "//tmp/table")
        set("//tmp/table/@compression_codec", "zlib_9")
        write_table("//tmp/table", {"b": "hello"})

        assert read_table("//tmp/table") == [{"b": "hello"}]

        chunk_id = get_first_chunk_id("//tmp/table")
        assert get("#%s/@compression_codec" % chunk_id) == "zlib_9"

    @authors("babenko", "ignat")
    def test_copy(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t"]

        assert read_table("//tmp/t") == [{"a": "b"}]

        copy("//tmp/t", "//tmp/t2")
        wait(lambda: sorted(get("#%s/@owning_nodes" % chunk_id)) == sorted(["//tmp/t", "//tmp/t2"]))
        assert read_table("//tmp/t2") == [{"a": "b"}]

        assert get("//tmp/t2/@resource_usage") == get("//tmp/t/@resource_usage")
        assert get("//tmp/t2/@replication_factor") == get("//tmp/t/@replication_factor")

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t2"]

        remove("//tmp/t2")

        wait(lambda: not exists("#%s" % chunk_id))

    @authors("ignat")
    def test_copy_to_the_same_table(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/t")

    @authors("babenko", "ignat")
    def test_copy_tx(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t"]

        tx = start_transaction()
        assert read_table("//tmp/t", tx=tx) == [{"a": "b"}]
        t2_id = copy("//tmp/t", "//tmp/t2", tx=tx)
        wait(
            lambda: sorted(get("#%s/@owning_nodes" % chunk_id))
            == sorted(
                [
                    "#" + t2_id,
                    "//tmp/t",
                    yson.to_yson_type("//tmp/t2", attributes={"transaction_id": tx}),
                ]
            )
        )
        assert read_table("//tmp/t2", tx=tx) == [{"a": "b"}]

        commit_transaction(tx)

        assert read_table("//tmp/t2") == [{"a": "b"}]

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert get("#{}/@owning_nodes".format(chunk_id)) == ["//tmp/t2"]

        remove("//tmp/t2")

        wait(lambda: not exists("#%s" % chunk_id))

    @authors("babenko", "ignat")
    def test_copy_not_sorted(self):
        create("table", "//tmp/t1")
        assert not get("//tmp/t1/@sorted")
        assert get("//tmp/t1/@key_columns") == []

        copy("//tmp/t1", "//tmp/t2")
        assert not get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@key_columns") == []

    @authors("babenko", "ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_copy_sorted(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        sort(
            in_="//tmp/t1",
            out="//tmp/t1",
            sort_by=[{"name": "key", "sort_order": sort_order}],
        )

        assert get("//tmp/t1/@sorted")
        assert get("//tmp/t1/@key_columns") == ["key"]

        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@key_columns") == ["key"]

    @authors("ignat")
    def test_remove_create_under_transaction(self):
        create("table", "//tmp/table_xxx")
        tx = start_transaction()

        remove("//tmp/table_xxx", tx=tx)
        create("table", "//tmp/table_xxx", tx=tx)

    @authors("ignat")
    def test_transaction_staff(self):
        create("table", "//tmp/table_xxx")

        tx = start_transaction()
        remove("//tmp/table_xxx", tx=tx)
        inner_tx = start_transaction(tx=tx)
        get("//tmp", tx=inner_tx)

    @authors("babenko")
    def test_exists(self):
        assert not exists("//tmp/t")
        assert not exists("<append=true>//tmp/t")

        create("table", "//tmp/t")
        assert exists("//tmp/t")
        assert not exists("//tmp/t/x")
        assert not exists("//tmp/t/1")
        assert not exists("//tmp/t/1/t")
        assert exists("<append=false>//tmp/t")
        assert exists("//tmp/t[:#100]")
        assert exists("//tmp/t/@")
        assert exists("//tmp/t/@chunk_ids")

    @authors("babenko", "ignat")
    def test_replication_factor_updates(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@replication_factor") == 3

        with pytest.raises(YtError):
            remove("//tmp/t/@replication_factor")
        with pytest.raises(YtError):
            set("//tmp/t/@replication_factor", 0)
        with pytest.raises(YtError):
            set("//tmp/t/@replication_factor", {})

        tx = start_transaction()
        with pytest.raises(YtError):
            set("//tmp/t/@replication_factor", 2, tx=tx)

    @authors("babenko", "ignat")
    def test_replication_factor_propagates_to_chunks(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 2)

        write_table("//tmp/t", {"foo": "bar"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get_chunk_replication_factor(chunk_id) == 2

    @authors("babenko")
    def test_replication_factor_recalculated_on_remove(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", {"foo": "bar"})

        chunk_id = get_singular_chunk_id("//tmp/t1")

        assert get_chunk_replication_factor(chunk_id) == 1

        copy("//tmp/t1", "//tmp/t2")
        set("//tmp/t2/@replication_factor", 2)

        wait(lambda: get_chunk_replication_factor(chunk_id) == 2)

        remove("//tmp/t2")

        wait(lambda: get_chunk_replication_factor(chunk_id) == 1)

    @authors("babenko", "ignat")
    def test_recursive_resource_usage(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})
        copy("//tmp/t1", "//tmp/t2")

        assert get_chunk_owner_disk_space("//tmp/t1") + get_chunk_owner_disk_space(
            "//tmp/t2"
        ) == get_recursive_disk_space("//tmp")

    @authors("ignat")
    def test_chunk_tree_balancer(self):
        create("table", "//tmp/t")
        for i in range(0, 40):
            write_table("<append=true>//tmp/t", {"a": "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        statistics = get("#" + chunk_list_id + "/@statistics")
        assert statistics["chunk_count"] == 40
        assert statistics["chunk_list_count"] == 2
        assert statistics["row_count"] == 40
        assert statistics["rank"] == 2

    @authors("psushin", "ignat", "babenko")
    @pytest.mark.skipif("True")  # very long test
    def test_chunk_tree_balancer_deep(self):
        create("table", "//tmp/t")
        tx_stack = list()
        tx = start_transaction()
        tx_stack.append(tx)

        for i in range(0, 1000):
            write_table("<append=true>//tmp/t", {"a": i}, tx=tx)

        chunk_list_id = get("//tmp/t/@chunk_list_id", tx=tx)
        statistics = get("#" + chunk_list_id + "/@statistics", tx=tx)
        assert statistics["chunk_count"] == 1000
        assert statistics["chunk_list_count"] == 2001
        assert statistics["row_count"] == 1000
        assert statistics["rank"] == 1001

        tbl_a = read_table("//tmp/t", tx=tx)

        commit_transaction(tx)

        def check():
            chunk_list_id = get("//tmp/t/@chunk_list_id")
            statistics = get("#" + chunk_list_id + "/@statistics")
            return (
                statistics["chunk_count"] == 1000
                and statistics["chunk_list_count"] == 2
                and statistics["row_count"] == 1000
                and statistics["rank"] == 2
            )

        wait(check)

        assert tbl_a == read_table("//tmp/t")

    def _check_replication_factor(self, path, expected_rf):
        chunk_ids = get(path + "/@chunk_ids")
        for id in chunk_ids:
            if get_chunk_replication_factor(id) != expected_rf:
                return False
        return True

    # In tests below we intentionally issue vital/replication_factor updates
    # using a temporary user "u"; cf. YT-3579.
    @authors("psushin", "babenko")
    def test_vital_update(self):
        create("table", "//tmp/t")
        create_user("u")
        for i in range(0, 5):
            write_table("<append=true>//tmp/t", {"a": "b"})

        def check_vital_chunks(is_vital):
            chunk_ids = get("//tmp/t/@chunk_ids")
            for id in chunk_ids:
                if get("#" + id + "/@vital") != is_vital:
                    return False
            return True

        assert get("//tmp/t/@vital")
        assert check_vital_chunks(True)

        set("//tmp/t/@vital", False, authenticated_user="u")
        assert not get("//tmp/t/@vital")
        wait(lambda: check_vital_chunks(False))

    @authors("babenko")
    def test_replication_factor_update1(self):
        create("table", "//tmp/t")
        create_user("u")
        for i in range(0, 5):
            write_table("<append=true>//tmp/t", {"a": "b"})
        set("//tmp/t/@replication_factor", 4, authenticated_user="u")
        wait(lambda: self._check_replication_factor("//tmp/t", 4))

    @authors("sandello", "babenko", "ignat")
    def test_replication_factor_update2(self):
        create("table", "//tmp/t")
        create_user("u")
        tx = start_transaction()
        for i in range(0, 5):
            write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 4, authenticated_user="u")
        commit_transaction(tx)
        wait(lambda: self._check_replication_factor("//tmp/t", 4))

    @authors("babenko")
    def test_replication_factor_update3(self):
        create("table", "//tmp/t")
        create_user("u")
        tx = start_transaction()
        for i in range(0, 5):
            write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 2, authenticated_user="u")
        commit_transaction(tx)
        wait(lambda: self._check_replication_factor("//tmp/t", 2))

    @authors("babenko")
    def test_key_columns(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "descending"},
                ]
            },
        )
        assert get("//tmp/t/@sorted")
        assert get("//tmp/t/@key_columns") == ["a", "b"]

    @authors("max42")
    def test_unavailable_descending_sort_order(self):
        set("//sys/accounts/tmp/@resource_limits/tablet_count", 10)

        schema_regular = yson.to_yson_type(
            [{"name": "k1", "type": "int64", "sort_order": "ascending"}, {"name": "v", "type": "int64"}],
            attributes={"unique_keys": True}
        )
        schema_descending = yson.to_yson_type(
            schema_regular[:1] + [{"name": "k2", "type": "int64", "sort_order": "descending"}] + schema_regular[1:],
            attributes={"unique_keys": True},
        )

        def create_static_descending(suffix):
            create("table", "//tmp/st_descending_" + suffix, attributes={"schema": schema_descending})

        def create_dynamic_descending(suffix):
            create("table", "//tmp/dt_descending_" + suffix, attributes={"schema": schema_descending, "dynamic": True})

        def alter_static_descending_to_dynamic_descending(suffix):
            path = "//tmp/st_descending2_" + suffix
            create("table", path, attributes={"schema": schema_descending})
            alter_table(path, dynamic=True)

        def alter_static_regular_to_static_descending(suffix):
            path = "//tmp/st_regular_" + suffix
            create("table", path, attributes={"schema": schema_regular})
            alter_table(path, schema=schema_descending)

        def alter_dynamic_regular_to_dynamic_descending(suffix):
            path = "//tmp/dt_regular_" + suffix
            create("table", path, attributes={"schema": schema_regular, "dynamic": True})
            alter_table(path, schema=schema_descending)

        # Both static and dynamic tables with descending sort order are allowed.
        suffix = "allow_all"
        create_static_descending(suffix)
        alter_static_regular_to_static_descending(suffix)
        alter_static_descending_to_dynamic_descending(suffix)
        create_dynamic_descending(suffix)
        alter_dynamic_regular_to_dynamic_descending(suffix)

        # Only static tables are allowed to have descending sort order.
        set("//sys/@config/enable_descending_sort_order_dynamic", False)
        suffix = "allow_static"
        create_static_descending(suffix)
        alter_static_regular_to_static_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_static_descending_to_dynamic_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            create_dynamic_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_dynamic_regular_to_dynamic_descending(suffix)

        # No tables are allowed to have descending sort order.
        suffix = "deny_all"
        set("//sys/@config/enable_descending_sort_order", False)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            create_static_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_static_regular_to_static_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_static_descending_to_dynamic_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            create_dynamic_descending(suffix)
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_dynamic_regular_to_dynamic_descending(suffix)

    @authors("babenko", "ignat")
    def test_statistics1(self):
        table = "//tmp/t"
        create("table", table)
        set("//tmp/t/@compression_codec", "snappy")
        write_table(table, {"foo": "bar"})

        for i in range(8):
            merge(in_=[table, table], out="<append=true>" + table)

        chunk_count = 3 ** 8
        assert get("//tmp/t/@row_count") == chunk_count
        assert len(get("//tmp/t/@chunk_ids")) == chunk_count

        codec_info = get("//tmp/t/@compression_statistics")
        assert codec_info["snappy"]["chunk_count"] == chunk_count

        erasure_info = get("//tmp/t/@erasure_statistics")
        assert erasure_info["none"]["chunk_count"] == chunk_count

    @authors("babenko", "ignat")
    def test_statistics2(self):
        tableA = "//tmp/a"
        create("table", tableA)
        write_table(tableA, {"foo": "bar"})

        tableB = "//tmp/b"
        create("table", tableB)
        set(tableB + "/@compression_codec", "snappy")

        map(in_=[tableA], out=[tableB], command="cat")

        codec_info = get(tableB + "/@compression_statistics")
        assert list(codec_info.keys()) == ["snappy"]

    @authors("asaitgalin")
    def test_optimize_for_statistics(self):
        table = "//tmp/a"
        create("table", table)
        write_table(table, {"foo": "bar"})

        optimize_for_info = get(table + "/@optimize_for_statistics")
        assert "lookup" in optimize_for_info and optimize_for_info["lookup"]["chunk_count"] == 1

        set(table + "/@optimize_for", "scan")

        for i in range(2):
            merge(
                in_=[table, table],
                out="<append=true>" + table,
                spec={"force_transform": True},
            )

        assert len(get("//tmp/a/@chunk_ids")) == 3

        optimize_for_info = get(table + "/@optimize_for_statistics")
        for key in ("scan", "lookup"):
            assert key in optimize_for_info
        assert optimize_for_info["lookup"]["chunk_count"] == 1
        assert optimize_for_info["scan"]["chunk_count"] == 2

    @authors("ignat")
    def test_json_format(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", b'{"x":"0"}\n{"x":"1"}', input_format="json", is_raw=True)
        assert b'{"x":"0"}\n{"x":"1"}\n' == read_table("//tmp/t", output_format="json")

    @authors("psushin")
    def test_yson_skip_nulls(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 0, "y": None}, {"x": None, "y": 1}])
        format = yson.loads(b"<skip_null_values=%true; format=text>yson")
        assert b'{"x"=0;};\n{"y"=1;};\n' == read_table("//tmp/t", output_format=format)
        del format.attributes["skip_null_values"]
        assert b'{"x"=0;"y"=#;};\n{"x"=#;"y"=1;};\n' == read_table("//tmp/t", output_format=format)

    @authors("ignat")
    def test_boolean(self):
        create("table", "//tmp/t")
        format = yson.loads(b"<format=text>yson")
        write_table(
            "//tmp/t",
            b"{x=%false};{x=%true};{x=false};",
            input_format=format,
            is_raw=True,
        )
        assert b'{"x"=%false;};\n{"x"=%true;};\n{"x"="false";};\n' == read_table("//tmp/t", output_format=format)

    @authors("babenko", "ignat", "lukyan")
    def test_uint64(self):
        create("table", "//tmp/t")
        format = yson.loads(b"<format=text>yson")
        write_table("//tmp/t", b"{x=1u};{x=4u};{x=9u};", input_format=format, is_raw=True)
        assert b'{"x"=1u;};\n{"x"=4u;};\n{"x"=9u;};\n' == read_table("//tmp/t", output_format=format)

    @authors("babenko")
    def test_concatenate(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        assert read_table("//tmp/t1") == [{"key": "x"}]

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        assert read_table("//tmp/t2") == [{"key": "y"}]

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}] * 2

    @authors("babenko")
    def test_concatenate_sorted(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert get("//tmp/t1/@sorted", "true")

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        sort(in_="//tmp/t2", out="//tmp/t2", sort_by="key")
        assert read_table("//tmp/t2") == [{"key": "y"}]
        assert get("//tmp/t2/@sorted", "true")

        create("table", "//tmp/union")
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t2", "//tmp/t1"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]
        assert get("//tmp/union/@sorted", "false")

    @authors("babenko")
    def test_extracting_table_columns_in_schemaful_dsv_from_complex_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table(
            "//tmp/t1",
            [
                {
                    "column1": {"childA": "some_value", "childB": "42"},
                    "column2": "value12",
                    "column3": "value13",
                },
                {
                    "column1": {"childA": "some_other_value", "childB": "321"},
                    "column2": "value22",
                    "column3": "value23",
                },
            ],
        )

        tabular_data = read_table(
            "//tmp/t1",
            output_format=yson.loads(b"<columns=[column2;column3]>schemaful_dsv"),
        )
        assert tabular_data == b"value12\tvalue13\nvalue22\tvalue23\n"

    @authors("babenko")
    def test_dynamic_table_schema_required(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"dynamic": True})

    @authors("savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_schema_validation(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        def init_table(path, schema):
            remove(path, force=True)
            create("table", path, attributes={"schema": schema})

        @authors("savrus")
        def test_positive(schema, rows):
            init_table("//tmp/t", schema)
            write_table("<append=%true>//tmp/t", rows)
            assert read_table("//tmp/t") == rows
            assert get("//tmp/t/@schema_mode") == "strong"
            assert normalize_schema(get("//tmp/t/@schema")) == schema

        @authors("savrus")
        def test_negative(schema, rows):
            init_table("//tmp/t", schema)
            with pytest.raises(YtError):
                write_table("<append=%true>//tmp/t", rows)
            self._wait_until_unlocked("//tmp/t")

        schema = make_schema(
            [{"name": "key", "type": "int64", "required": False}],
            strict=False,
            unique_keys=False,
        )
        test_positive(schema, [{"key": 1}])
        test_negative(schema, [{"key": False}])

        schema = make_schema(
            [{"name": "key", "type": "int64", "required": False}],
            strict=True,
            unique_keys=False,
        )
        test_negative(schema, [{"values": 1}])

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        if sort_order == "descending":
            rows = rows[::-1]

        schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "string", "required": False},
            ],
            strict=False,
            unique_keys=False,
        )
        test_positive(schema, rows)
        test_negative(schema, list(reversed(rows)))

        schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "string", "required": False},
            ],
            strict=False,
            unique_keys=True,
        )
        test_positive(schema, rows)

        rows = [{"key": 1, "value": str(i)} for i in range(10)]
        if sort_order == "descending":
            rows = rows[::-1]
        test_negative(schema, rows)

    @authors("max42")
    def test_type_conversion(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "int64", "type": "int64", "sort_order": "ascending"},
                        {"name": "uint64", "type": "uint64"},
                        {"name": "boolean", "type": "boolean"},
                        {"name": "double", "type": "double"},
                        {"name": "any", "type": "any"},
                    ],
                    strict=False,
                )
            },
        )

        row = b'{int64=3u; uint64=42; boolean="false"; double=18; any={}; extra=qwe}'

        yson_without_type_conversion = yson.loads(b"yson")
        yson_with_type_conversion = yson.loads(b"<enable_type_conversion=%true>yson")

        with pytest.raises(YtError):
            write_table("//tmp/t", row, is_raw=True, input_format=yson_without_type_conversion)
        self._wait_until_unlocked("//tmp/t")

        write_table("//tmp/t", row, is_raw=True, input_format=yson_with_type_conversion)

    @authors("savrus")
    def test_writer_config(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none",
            },
        )

        write_table("//tmp/t", [{"value": "A" * 1024} for i in range(10)])
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("ignat")
    def test_read_blob_table(self):
        create("table", "//tmp/ttt")
        write_table(
            "<sorted_by=[key]>//tmp/ttt",
            [
                {"key": "x", "part_index": 0, "data": "hello "},
                {"key": "x", "part_index": 1, "data": "world!"},
                {"key": "y", "part_index": 0, "data": "AAA"},
                {"key": "z", "part_index": 0},
                {"key": "za", "part_index": 0, "data": "abacaba"},
                {"key": "za", "part_index": 1, "data": "abac"},
                {"key": "zb", "part_index": 0, "data": "abacaba"},
                {"key": "zb", "part_index": 1, "data": "abacabacab"},
                {"key": "zc", "part_index": 0, "data": "abac"},
                {"key": "zc", "part_index": 1, "data": "abacaba"},
            ],
        )

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt", part_size=6)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[#2:#4]", part_size=3)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[:#3]", part_size=6)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[:#1]")

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[:#1]", start_part_index=1, part_size=6)

        assert b"hello " == read_blob_table("//tmp/ttt[:#1]", part_size=6)
        assert b"world!" == read_blob_table("//tmp/ttt[#1:#2]", part_size=6, start_part_index=1)
        assert b"rld!" == read_blob_table("//tmp/ttt[#1:#2]", part_size=6, start_part_index=1, offset=2)
        assert b"hello world!" == read_blob_table("//tmp/ttt[:#2]", part_size=6)
        assert b"AAA" == read_blob_table("//tmp/ttt[#2]", part_size=3)

        assert b"hello world!" == read_blob_table("//tmp/ttt[x]", part_size=6)
        assert b"AAA" == read_blob_table("//tmp/ttt[y]", part_size=3)
        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[x:z]", part_size=3)

        assert b"abacabaabac" == read_blob_table("//tmp/ttt[za]", part_size=7)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[zb]", part_size=7)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[zc]", part_size=7)

        write_table(
            "//tmp/ttt",
            [
                {"key": "x", "index": 0, "value": "hello "},
                {"key": "x", "index": 1, "value": "world!"},
            ],
        )
        assert b"hello world!" == read_blob_table(
            "//tmp/ttt",
            part_index_column_name="index",
            data_column_name="value",
            part_size=6,
        )

    @authors("savrus", "gritukan")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_chunk_format_statistics(self, optimize_for):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        write_table("//tmp/t", [{"a": "b"}])
        table_chunk_format = "unversioned_schemaless_horizontal" if optimize_for == "lookup" else "unversioned_columnar"
        chunk_format = "table_unversioned_schemaless_horizontal" if optimize_for == "lookup" else "table_unversioned_columnar"
        assert get("//tmp/t/@table_chunk_format_statistics/{0}/chunk_count".format(table_chunk_format)) == 1
        assert get("//tmp/t/@chunk_format_statistics/{0}/chunk_count".format(chunk_format)) == 1
        chunk = get_singular_chunk_id("//tmp/t")
        assert get("#{0}/@table_chunk_format".format(chunk)) == table_chunk_format
        assert get("#{0}/@chunk_format".format(chunk)) == chunk_format

    @authors("savrus")
    def test_get_start_row_index(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        write_table("//tmp/t", [{"key": 1, "value": "a"}, {"key": 1, "value": "b"}])
        write_table(
            "<append=%true>//tmp/t",
            [{"key": 1, "value": "c"}, {"key": 2, "value": "a"}],
        )
        response_parameters = {}
        read_table(
            "//tmp/t[(2)]",
            start_row_index_only=True,
            response_parameters=response_parameters,
        )
        assert response_parameters["start_row_index"] == 3

    @authors("shakurov")
    def test_attr_copy_on_lock(self):
        attributes = {
            "optimize_for": "scan",
            "atomicity": "none",
            "commit_ordering": "strong",
            "in_memory_mode": "uncompressed",
            "desired_tablet_count": 1,
        }

        create("table", "//tmp/t", attributes=attributes)
        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        for k, v in attributes.items():
            assert get("//tmp/t/@" + k, tx=tx) == v

        copy("//tmp/t", "//tmp/t2", tx=tx)
        commit_transaction(tx)
        for k, v in attributes.items():
            assert get("//tmp/t2/@" + k) == v

    @authors("savrus")
    def test_max_read_duration(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", {"b": "hello"})

        with pytest.raises(YtError):
            read_table("//tmp/table", table_reader={"max_read_duration": 0})

    def _print_chunk_list_recursive(self, chunk_list):
        result = []

        def recursive(chunk_list, level):
            t = get("#{0}/@type".format(chunk_list))
            result.append([level, chunk_list, t, None, None])
            if t == "chunk":
                r = get("#{0}/@row_count".format(chunk_list))
                u = get("#{0}/@uncompressed_data_size".format(chunk_list))
                result[-1][3] = {"row_count": r, "data_size": u}
            if t == "chunk_list":
                s = get("#{0}/@statistics".format(chunk_list))
                cs = get("#{0}/@cumulative_statistics".format(chunk_list))
                result[-1][3] = s
                result[-1][4] = cs
                for c in get("#{0}/@child_ids".format(chunk_list)):
                    recursive(c, level + 1)

        recursive(chunk_list, 0)
        for r in result:
            print_debug("%s%s %s %s %s" % ("   " * r[0], r[1], r[2], r[3], r[4]))

    @authors("savrus", "ifsmirnov")
    def test_cumulative_statistics(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "hello"})
        write_table("<append=true>//tmp/t", [{"c": "hello"}, {"d": "hello"}])

        chunk_list = get("//tmp/t/@chunk_list_id")
        statistics = get("#{0}/@cumulative_statistics".format(chunk_list))
        assert len(statistics) == 2
        assert statistics[1]["row_count"] == 3
        assert statistics[1]["chunk_count"] == 2

        chunk_list = get("#{0}/@child_ids".format(chunk_list))[0]
        statistics = get("#{0}/@cumulative_statistics".format(chunk_list))
        assert len(statistics) == 3
        assert statistics[1]["row_count"] == 1
        assert statistics[2]["row_count"] == 3

        write_table(
            "//tmp/t",
            [{"key": str(i)} for i in range(0, 2)],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )

        chunk_list = get("//tmp/t/@chunk_list_id")
        statistics = get("#{0}/@cumulative_statistics".format(chunk_list))
        assert len(statistics) == 3
        assert statistics[0]["row_count"] == 0
        assert statistics[1]["row_count"] == 1
        assert statistics[2]["row_count"] == 2
        assert statistics[0]["chunk_count"] == 0
        assert statistics[1]["chunk_count"] == 1
        assert statistics[2]["chunk_count"] == 2

    @authors("babenko", "shakurov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_sorted_to_corrupted_table_YT_11060(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t")

        sorted_by = '[{name=key;sort_order=' + sort_order + '}]'
        mul = 1 if sort_order == "ascending" else -1

        write_table("<append=true; sorted_by={}>//tmp/t".format(sorted_by), {"key": 1 * mul})
        assert get("//tmp/t/@sorted")

        tx1 = start_transaction()
        tx11 = start_transaction(tx=tx1)
        lock("//tmp/t", tx=tx11, mode="exclusive")
        abort_transaction(tx11)

        write_table("<append=true>//tmp/t", [{"key": 3 * mul}, {"key": 2 * mul}])

        tx12 = start_transaction(tx=tx1)
        lock("//tmp/t", tx=tx12, mode="exclusive")
        commit_transaction(tx=tx12)

        assert not get("//tmp/t/@sorted", tx=tx1)
        commit_transaction(tx=tx1)
        assert not get("//tmp/t/@sorted")

        with pytest.raises(YtError):
            write_table(
                "<append=true;sorted_by={}>//tmp/t".format(sorted_by),
                [{"key": 15 * mul}, {"key": 20 * mul}, {"key": 25 * mul}],
            )
        self._wait_until_unlocked("//tmp/t")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_boundary_keys_attribute(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                        {"name": "b", "type": "string"},
                    ]
                )
            },
        )

        assert get("//tmp/t1/@boundary_keys") == {}
        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "x"}, {"a": 3, "b": "x"}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows)
        assert get("//tmp/t1/@boundary_keys/min_key") == [rows[0]["a"]]
        assert get("//tmp/t1/@boundary_keys/max_key") == [rows[-1]["a"]]

        create("table", "//tmp/t2")
        assert not exists("//tmp/t2/@boundary_keys")

    @authors("gritukan")
    def test_chunk_sort_columns(self):
        def make_row(a, b, c):
            return {"a": a, "b": b, "c": c}

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "descending"},
                        {"name": "b", "type": "int64", "sort_order": "ascending"},
                        {"name": "c", "type": "int64"},
                    ]
                )
            },
        )

        write_table("<chunk_sort_columns=[{name=a;sort_order=descending};{name=b;sort_order=ascending}]>//tmp/t1",
                    [make_row(-1, 2, 3), make_row(-4, 5, 6)])
        assert read_table("//tmp/t1") == [make_row(-1, 2, 3), make_row(-4, 5, 6)]

        with raises_yt_error(yt_error_codes.SchemaViolation):
            write_table("<chunk_sort_columns=[{name=a;sort_order=descending}]>//tmp/t1", [make_row(-31, 41, 59)])

        with raises_yt_error(yt_error_codes.IncompatibleKeyColumns):
            write_table("<chunk_sort_columns=[{name=a;sort_order=ascending};{name=b;sort_order=ascending}]>//tmp/t1",
                        [make_row(-31, 41, 59)])

        with raises_yt_error(yt_error_codes.IncompatibleKeyColumns):
            write_table("<chunk_sort_columns=[{name=f;sort_order=ascending};{name=b;sort_order=ascending}]>//tmp/t1",
                        [make_row(-31, 41, 59)])

        with raises_yt_error(yt_error_codes.IncompatibleKeyColumns):
            write_table(
                "<chunk_sort_columns=["
                "{name=a;sort_order=descending};{name=b;sort_order=ascending};{name=d;sort_order=ascending}"
                "]>//tmp/t1",
                [{"a": -31, "b": 41, "d": 59, "c": 23}])

        with raises_yt_error(yt_error_codes.SortOrderViolation):
            write_table(
                "<chunk_sort_columns=["
                "{name=a;sort_order=descending};{name=b;sort_order=ascending};{name=c;sort_order=ascending}"
                "]>//tmp/t1",
                [make_row(-100, 200, 300), make_row(-100, 200, 100)],
            )

        # Check, whether boundary keys are validated by table schema not chunk schema.
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "int64"},
                        {"name": "c", "type": "int64"},
                    ]
                )
            },
        )

        write_table("<chunk_sort_columns=[{name=a;sort_order=descending};{name=b;sort_order=ascending}]>//tmp/t3",
                    [make_row(-3, 3, 3), make_row(-4, 4, 4)])
        write_table(
            "<chunk_sort_columns=[{name=a;sort_order=descending};{name=b;sort_order=ascending}];append=true>//tmp/t3",
            [make_row(-1, 1, 1), make_row(-2, 2, 2)],
        )
        assert read_table("//tmp/t3") == [
            make_row(-3, 3, 3),
            make_row(-4, 4, 4),
            make_row(-1, 1, 1),
            make_row(-2, 2, 2),
        ]

    @authors("gritukan")
    def test_chunk_sort_columns_locks(self):
        # Check whether shared lock is enough to append ordered chunks to unordered table.
        create(
            "table",
            "//tmp/t",
            attributes={"schema": make_schema([{"name": "a", "type": "int64"}])},
        )

        tx1 = start_transaction()
        tx2 = start_transaction()

        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t",
            [{"a": 1}],
            tx=tx1,
        )
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t",
            [{"a": 2}],
            tx=tx2,
        )

        commit_transaction(tx=tx1)
        commit_transaction(tx=tx2)

        assert get("//tmp/t/@chunk_count") == 2
        assert sorted_dicts(read_table("//tmp/t")) == [{"a": 1}, {"a": 2}]

    @authors("gritukan")
    def test_chunk_sort_columns_unique_keys_violation(self):
        def make_rows(values):
            return [{"a": value} for value in values]

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                    ],
                    unique_keys=True,
                )
            },
        )

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                    ],
                    unique_keys=False,
                )
            },
        )

        with raises_yt_error(yt_error_codes.SchemaViolation):
            write_table(
                "<chunk_sort_columns=[a];append=true>//tmp/t1",
                make_rows([1, 2, 2, 3]),
            )

        write_table("<chunk_sort_columns=[a];append=true>//tmp/t2", make_rows([1, 2, 2, 3]))
        write_table("<chunk_sort_columns=[a];append=true>//tmp/t2", make_rows([3, 4, 4, 5]))

        assert read_table("//tmp/t2") == make_rows([1, 2, 2, 3, 3, 4, 4, 5])

    @authors("gritukan")
    def test_chunk_sort_columns_unique_keys(self):
        def make_rows(values):
            return [{"a": value, "b": 0} for value in values]

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                        {"name": "b", "type": "int64"},
                    ],
                    unique_keys=False,
                )
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "int64"},
                    ]
                )
            },
        )

        # Write simple chunk.
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t1",
            make_rows([1]),
        )
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t2",
            make_rows([1]),
        )

        # Keys inside chunks are unique, but resulting table has two same keys.
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t1",
            make_rows([1]),
        )
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t2",
            make_rows([1]),
        )

        # Keys inside chunks are not unique.
        with raises_yt_error(yt_error_codes.UniqueKeyViolation):
            write_table(
                "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t1",
                make_rows([2, 2]),
            )
        with raises_yt_error(yt_error_codes.UniqueKeyViolation):
            write_table(
                "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t2",
                make_rows([2, 2]),
            )

        # `key_column_count' infers from table schema.
        write_table("<chunk_unique_keys=true;append=true>//tmp/t1", make_rows([3]))
        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            write_table("<chunk_unique_keys=true;append=true>//tmp/t2", make_rows([3]))

        # Keys are not ordered between chunks.
        with raises_yt_error(yt_error_codes.SortOrderViolation):
            write_table(
                "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t1",
                make_rows([0]),
            )
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=true;append=true>//tmp/t2",
            make_rows([0]),
        )

        # Do not check unique keys inside chunk.
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=false;append=true>//tmp/t1",
            make_rows([4, 4]),
        )
        write_table(
            "<chunk_sort_columns=[a];chunk_unique_keys=false;append=true>//tmp/t2",
            make_rows([4, 4]),
        )

        write_table(
            "<chunk_sort_columns=[a;b];chunk_unique_keys=true;append=true>//tmp/t1",
            [{"a": 5, "b": 1}, {"a": 5, "b": 2}],
        )
        write_table(
            "<chunk_sort_columns=[a;b];chunk_unique_keys=true;append=true>//tmp/t2",
            [{"a": 5, "b": 1}, {"a": 5, "b": 2}],
        )

        assert read_table("//tmp/t1") == make_rows([1, 1, 3, 4, 4]) + [
            {"a": 5, "b": 1},
            {"a": 5, "b": 2},
        ]
        assert read_table("//tmp/t2") == make_rows([1, 1, 0, 4, 4]) + [
            {"a": 5, "b": 1},
            {"a": 5, "b": 2},
        ]

    @authors("gritukan")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("unique_keys", [False, True])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_alter_key_column(self, unique_keys, optimize_for, sort_order):
        old_schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "int64", "required": False},
            ],
            unique_keys=unique_keys,
            strict=True,
        )
        new_schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "int64", "required": False},
            ],
            unique_keys=unique_keys,
            strict=True,
        )
        bad_schema_1 = make_schema(
            [
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "int64", "required": False},
            ],
            unique_keys=unique_keys,
            strict=True,
        )
        bad_schema_2 = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "int64", "required": False},
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
            ],
            unique_keys=unique_keys,
            strict=True,
        )
        opposite_sort_order = "descending" if sort_order == "ascending" else "ascending"
        bad_schema_3 = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": opposite_sort_order,
                },
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "int64", "required": False},
            ],
            unique_keys=unique_keys,
            strict=True,
        )

        create(
            "table",
            "//tmp/t",
            attributes={"schema": old_schema, "optimize_for": optimize_for},
        )
        rows = [{"key": 1, "value": 1}, {"key": 2, "value": 2}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t", rows[0])
        write_table("<append=true>//tmp/t", rows[1])
        assert get("//tmp/t/@chunk_count") == 2

        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table("//tmp/t", schema=bad_schema_1)

        with raises_yt_error(yt_error_codes.InvalidSchemaValue):
            alter_table("//tmp/t", schema=bad_schema_2)

        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table("//tmp/t", schema=bad_schema_3)

        alter_table("//tmp/t", schema=new_schema)

        assert read_table("//tmp/t") == rows

        # TODO(gritukan): Descending merge.
        if sort_order == "ascending":
            create("table", "//tmp/t_out", attributes={"schema": new_schema})
            merge(combine_chunks=True, mode="sorted", in_=["//tmp/t"], out="//tmp/t_out")

            assert read_table("//tmp/t_out") == [
                {"key": 1, "value": 1, "x": yson.YsonEntity()},
                {"key": 2, "value": 2, "x": yson.YsonEntity()},
            ]
            assert get("//tmp/t_out/@chunk_count") == 1

            with raises_yt_error(yt_error_codes.IncompatibleSchemas):
                alter_table("//tmp/t", schema=old_schema)

    @authors("akozhikhov")
    @pytest.mark.parametrize("key_columns_action", ["shorten", "widen"])
    def test_traverse_table_with_alter_and_ranges(self, key_columns_action):
        schema1 = make_schema(
            [
                {"name": "key1", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "value1", "type": "string"},
            ],
            unique_keys=True,
            strict=True,
        )
        schema2 = make_schema(
            [
                {"name": "key1", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64"},
                {"name": "value1", "type": "string"},
            ],
            unique_keys=False,
            strict=True,
        )
        schema3 = make_schema(
            [
                {"name": "key1", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "key3", "type": "int64", "sort_order": "ascending"},
                {"name": "value1", "type": "string"},
            ],
            unique_keys=True,
            strict=True,
        )

        create("table", "//tmp/t", attributes={"schema": schema1})

        row = [{"key1": 0, "key2": 0, "value1": "0"}]
        write_table("//tmp/t", row)

        expected_before_action = [
            row,
            [],
            row,
            [],
            [],
            [],
            [],
            row,
            row,
            row,
            row,
            row,
            [],
            [],
            [],
            row,
        ]
        expected_after_shortening = [
            row,
            [],
            [],
            row,
            [],
            [],
            [],
            row,
            row,
            row,
            [],
            [],
            [],
            row,
            row,
            row,
        ]
        expected_after_widening = [
            row,
            [],
            row,
            [],
            row,
            row,
            [],
            [],
            [],
            row,
            row,
            row,
            [],
            [],
            [],
            row,
        ]

        def _check(expected):
            assert read_table("<ranges=[{lower_limit={key=[0]}}]>//tmp/t") == expected[0]
            assert read_table("<ranges=[{upper_limit={key=[0]}}]>//tmp/t") == expected[1]

            assert read_table("<ranges=[{lower_limit={key=[0; 0]}}]>//tmp/t") == expected[2]
            assert read_table("<ranges=[{upper_limit={key=[0; 0]}}]>//tmp/t") == expected[3]

            # 3 key columns
            assert read_table("<ranges=[{lower_limit={key=[0; 0; <type=min>#]}}]>//tmp/t") == expected[4]
            assert read_table("<ranges=[{lower_limit={key=[0; 0; <type=null>#]}}]>//tmp/t") == expected[5]
            assert read_table("<ranges=[{lower_limit={key=[0; 0; <type=max>#]}}]>//tmp/t") == expected[6]

            assert read_table("<ranges=[{upper_limit={key=[0; 0; <type=min>#]}}]>//tmp/t") == expected[7]
            assert read_table("<ranges=[{upper_limit={key=[0; 0; <type=null>#]}}]>//tmp/t") == expected[8]
            assert read_table("<ranges=[{upper_limit={key=[0; 0; <type=max>#]}}]>//tmp/t") == expected[9]

            # 2 key columns
            assert read_table("<ranges=[{lower_limit={key=[0; <type=min>#]}}]>//tmp/t") == expected[10]
            assert read_table("<ranges=[{lower_limit={key=[0; <type=null>#]}}]>//tmp/t") == expected[11]
            assert read_table("<ranges=[{lower_limit={key=[0; <type=max>#]}}]>//tmp/t") == expected[12]

            assert read_table("<ranges=[{upper_limit={key=[0; <type=min>#]}}]>//tmp/t") == expected[13]
            assert read_table("<ranges=[{upper_limit={key=[0; <type=null>#]}}]>//tmp/t") == expected[14]
            assert read_table("<ranges=[{upper_limit={key=[0; <type=max>#]}}]>//tmp/t") == expected[15]

            # 1 key column
            assert read_table("<ranges=[{lower_limit={key=[<type=min>#]}}]>//tmp/t") == row
            assert read_table("<ranges=[{lower_limit={key=[<type=null>#]}}]>//tmp/t") == row
            assert read_table("<ranges=[{lower_limit={key=[<type=max>#]}}]>//tmp/t") == []

            assert read_table("<ranges=[{upper_limit={key=[<type=min>#]}}]>//tmp/t") == []
            assert read_table("<ranges=[{upper_limit={key=[<type=null>#]}}]>//tmp/t") == []
            assert read_table("<ranges=[{upper_limit={key=[<type=max>#]}}]>//tmp/t") == row

        _check(expected_before_action)

        shortened = key_columns_action == "shorten"
        alter_table("//tmp/t", schema=schema2 if shortened else schema3)

        _check(expected_after_shortening if shortened else expected_after_widening)

    @authors("max42")
    @pytest.mark.parametrize("sort_order", ["descending", "ascending"])
    def test_append_to_sorted_altered_table(self, sort_order):
        create("table", "//tmp/single_row")
        write_table("//tmp/single_row", [{"a": 1}])

        partial_schema = [{"name": "a", "type": "int64", "sort_order": sort_order}]
        full_schema = [{"name": "a", "type": "int64", "sort_order": sort_order},
                       {"name": "b", "type": "int64", "sort_order": sort_order}]

        create("table", "//tmp/t", attributes={"schema": partial_schema})
        write_table("//tmp/t", [{"a": 42}])
        alter_table("//tmp/t", schema=full_schema)

        def append_via_write_table(dst):
            write_table("<append=%true>" + dst, [{"a": 42, "b": 23}])

        def append_via_map(dst):
            map(in_="//tmp/single_row",
                out="<append=%true>" + dst,
                command="echo '{a=42;b=23}';")

        def append_via_concatenate(dst):
            create("table", "//tmp/delta", attributes={"schema": full_schema})
            write_table("//tmp/delta", [{"a": 42, "b": 23}])
            concatenate(source_paths=["//tmp/delta"],
                        destination_path="<append=%true>" + dst)

        def validate_or_expect_error(append_fn, suffix):
            dst = "//tmp/t_" + suffix
            copy("//tmp/t", dst)
            if sort_order == "ascending":
                append_fn(dst)
                assert read_table(dst) == [{"a": 42}, {"a": 42, "b": 23}]
            else:
                # NB: after table altering last key becomes [42, #], which is greater than
                # [42, 23] in descending sort order.
                with raises_yt_error(yt_error_codes.SortOrderViolation):
                    append_fn(dst)

        validate_or_expect_error(append_via_write_table, "write_table")
        validate_or_expect_error(append_via_map, "map")
        validate_or_expect_error(append_via_concatenate, "concatenate")

    @authors("babenko")
    def test_read_with_hedging(self):
        create("table", "//tmp/t")

        ROWS = list([{"key": i} for i in range(10)])
        write_table("//tmp/t", ROWS)

        chunk_id = get("//tmp/t/@chunk_ids/0")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_id))) == 3)

        for _ in range(10):
            rows = read_table(
                "//tmp/t",
                table_reader={
                    "meta_rpc_hedging_delay": 1,
                    "block_rpc_hedging_delay": 1,
                    "cancel_primary_block_rpc_request_on_hedging": True,
                },
            )
            assert rows == ROWS

    @authors("max42")
    def test_block_reordering(self):
        col_count = 100
        row_count = 100

        r = random.Random()

        def random_str():
            return hex(r.randint(0, 16 ** 50 - 1))[2:]

        schema = [{"name": "col{:2d}".format(i), "type": "string"} for i in range(col_count)]
        for reordering_config in (
            {"enable_block_reordering": False},
            {"enable_block_reordering": True},
            {"enable_block_reordering": True, "shuffle_blocks": True},
        ):
            for optimize_for in ("scan", "lookup"):
                for erasure_codec in ("lrc_12_2_2", "none"):
                    print_debug(
                        "Checking combination of reodering_config = {}, optimize_for = {}, "
                        "erasure_codec = {}".format(reordering_config, optimize_for, erasure_codec)
                    )
                    create(
                        "table",
                        "//tmp/t",
                        attributes={
                            "optimize_for": optimize_for,
                            "erasure_codec": erasure_codec,
                            "schema": schema,
                        },
                        verbose=False,
                    )
                    content = [{schema[i]["name"]: random_str() for i in range(col_count)} for j in range(row_count)]
                    write_table(
                        "//tmp/t",
                        content,
                        verbose=False,
                        table_writer=reordering_config,
                    )
                    assert read_table("//tmp/t", verbose=False) == content
                    remove("//tmp/t")

    @authors("gritukan")
    @pytest.mark.parametrize("horizontal", [False, True])
    def test_block_sampling(self, horizontal):
        schema = make_schema(
            [
                {"name": "x", "type": "string"},
                {"name": "y", "type": "int64"},
                {"name": "z", "type": "string"},
            ]
        )

        if horizontal:
            create(
                "table",
                "//tmp/t1",
                attributes={
                    "chunk_writer": {"block_size": 1024},
                    "optimize_for": "lookup",
                    "schema": schema,
                },
            )
            write_table("//tmp/t1", [{"x": "A" * 1024, "y": i} for i in range(100)])
            assert get("//tmp/t1/@chunk_count") == 1
            chunk_id = get_singular_chunk_id("//tmp/t1")
            assert get("#{}/@max_block_size".format(chunk_id)) < 2 * 1024
        else:
            create(
                "table",
                "//tmp/t1",
                attributes={
                    "chunk_writer": {"block_size": 1024},
                    "optimize_for": "scan",
                    "schema": schema,
                },
            )
            write_table(
                "<compression_codec=none>//tmp/t1",
                [{"x": "x", "y": i, "z": "z" * 100000} for i in range(100)],
            )
            assert get("//tmp/t1/@chunk_count") == 1

        def sample_rows(table, sampling_rate, lower_index=None, upper_index=None):
            if lower_index:
                table = "{}[#{}:#{}]".format(table, lower_index, upper_index)
            table_reader_options = {
                "sampling_mode": "block",
                "sampling_rate": sampling_rate,
            }
            control_attributes = {"enable_row_index": True}
            rows = read_table(
                table,
                table_reader=table_reader_options,
                control_attributes=control_attributes,
                verbose=False,
            )
            result = []
            row_index = -1
            for row in rows:
                if "row_index" in row.attributes:
                    row_index = row.attributes["row_index"]
                else:
                    assert row["y"] == row_index
                    if lower_index:
                        assert row_index >= lower_index
                    if upper_index:
                        assert row_index < upper_index
                    row_index += 1
                    result.append(row)
            return result

        for _ in range(10):
            assert len(sample_rows("//tmp/t1", 0)) == 0
            assert len(sample_rows("//tmp/t1", 0, 25, 75)) == 0
            assert 25 <= len(sample_rows("//tmp/t1", 0.5)) <= 75
            assert 8 <= len(sample_rows("//tmp/t1", 0.5, 25, 75)) <= 41
            assert len(sample_rows("//tmp/t1", 1)) == 100
            assert len(sample_rows("//tmp/t1", 1, 25, 75)) == 50

        if horizontal:
            create(
                "table",
                "//tmp/t2",
                attributes={
                    "chunk_writer": {"block_size": 10 ** 6},
                    "optimize_for": "lookup",
                },
            )
            write_table("//tmp/t2", [{"x": 1, "y": i} for i in range(100)])
            assert get("//tmp/t2/@chunk_count") == 1
        else:
            create(
                "table",
                "//tmp/t2",
                attributes={"optimize_for": "scan", "schema": schema},
            )
            write_table(
                "<compression_codec=none>//tmp/t2",
                [{"x": "x", "y": i, "z": "z"} for i in range(100)],
            )
            assert get("//tmp/t2/@chunk_count") == 1
            chunk_id = get_singular_chunk_id("//tmp/t2")

        for _ in range(10):
            assert len(sample_rows("//tmp/t2", 0)) == 0
            assert len(sample_rows("//tmp/t2", 0, 25, 75)) == 0
            assert len(sample_rows("//tmp/t2", 0.5)) in [0, 100]
            assert len(sample_rows("//tmp/t2", 0.5, 25, 75)) in [0, 50]
            assert len(sample_rows("//tmp/t2", 1)) == 100
            assert len(sample_rows("//tmp/t2", 1, 25, 75)) == 50

    @authors("akozhikhov")
    def test_write_read_static_table_with_hunks(self):
        create("table", "//tmp/t_in", attributes={"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]})

        create("table", "//tmp/t_out", attributes={"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "max_inline_hunk_size": 10},
        ]})

        rows = [{"key": 0, "value": "0"}, {"key": 1, "value": "1" * 20}]
        write_table("//tmp/t_in", rows)

        merge(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": "sorted",
            }
        )
        assert read_table("//tmp/t_in") == rows

        merge(
            in_="//tmp/t_out",
            out="//tmp/t_in",
            spec={
                "force_transform": True,
                "mode": "sorted",
            }
        )
        assert read_table("//tmp/t_in") == rows

    @authors("h0pless")
    def test_schemaful_node_type_handler(self):
        def create_table_on_specific_cell(path, schema):
            if self.is_multicell():
                create("table", path, attributes={"schema": schema, "external_cell_tag": 13})
            else:
                create("table", path, attributes={"schema": schema, "external": False})

        def branch_my_table():
            tx = start_transaction()
            lock("//tmp/table", mode="exclusive", tx=tx)
            assert new_schema_id == get("//tmp/table/@schema_id", tx=tx)
            return tx

        def alter_my_table(tx):
            alter_table("//tmp/table", schema=original_schema, tx=tx)
            assert original_schema_id == get("//tmp/table/@schema_id", tx=tx)

        original_schema = make_schema(
            [
                {"name": "headline", "type": "string"},
                {"name": "something_something_pineapple", "type": "string"},
                {"name": "coolness_factor", "type": "int64"},
            ]
        )
        create_table_on_specific_cell("//tmp/original_schema_holder", original_schema)
        original_schema_id = get("//tmp/original_schema_holder/@schema_id")

        new_schema = make_schema(
            [
                {"name": "weird_id", "type": "int64"},
                {"name": "something_something_pomegranate", "type": "string"},
                {"name": "normal_id", "type": "int64"},
            ]
        )
        create_table_on_specific_cell("//tmp/new_schema_holder", new_schema)
        new_schema_id = get("//tmp/new_schema_holder/@schema_id")

        create_table_on_specific_cell("//tmp/table", original_schema)

        # Test that alter still works
        alter_table("//tmp/table", schema=new_schema)
        assert new_schema_id == get("//tmp/table/@schema_id")

        # Check that nothing changes from branching
        tx = branch_my_table()
        commit_transaction(tx)
        assert new_schema_id == get("//tmp/table/@schema_id")

        # Check that nothing changes if transaction is aborted
        tx = branch_my_table()
        alter_my_table(tx)
        abort_transaction(tx)
        assert new_schema_id == get("//tmp/table/@schema_id")

        # Check that changes made inside a transaction actually apply
        tx = branch_my_table()
        alter_my_table(tx)
        commit_transaction(tx)
        assert original_schema_id == get("//tmp/table/@schema_id")

        # Cross shard copy if portals are enabled
        if self.ENABLE_TMP_PORTAL:
            create("portal_entrance", "//non_tmp", attributes={"exit_cell_tag": 12})
            copy("//tmp/table", "//non_tmp/table_copy")
            assert get("//tmp/table/@schema") == get("//non_tmp/table_copy/@schema")


##################################################################


class TestTablesChunkFormats(YTEnvSetup):
    NUM_MASTERS = 1

    @authors("babenko")
    def test_validate_chunk_format_on_create(self):
        with raises_yt_error("Error parsing EChunkFormat"):
            create("table", "//tmp/t", attributes={"chunk_format": "nonexisting_format"})
        with raises_yt_error(yt_error_codes.InvalidTableChunkFormat):
            create("table", "//tmp/t", attributes={"chunk_format": "file_default"})
        with raises_yt_error(yt_error_codes.InvalidTableChunkFormat):
            create("table", "//tmp/t", attributes={"optimize_for": "lookup", "chunk_format": "table_unversioned_columnar"})

    @authors("babenko")
    def test_deduce_optimize_for_from_chunk_format_on_create(self):
        create("map_node", "//tmp/m", attributes={"optimize_for": "lookup"})
        create("table", "//tmp/m/t", attributes={"chunk_format": "table_unversioned_columnar"})
        assert get("//tmp/m/t/@optimize_for") == "scan"
        assert get("//tmp/m/t/@chunk_format") == "table_unversioned_columnar"

    @authors("babenko")
    def test_set_and_remove_chunk_format_attr(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@optimize_for") == "lookup"
        assert not exists("//tmp/t/@chunk_format")
        # Sadly this is possible.
        set("//tmp/t/@chunk_format", "table_unversioned_columnar")
        assert get("//tmp/t/@optimize_for") == "lookup"
        assert get("//tmp/t/@chunk_format") == "table_unversioned_columnar"


##################################################################


def check_multicell_statistics(path, chunk_count_map):
    statistics = get(path + "/@multicell_statistics")
    assert len(statistics) == len(chunk_count_map)
    for cell_tag in statistics:
        assert statistics[cell_tag]["chunk_count"] == chunk_count_map[cell_tag]


class TestTablesMulticell(TestTables):
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("babenko")
    def test_concatenate_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", {"key": "x"})
        assert read_table("//tmp/t1") == [{"key": "x"}]

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        write_table("//tmp/t2", {"key": "y"})
        assert read_table("//tmp/t2") == [{"key": "y"}]

        create("table", "//tmp/union", attributes={"external": False})

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        check_multicell_statistics("//tmp/union", {"11": 1, "12": 1})

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}] * 2
        check_multicell_statistics("//tmp/union", {"11": 2, "12": 2})

    @authors("babenko")
    def test_concatenate_sorted_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", {"key": "x"})
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert get("//tmp/t1/@sorted", "true")

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        write_table("//tmp/t2", {"key": "y"})
        sort(in_="//tmp/t2", out="//tmp/t2", sort_by="key")
        assert read_table("//tmp/t2") == [{"key": "y"}]
        assert get("//tmp/t2/@sorted", "true")

        create("table", "//tmp/union", attributes={"external": False})
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t2", "//tmp/t1"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]
        assert get("//tmp/union/@sorted", "false")
        check_multicell_statistics("//tmp/union", {"11": 1, "12": 1})

    @authors("babenko")
    def test_concatenate_foreign_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        create("table", "//tmp/t3", attributes={"external_cell_tag": 13})

        write_table("//tmp/t1", {"key": "x"})
        concatenate(["//tmp/t1", "//tmp/t1"], "//tmp/t2")
        assert read_table("//tmp/t2") == [{"key": "x"}] * 2
        check_multicell_statistics("//tmp/t2", {"11": 2})

        concatenate(["//tmp/t2", "//tmp/t2"], "//tmp/t3")
        assert read_table("//tmp/t3") == [{"key": "x"}] * 4
        check_multicell_statistics("//tmp/t3", {"11": 4})

    @authors("ermolovd")
    def test_nan_values_operations(self):
        create("table", "//tmp/input")
        create("table", "//tmp/sorted")
        create("table", "//tmp/map_reduce")

        write_table(
            "//tmp/input",
            [
                {"foo": float(1), "bar": "a"},
                {"foo": float("nan"), "bar": "b"},
                {"foo": float("nan"), "bar": "d"},
                {"foo": float(2), "bar": "c"},
            ],
        )

        sort(in_="//tmp/input", out="//tmp/sorted", sort_by=["foo"])

        # check no errors
        map_reduce(
            in_="//tmp/input",
            out="//tmp/map_reduce",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["foo"],
        )

        rows = read_table("//tmp/sorted")
        for r in rows:
            # N.B. Python cannot compare NaN values correctly so we cast them to strings
            if math.isnan(r["foo"]):
                r["foo"] = "nan"

        assert rows == [
            {"foo": 1.0, "bar": "a"},
            {"foo": 2.0, "bar": "c"},
            {"foo": "nan", "bar": "b"},
            {"foo": "nan", "bar": "d"},
        ]

        create("table", "//tmp/composite_input", attributes={
            "schema": [
                {"name": "foo", "type_v3": list_type("double")},
                {"name": "bar", "type_v3": "string"},
            ]
        })
        create("table", "//tmp/composite_sorted")

        write_table(
            "//tmp/composite_input",
            [
                {"foo": [float(1)], "bar": "a"},
                {"foo": [float("nan")], "bar": "b"},
                {"foo": [float("nan"), float("nan")], "bar": "d"},
                {"foo": [float(2)], "bar": "c"},
            ],
        )
        sort(in_="//tmp/composite_input", out="//tmp/composite_sorted", sort_by=["foo"])

        rows = read_table("//tmp/composite_sorted")
        for r in rows:
            # N.B. Python cannot compare NaN values correctly so we cast them to strings
            for i in range(len(r["foo"])):
                if math.isnan(r["foo"][i]):
                    r["foo"][i] = "nan"

        assert rows == [
            {"foo": [1.0], "bar": "a"},
            {"foo": [2.0], "bar": "c"},
            {"foo": ["nan"], "bar": "b"},
            {"foo": ["nan", "nan"], "bar": "d"},
        ]

    @authors("ermolovd", "kiselyovp")
    def test_nan_values_sorted_write(self):
        create(
            "table",
            "//tmp/input",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "foo", "type": "double", "sort_order": "ascending"},
                        {"name": "bar", "type": "string"},
                    ]
                )
            },
        )

        write_table(
            "//tmp/input",
            [
                {"foo": 1.0, "bar": "a"},
                {"foo": 2.0, "bar": "b"},
            ],
        )

        write_table(
            "<append=%true>//tmp/input",
            [
                {"foo": 3.0, "bar": "c"},
                {"foo": 4.0, "bar": "d"},
            ],
        )

        write_table(
            "<append=%true>//tmp/input",
            [
                {"foo": float("nan"), "bar": "e"},
            ],
        )
        self._wait_until_unlocked("//tmp/input")

    @authors("gritukan")
    def test_unsupported_chunk_feature(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "chunk_writer": {
                    "testing_options": {
                        "add_unsupported_feature": True
                    },
                },
            },
        )
        write_table("//tmp/t", [{"foo": "bar"}])

        with raises_yt_error(yt_error_codes.UnsupportedChunkFeature):
            read_table("//tmp/t")

        create("table", "//tmp/t_out")
        with raises_yt_error(yt_error_codes.UnsupportedChunkFeature):
            remote_copy(
                in_="//tmp/t",
                out="//tmp/t_out",
                spec={"cluster_connection": self.__class__.Env.configs["driver"]},
            )

    @authors("shakurov")
    def test_cloned_table_statistics_yt_18290(self):
        if not self.ENABLE_TMP_PORTAL:
            create("map_node", "//portals", force=True)

        create("table", "//tmp/t")
        tx = start_transaction()
        write_table("<append=%true>//tmp/t", [{"foo": "bar"}], tx=tx)
        copy("//tmp/t", "//portals/t3", tx=tx)
        commit_transaction(tx)
        assert get("//portals/t3/@snapshot_statistics/chunk_count") == 1
        assert get("//portals/t3/@delta_statistics/chunk_count") == 0

    @authors("shakurov")
    def test_trunk_node_has_zero_delta_statistics_after_append(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"foo": "bar"}])
        assert get("//tmp/t/@snapshot_statistics/chunk_count") == 1
        assert get("//tmp/t/@delta_statistics/chunk_count") == 0
        write_table("<append=%true>//tmp/t", [{"foo": "bar"}])
        assert get("//tmp/t/@snapshot_statistics/chunk_count") == 2
        assert get("//tmp/t/@delta_statistics/chunk_count") == 0

##################################################################


class TestTablesPortal(TestTablesMulticell):
    ENABLE_TMP_PORTAL = True


class TestTablesShardedTx(TestTablesPortal):
    NUM_SECONDARY_MASTER_CELLS = 4
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["transaction_coordinator", "chunk_host"]},
        "14": {"roles": ["transaction_coordinator"]},
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            # COMPAT(shakurov): change the default to false and remove
            # this delta once masters are up to date.
            "enable_prerequisites_for_starting_completion_transactions": False,
        }
    }


class TestTablesShardedTxCTxS(TestTablesShardedTx):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


class TestTablesRpcProxy(TestTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    @authors("akozhikhov")
    @pytest.mark.parametrize("proxy_type", ["http", "rpc"])
    def test_path_in_error_attributes(self, proxy_type):
        try:
            if proxy_type == "http":
                client = yt.wrapper.YtClient(proxy=self.Env.get_proxy_address())
                client.get("//tmp/t/@id")
            else:
                get("//tmp/t/@id")
        except YtError as err:
            attrs = err.inner_errors[0]["attributes"]
            assert attrs.get("path", "") == "//tmp/t/@id"


##################################################################


class TestYPathTypeConversion(TestTables):
    @authors("gepardo")
    def test_type_conversion(self):
        create(
            "table",
            "//tmp/input",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "foo", "type": "double", "sort_order": "ascending"},
                        {"name": "bar", "type": "string"},
                    ]
                )
            },
        )
        write_table("//tmp/input", [
            {"foo": 41.0, "bar": "val0"},
            {"foo": 41.5, "bar": "val1"},
            {"foo": 42.0, "bar": "val2"},
            {"foo": 42.5, "bar": "val3"},
            {"foo": 43.0, "bar": "val4"}
        ])
        assert read_table("//tmp/input[42]") == [{"foo": 42.0, "bar": "val2"}]

        create(
            "table",
            "//tmp/input2",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "foo", "type": "uint64", "sort_order": "ascending"},
                        {"name": "bar", "type": "string"},
                    ]
                )
            },
        )
        write_table("//tmp/input2", [
            {"foo": 44, "bar": "val0"},
            {"foo": 45, "bar": "val1"},
            {"foo": 46, "bar": "val2"}
        ])
        assert read_table("//tmp/input2[45]") == [{"foo": 45, "bar": "val1"}]
