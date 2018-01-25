from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from yt.yson import to_yson_type, loads
from yt.environment.helpers import assert_items_equal

from time import sleep
import pytest

##################################################################

class TestTables(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_invalid_type(self):
        with pytest.raises(YtError): read_table("//tmp")
        with pytest.raises(YtError): write_table("//tmp", [])

    def test_simple(self):
        create("table", "//tmp/table")

        assert read_table("//tmp/table") == []
        assert get("//tmp/table/@row_count") == 0
        assert get("//tmp/table/@chunk_count") == 0
        assert get("//tmp/table/@uncompressed_data_size") == 0
        assert get("//tmp/table/@compressed_data_size") == 0
        assert get("//tmp/table/@data_weight") == 0

        write_table("//tmp/table", {"b": "hello"})
        assert read_table("//tmp/table") == [{"b":"hello"}]
        assert get("//tmp/table/@row_count") == 1
        assert get("//tmp/table/@chunk_count") == 1
        assert get("//tmp/table/@uncompressed_data_size") == 13
        assert get("//tmp/table/@compressed_data_size") == 39
        assert get("//tmp/table/@data_weight") == 6

        write_table("<append=true>//tmp/table", [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}])
        assert read_table("//tmp/table") == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get("//tmp/table/@row_count") == 3
        assert get("//tmp/table/@chunk_count") == 2
        assert get("//tmp/table/@uncompressed_data_size") == 46
        assert get("//tmp/table/@compressed_data_size") == 99
        assert get("//tmp/table/@data_weight") == 16

    def test_unavailable(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}])

        nodes = ls("//sys/nodes")
        for node in nodes:
            self.set_node_banned(node, True)

        with pytest.raises(YtError): read_table("//tmp/table")

        for node in nodes:
            self.set_node_banned(node, False)

    def test_sorted_write_table(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}], sorted_by="key")

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["key"]
        assert get("//tmp/table/@row_count") == 4

        # sorted flag is discarded when writing unsorted data to sorted table
        write_table("<append=true>//tmp/table", {"key": 4})
        assert not get("//tmp/table/@sorted")
        with pytest.raises(YtError): get("//tmp/table/@sorted_by")

    def test_append_sorted_simple(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 0, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0}], sorted_by=["a", "b"])
        write_table("<append=true>//tmp/table", [{"a": 1, "b": 0}, {"a": 2, "b": 0}], sorted_by=["a", "b"])

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["a", "b"]
        assert get("//tmp/table/@row_count") == 5

    def test_append_sorted_with_less_key_columns(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 0, "b": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0}], sorted_by=["a", "b"])
        write_table("<append=true>//tmp/table", [{"a": 1, "b": 1}, {"a": 2, "b": 0}], sorted_by=["a"])

        assert get("//tmp/table/@sorted")
        assert get("//tmp/table/@sorted_by") == ["a"]
        assert get("//tmp/table/@row_count") == 5

    def test_append_sorted_order_violated(self):
        create("table", "//tmp/table");
        write_table("//tmp/table", [{"a": 1}, {"a": 2}], sorted_by=["a"])
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"a": 0}], sorted_by=["a"])

    def test_append_sorted_to_unsorted(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 2}, {"a": 1}, {"a": 0}])
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"a": 2}, {"a": 3}], sorted_by=["a"])

    def test_append_sorted_with_more_key_columns(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}], sorted_by=["a"])
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"a": 2, "b": 1}, {"a": 3, "b": 0}], sorted_by=["a", "b"])

    def test_append_sorted_with_different_key_columns(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}], sorted_by=["a"])
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"b": 0}, {"b": 1}], sorted_by=["b"])

    def test_append_sorted_concurrently(self):
        create("table", "//tmp/table")
        tx1 = start_transaction()
        tx2 = start_transaction()
        write_table("<append=true>//tmp/table", [{"a": 0}, {"a": 1}], sorted_by=["a"], tx=tx1)
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"a": 1}, {"a": 2}], sorted_by=["a"], tx=tx2)

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

    def test_invalid_cases(self):
        create("table", "//tmp/table")

        # we can write only list fragments
        with pytest.raises(YtError): write_table("<append=true>//tmp/table", yson.loads("string"))
        with pytest.raises(YtError): write_table("<append=true>//tmp/table", yson.loads("100"))
        with pytest.raises(YtError): write_table("<append=true>//tmp/table", yson.loads("3.14"))

        # check max_row_weight limit
        with pytest.raises(YtError):
            write_table("//tmp/table", {"a" : "long_string"}, table_writer = {"max_row_weight" : 2})

        # check max_key_weight limit
        with pytest.raises(YtError):
            write_table("//tmp/table", {"a" : "long_string"}, sorted_by=["a"], table_writer = {"max_key_weight" : 2})

        # check duplicate ids
        with pytest.raises(YtError):
            write_table("//tmp/table", "{a=version1; a=version2}", is_raw=True)

        content = "some_data"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        with pytest.raises(YtError): read_table("//tmp/file")

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schemaful_write(self, optimize_for):
        create("table", "//tmp/table",
            attributes={"optimize_for" : optimize_for})

        assert get("//tmp/table/@optimize_for") == optimize_for
        assert get("//tmp/table/@schema_mode") == "weak"

        with pytest.raises(YtError):
            # append and schema are not compatible
            write_table("<append=true; schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table", [{"key": 1}])

        with pytest.raises(YtError):
            # sorted_by and schema are not compatible
            write_table("<sorted_by=[a]; schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table", [{"key": 2}])

        with pytest.raises(YtError):
            # invalid schema - duplicate columns
            write_table("<schema=[{name=key; type=int64};{name=key; type=string}]>//tmp/table", [])

        write_table("<schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
            [{"key": 0}, {"key": 1}])

        assert get("//tmp/table/@schema_mode") == "strong"
        assert read_table("//tmp/table") == [{"key": i} for i in xrange(2)]

        # schemas are equal so this must work; data is overwritten
        write_table("<schema=[{name=key; type=int64; sort_order=ascending}]>//tmp/table",
            [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}])
        assert read_table("//tmp/table") == [{"key": i} for i in xrange(4)]

        write_table("<append=true>//tmp/table", [{"key": 4}, {"key": 5}])
        assert get("//tmp/table/@schema_mode") == "strong"
        assert read_table("//tmp/table") == [{"key": i} for i in xrange(6)]

        # data is overwritten, schema is reset
        write_table("<schema=[{name=key; type=any}]>//tmp/table",
            [{"key": 4}, {"key": 5}])
        assert get("//tmp/table/@row_count") == 2

    def test_write_with_optimize_for(self):
        create("table", "//tmp/table")

        write_table("<optimize_for=lookup>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@optimize_for") == "lookup"

        write_table("<optimize_for=scan>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@optimize_for") == "scan"

        with pytest.raises(YtError):
            write_table("<append=true;optimize_for=scan>//tmp/table", [{"key": 0}])

    def test_write_with_compression(self):
        create("table", "//tmp/table")

        write_table("<compression_codec=none>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@compression_codec") == "none"

        write_table("<compression_codec=lz4>//tmp/table", [{"key": 0}])
        assert get("//tmp/table/@compression_codec") == "lz4"

        with pytest.raises(YtError):
            write_table("<append=true;compression_codec=lz4>//tmp/table", [{"key": 0}])

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_sorted_unique(self, optimize_for):
        create("table", "//tmp/table",
            attributes={
                "optimize_for" : optimize_for,
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=True)
            })

        assert get("//tmp/table/@schema_mode") == "strong"

        write_table("//tmp/table", [{"key": 0}, {"key": 1}])

        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"key": 1}])

        with pytest.raises(YtError):
            write_table("<append=true>//tmp/table", [{"key": 2}, {"key": 2}])

        write_table("<append=true>//tmp/table", [{"key": 2}])

        assert get("//tmp/table/@schema_mode") == "strong"
        assert get("//tmp/table/@schema/@unique_keys")
        assert read_table("//tmp/table") == [{"key": i} for i in xrange(3)]

    def test_computed_columns(self):
        create("table", "//tmp/table",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2", "sort_order": "ascending"},
                    {"name": "k2", "type": "int64"}]
            })

        assert get("//tmp/table/@schema_mode") == "strong"

        with pytest.raises(YtError):
            write_table("//tmp/table", [{"k2": 1}, {"k2": 0}])

        write_table("//tmp/table", [{"k2": 0}, {"k2": 1}])
        assert read_table("//tmp/table") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

    def test_row_index_selector(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 0}, {"b": 1}, {"c": 2}, {"d": 3}])

        # closed ranges
        assert read_table("//tmp/table[#0:#2]") == [{"a": 0}, {"b" : 1}] # simple
        assert read_table("//tmp/table[#-1:#1]") == [{"a": 0}] # left < min
        assert read_table("//tmp/table[#2:#5]") == [{"c": 2}, {"d": 3}] # right > max
        assert read_table("//tmp/table[#-10:#-5]") == [] # negative indexes

        assert read_table("//tmp/table[#1:#1]") == [] # left = right
        assert read_table("//tmp/table[#3:#1]") == [] # left > right

        # open ranges
        assert read_table("//tmp/table[:]") == [{"a": 0}, {"b" : 1}, {"c" : 2}, {"d" : 3}]
        assert read_table("//tmp/table[:#3]") == [{"a": 0}, {"b" : 1}, {"c" : 2}]
        assert read_table("//tmp/table[#2:]") == [{"c" : 2}, {"d" : 3}]

        # multiple ranges
        assert read_table("//tmp/table[:,:]") == [{"a": 0}, {"b" : 1}, {"c" : 2}, {"d" : 3}] * 2
        assert read_table("//tmp/table[#1:#2,#3:#4]") == [{"b": 1}, {"d": 3}]
        assert read_table("//tmp/table[#0]") == [{"a": 0}]
        assert read_table("//tmp/table[#1]") == [{"b": 1}]

        # reading key selectors from unsorted table
        with pytest.raises(YtError): read_table("//tmp/table[:a]")

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
        assert read_table("<lower_limit={chunk_index=2}>//tmp/table") == [{"c": 2}, {"d" : 3}, {"e" : 4}, {"f" : 5}, {"g" : 6}, {"h" : 7}]
        assert read_table("<lower_limit={chunk_index=1};upper_limit={chunk_index=2}>//tmp/table") == [{"b": 1}]
        assert read_table("<ranges=[{exact={chunk_index=1}}]>//tmp/table") == [{"b": 1}]

        rows = read_table("//tmp/table", unordered=True)
        d = dict()
        for r in rows:
            d.update(r)

        assert d == {"a" : 0, "b" : 1, "c" : 2, "d" : 3, "e" : 4, "f" : 5, "g" : 6, "h" : 7}

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_key_selector(self, optimize_for):
        create("table", "//tmp/table",  attributes={"optimize_for" : optimize_for})

        v1 = {"s" : "a", "i": 0,    "d" : 15.5}
        v2 = {"s" : "a", "i": 10,   "d" : 15.2}
        v3 = {"s" : "b", "i": 5,    "d" : 20.}
        v4 = {"s" : "b", "i": 20,   "d" : 20.}
        v5 = {"s" : "c", "i": -100, "d" : 10.}

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
        assert read_table('//tmp/table{s, d}[aa: (b, 10)]') == [{"s" : "b", "d" : 20.}]

        # limits of different types
        assert read_table("//tmp/table[#0:c]") == [v1, v2, v3, v4]

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_key_selector_types(self, optimize_for):
        create("table", "//tmp/table",  attributes={"optimize_for" : optimize_for})

        v1 = {"s" : None, "i": 0}
        v2 = {"s" : None, "i": 1}
        v3 = {"s" : 1, "i": 2}
        v4 = {"s" : 2, "i": 3}
        v5 = {"s" : yson.YsonUint64(3), "i": 4}
        v6 = {"s" : yson.YsonUint64(4), "i": 5}
        v7 = {"s" : 2.0, "i": 6}
        v8 = {"s" : 4.0, "i": 7}
        v9 = {"s" : False, "i": 8}
        v10 = {"s" : True, "i": 9}
        v11 = {"s" : "", "i": 10}
        v12 = {"s" : "b", "i": 11}

        values = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12]
        write_table("//tmp/table", values, sorted_by=["s", "i"])

        assert read_table("//tmp/table[(#,0):(#,100)]") == [v1, v2]
        assert read_table("//tmp/table[(1,0):(2,100)]") == [v3, v4]
        assert read_table("//tmp/table[(3u,0):(4u,100)]") == [v5, v6]
        assert read_table("//tmp/table[(2.0,0):(4.0,100)]") == [v7, v8]
        assert read_table("//tmp/table[(%false,0):(%true,100)]") == [v9, v10]
        assert read_table("//tmp/table[(\"\",0):(\"b\",100)]") == [v11, v12]

    def test_column_selector(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1, "aa": 2, "b": 3, "bb": 4, "c": 5})
        # empty columns
        assert read_table("//tmp/table{}") == [{}]

        # single columms
        assert read_table("//tmp/table{a}") == [{"a" : 1}]
        assert read_table("//tmp/table{a, }") == [{"a" : 1}] # extra comma
        assert read_table("//tmp/table{a, a}") == [{"a" : 1}]
        assert read_table("//tmp/table{c, b}") == [{"b" : 3, "c" : 5}]
        assert read_table("//tmp/table{zzzzz}") == [{}] # non existent column

        assert read_table("//tmp/table{a}") == [{"a" : 1}]
        assert read_table("//tmp/table{a, }") == [{"a" : 1}] # extra comma
        assert read_table("//tmp/table{a, a}") == [{"a" : 1}]
        assert read_table("//tmp/table{c, b}") == [{"b" : 3, "c" : 5}]
        assert read_table("//tmp/table{zzzzz}") == [{}] # non existent column

    def test_range_and_row_index(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}])

        v1 = to_yson_type(None, attributes={"range_index": 0})
        v2 = to_yson_type(None, attributes={"row_index": 0})
        v3 = {"a": 0}
        v4 = {"a": 1}
        v5 = {"a": 2}
        v6 = to_yson_type(None, attributes={"range_index": 1})
        v7 = to_yson_type(None, attributes={"row_index": 2})
        v8 = {"a": 2}
        v9 = {"a": 3}

        control_attributes = {"enable_range_index": True, "enable_row_index": True}
        result = read_table("//tmp/table[#0:#3, #2:#4]", control_attributes=control_attributes)
        assert result == [v1, v2, v3, v4, v5, v6, v7, v8, v9]

        # Test row_index without range index.
        control_attributes = {"enable_row_index": True}
        result = read_table("//tmp/table[#0:#3, #2:#4]", control_attributes=control_attributes)
        assert result == [v2, v3, v4, v5, v7, v8, v9]

    def test_range_and_row_index2(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}], sorted_by="a")

        v1 = to_yson_type(None, attributes={"range_index": 0})
        v2 = to_yson_type(None, attributes={"row_index": 2})
        v3 = {"a": 2}
        v4 = {"a": 3}
        v5 = {"a": 4}

        control_attributes = {"enable_range_index": True, "enable_row_index": True}
        result = read_table("//tmp/table[2:5]", control_attributes=control_attributes)
        assert result == [v1, v2, v3, v4, v5]

    def test_row_key_selector_yt_4840(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        write_table("<append=true>//tmp/table", [{"a": 0}, {"a": 1}, {"a": 2}, {"a": 3}], sorted_by="a", tx=tx)
        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        write_table("<append=true>//tmp/table", [{"a": 3}, {"a": 4}, {"a": 5}], sorted_by="a", tx=tx)
        write_table("<append=true>//tmp/table", [], sorted_by="a", tx=tx)
        assert read_table("//tmp/table[1:5]", tx=tx) == [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 3}, {"a": 4}]

    def test_shared_locks_two_chunks(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", {"a": 1}, tx=tx)
        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx)

        assert read_table("//tmp/table") == []
        assert read_table("//tmp/table", tx=tx) == [{"a":1}, {"b":2}]

        commit_transaction(tx)
        assert read_table("//tmp/table") == [{"a":1}, {"b":2}]

    def test_shared_locks_three_chunks(self):
        create("table", "//tmp/table")
        tx = start_transaction()

        write_table("<append=true>//tmp/table", {"a": 1}, tx=tx)
        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx)
        write_table("<append=true>//tmp/table", {"c": 3}, tx=tx)

        assert read_table("//tmp/table") == []
        assert read_table("//tmp/table", tx=tx) == [{"a":1}, {"b":2}, {"c" : 3}]

        commit_transaction(tx)
        assert read_table("//tmp/table") == [{"a":1}, {"b":2}, {"c" : 3}]

    def test_shared_locks_parallel_tx(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1})

        tx1 = start_transaction()
        tx2 = start_transaction()

        write_table("<append=true>//tmp/table", {"b": 2}, tx=tx1)

        write_table("<append=true>//tmp/table", {"c": 3}, tx=tx2)
        write_table("<append=true>//tmp/table", {"d": 4}, tx=tx2)

        # check which records are seen from different transactions
        assert read_table("//tmp/table") == [{"a" : 1}]
        assert read_table("//tmp/table", tx = tx1) == [{"a" : 1}, {"b": 2}]
        assert read_table("//tmp/table", tx = tx2) == [{"a" : 1}, {"c": 3}, {"d" : 4}]

        commit_transaction(tx2)
        assert read_table("//tmp/table") == [{"a" : 1}, {"c": 3}, {"d" : 4}]
        assert read_table("//tmp/table", tx = tx1) == [{"a" : 1}, {"b": 2}]

        # now all records are in table in specific order
        commit_transaction(tx1)
        assert read_table("//tmp/table") == [{"a" : 1}, {"c": 3}, {"d" : 4}, {"b" : 2}]

    def test_set_schema_in_tx(self):
        create("table", "//tmp/table")

        tx1 = start_transaction()
        tx2 = start_transaction()

        schema = get("//tmp/table/@schema")
        schema1 = make_schema(
            [{"name": "a", "type": "string"}],
            strict=False,
            unique_keys=False)
        schema2 = make_schema(
            [{"name": "b", "type": "string"}],
            strict=False,
            unique_keys=False)

        alter_table("//tmp/table", schema=schema1, tx = tx1)

        with pytest.raises(YtError):
            alter_table("//tmp/table", schema=schema2, tx = tx2)

        assert get("//tmp/table/@schema") == schema
        assert get("//tmp/table/@schema", tx = tx1) == schema1
        assert get("//tmp/table/@schema", tx = tx2) == schema

        commit_transaction(tx1)
        abort_transaction(tx2)
        assert get("//tmp/table/@schema") == schema1

    def test_shared_locks_nested_tx(self):
        create("table", "//tmp/table")

        v1 = {"k" : 1}
        v2 = {"k" : 2}
        v3 = {"k" : 3}
        v4 = {"k" : 4}

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

    def test_codec_in_writer(self):
        create("table", "//tmp/table")
        set("//tmp/table/@compression_codec", "zlib_9")
        write_table("//tmp/table", {"b": "hello"})

        assert read_table("//tmp/table") == [{"b":"hello"}]

        chunk_id = get("//tmp/table/@chunk_ids/0")
        assert get("#%s/@compression_codec" % chunk_id) == "zlib_9"

    def test_copy(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t"]

        assert read_table("//tmp/t") == [{"a" : "b"}]

        copy("//tmp/t", "//tmp/t2")
        assert sorted(get("#%s/@owning_nodes" % chunk_id)) == sorted(["//tmp/t", "//tmp/t2"])
        assert read_table("//tmp/t2") == [{"a" : "b"}]

        assert get("//tmp/t2/@resource_usage") == get("//tmp/t/@resource_usage")
        assert get("//tmp/t2/@replication_factor") == get("//tmp/t/@replication_factor")

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a" : "b"}]
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t2"]

        remove("//tmp/t2")

        gc_collect()
        multicell_sleep()
        assert not exists("#%s" % chunk_id)

    def test_copy_to_the_same_table(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        with pytest.raises(YtError): copy("//tmp/t", "//tmp/t")

    def test_copy_tx(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t"]

        tx = start_transaction()
        assert read_table("//tmp/t", tx=tx) == [{"a" : "b"}]
        t2_id = copy("//tmp/t", "//tmp/t2", tx=tx)
        print t2_id
        assert sorted(get("#%s/@owning_nodes" % chunk_id)) == sorted(["#%s" % t2_id, "//tmp/t", to_yson_type("//tmp/t2", attributes = {"transaction_id" : tx})])
        assert read_table("//tmp/t2", tx=tx) == [{"a" : "b"}]

        commit_transaction(tx)

        assert read_table("//tmp/t2") == [{"a" : "b"}]

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a" : "b"}]
        assert get("#%s/@owning_nodes" % chunk_id) == ["//tmp/t2"]

        remove("//tmp/t2")

        gc_collect()
        multicell_sleep()
        assert not exists("#%s" % chunk_id)

    def test_copy_not_sorted(self):
        create("table", "//tmp/t1")
        assert not get("//tmp/t1/@sorted")
        assert get("//tmp/t1/@key_columns") == []

        copy("//tmp/t1", "//tmp/t2")
        assert not get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@key_columns") == []

    def test_copy_sorted(self):
        create("table", "//tmp/t1")
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert get("//tmp/t1/@sorted")
        assert get("//tmp/t1/@key_columns") == ["key"]

        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@key_columns") == ["key"]

    def test_remove_create_under_transaction(self):
        create("table", "//tmp/table_xxx")
        tx = start_transaction()

        remove("//tmp/table_xxx", tx=tx)
        create("table", "//tmp/table_xxx", tx=tx)

    def test_transaction_staff(self):
        create("table", "//tmp/table_xxx")

        tx = start_transaction()
        remove("//tmp/table_xxx", tx=tx)
        inner_tx = start_transaction(tx=tx)
        get("//tmp", tx=inner_tx)

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

    def test_replication_factor_updates(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@replication_factor") == 3

        with pytest.raises(YtError): remove("//tmp/t/@replication_factor")
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 0)
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", {})

        tx = start_transaction()
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 2, tx=tx)

    def test_replication_factor_propagates_to_chunks(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 2)

        write_table("//tmp/t", {"foo" : "bar"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1

        chunk_id = chunk_ids[0]
        assert get_chunk_replication_factor(chunk_id) == 2

    def test_replication_factor_recalculated_on_remove(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", {"foo" : "bar"})

        chunk_ids = get("//tmp/t1/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert get_chunk_replication_factor(chunk_id) == 1

        copy("//tmp/t1", "//tmp/t2")
        set("//tmp/t2/@replication_factor", 2)

        sleep(0.2)
        assert get_chunk_replication_factor(chunk_id) == 2

        remove("//tmp/t2")

        sleep(0.2)
        assert get_chunk_replication_factor(chunk_id) == 1

    def test_recursive_resource_usage(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})
        copy("//tmp/t1", "//tmp/t2")

        assert get_chunk_owner_disk_space("//tmp/t1") + \
            get_chunk_owner_disk_space("//tmp/t2") == \
            get_recursive_disk_space("//tmp")

    def test_chunk_tree_balancer(self):
        create("table", "//tmp/t")
        for i in xrange(0, 40):
            write_table("<append=true>//tmp/t", {"a" : "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        statistics = get("#" + chunk_list_id + "/@statistics")
        assert statistics["chunk_count"] == 40
        assert statistics["chunk_list_count"] == 2
        assert statistics["row_count"] == 40
        assert statistics["rank"] == 2

    @pytest.mark.skipif("True") # very long test
    def test_chunk_tree_balancer_deep(self):
        create("table", "//tmp/t")
        tx_stack = list()
        tx = start_transaction()
        tx_stack.append(tx)

        for i in xrange(0, 1000):
            write_table("<append=true>//tmp/t", {"a" : i}, tx=tx)

        chunk_list_id = get("//tmp/t/@chunk_list_id", tx=tx)
        statistics = get("#" + chunk_list_id + "/@statistics", tx=tx)
        assert statistics["chunk_count"] == 1000
        assert statistics["chunk_list_count"] == 2001
        assert statistics["row_count"] == 1000
        assert statistics["rank"] == 1001

        tbl_a = read_table("//tmp/t", tx=tx)

        commit_transaction(tx)
        sleep(1.0)

        chunk_list_id = get("//tmp/t/@chunk_list_id")
        statistics = get("#" + chunk_list_id + "/@statistics")
        assert statistics["chunk_count"] == 1000
        assert statistics["chunk_list_count"] == 2
        assert statistics["row_count"] == 1000
        assert statistics["rank"] == 2

        assert tbl_a == read_table("//tmp/t")

    def _check_replication_factor(self, path, expected_rf):
        chunk_ids = get(path + "/@chunk_ids")
        for id in chunk_ids:
            assert get_chunk_replication_factor(id) == expected_rf

    # In tests below we intentionally issue vital/replication_factor updates
    # using a temporary user "u"; cf. YT-3579.
    def test_vital_update(self):
        create("table", "//tmp/t")
        create_user("u")
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"})

        def check_vital_chunks(is_vital):
            chunk_ids = get("//tmp/t/@chunk_ids")
            for id in chunk_ids:
                assert get("#" + id + "/@vital") == is_vital

        assert get("//tmp/t/@vital")
        check_vital_chunks(True)

        set("//tmp/t/@vital", False, authenticated_user="u")
        assert not get("//tmp/t/@vital")
        sleep(2)

        check_vital_chunks(False)

    def test_replication_factor_update1(self):
        create("table", "//tmp/t")
        create_user("u")
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"})
        set("//tmp/t/@replication_factor", 4, authenticated_user="u")
        sleep(2)
        self._check_replication_factor("//tmp/t", 4)

    def test_replication_factor_update2(self):
        create("table", "//tmp/t")
        create_user("u")
        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 4, authenticated_user="u")
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor("//tmp/t", 4)

    def test_replication_factor_update3(self):
        create("table", "//tmp/t")
        create_user("u")
        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 2, authenticated_user="u")
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor("//tmp/t", 2)

    def test_key_columns1(self):
        create("table", "//tmp/t",
                attributes = {
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "ascending"}]
                })
        assert get("//tmp/t/@sorted")
        assert get("//tmp/t/@key_columns") == ["a", "b"]

    def test_statistics1(self):
        table = "//tmp/t"
        create("table", table)
        set("//tmp/t/@compression_codec", "snappy")
        write_table(table, {"foo": "bar"})

        for i in xrange(8):
            merge(in_=[table, table], out="<append=true>" + table)

        chunk_count = 3**8
        assert len(get("//tmp/t/@chunk_ids")) == chunk_count

        codec_info = get("//tmp/t/@compression_statistics")
        assert codec_info["snappy"]["chunk_count"] == chunk_count

        erasure_info = get("//tmp/t/@erasure_statistics")
        assert erasure_info["none"]["chunk_count"] == chunk_count

    @unix_only
    def test_statistics2(self):
        tableA = "//tmp/a"
        create("table", tableA)
        write_table(tableA, {"foo": "bar"})

        tableB = "//tmp/b"
        create("table", tableB)
        set(tableB + "/@compression_codec", "snappy")

        map(in_=[tableA], out=[tableB], command="cat")

        codec_info = get(tableB + "/@compression_statistics")
        assert codec_info.keys() == ["snappy"]

    def test_optimize_for_statistics(self):
        table = "//tmp/a"
        create("table", table)
        write_table(table, {"foo": "bar"})

        optimize_for_info = get(table + "/@optimize_for_statistics")
        assert "lookup" in optimize_for_info and optimize_for_info["lookup"]["chunk_count"] == 1

        set(table + "/@optimize_for", "scan")

        for i in xrange(2):
            merge(in_=[table, table], out="<append=true>" + table, spec={"force_transform": True})

        assert len(get("//tmp/a/@chunk_ids")) == 3

        optimize_for_info = get(table + "/@optimize_for_statistics")
        for key in ("scan", "lookup"):
            assert key in optimize_for_info
        assert optimize_for_info["lookup"]["chunk_count"] == 1
        assert optimize_for_info["scan"]["chunk_count"] == 2

    def test_json_format(self):
        create("table", "//tmp/t")
        write_table('//tmp/t', '{"x":"0"}\n{"x":"1"}', input_format="json", is_raw=True)
        assert '{"x":"0"}\n{"x":"1"}\n' == read_table("//tmp/t", output_format="json")

    def test_yson_skip_nulls(self):
        create("table", "//tmp/t")
        write_table('//tmp/t', [{"x" : 0, "y" : None}, {"x" : None, "y" : 1}])
        format = yson.loads("<skip_null_values=%true; format=text>yson")
        assert '{"x"=0;};\n{"y"=1;};\n' == read_table("//tmp/t", output_format=format)
        del format.attributes["skip_null_values"]
        assert '{"y"=#;"x"=0;};\n{"y"=1;"x"=#;};\n' == read_table("//tmp/t", output_format=format)

    def test_boolean(self):
        create("table", "//tmp/t")
        format = yson.loads("<boolean_as_string=false;format=text>yson")
        write_table("//tmp/t", "{x=%false};{x=%true};{x=false};", input_format=format, is_raw=True)
        assert '{"x"=%false;};\n{"x"=%true;};\n{"x"="false";};\n' == read_table("//tmp/t", output_format=format)

    def test_uint64(self):
        create("table", "//tmp/t")
        format = yson.loads("<format=text>yson")
        write_table("//tmp/t", "{x=1u};{x=4u};{x=9u};", input_format=format, is_raw=True)
        assert '{"x"=1u;};\n{"x"=4u;};\n{"x"=9u;};\n' == read_table("//tmp/t", output_format=format)

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

    def test_extracting_table_columns_in_schemaful_dsv_from_complex_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [
            {"column1": {"childA" : "some_value", "childB" : "42"},
            "column2" : "value12",
            "column3" : "value13"},
            {"column1": {"childA" : "some_other_value", "childB" : "321"},
            "column2" : "value22",
            "column3" : "value23"}])

        tabular_data = read_table("//tmp/t1", output_format=yson.loads("<columns=[column2;column3]>schemaful_dsv"))
        assert tabular_data == "value12\tvalue13\nvalue22\tvalue23\n"

    def test_dynamic_table_schema_required(self):
        with pytest.raises(YtError): create("table", "//tmp/t",
            attributes={"dynamic": True})

    def test_schema_validation(self):
        def init_table(path, schema):
            remove(path, force=True)
            create("table", path, attributes={"schema": schema})

        def test_positive(schema, rows):
            init_table("//tmp/t", schema)
            write_table("<append=%true>//tmp/t", rows)
            assert read_table("//tmp/t") == rows
            assert get("//tmp/t/@schema_mode") == "strong"
            assert get("//tmp/t/@schema") == schema

        def test_negative(schema, rows):
            init_table("//tmp/t", schema)
            with pytest.raises(YtError):
                write_table("<append=%true>//tmp/t", rows)
            # XXX(babenko): workaround for YT-5604
            sleep(1.0)

        schema = make_schema([
            {"name": "key", "type": "int64"}],
            strict=False,
            unique_keys=False)
        test_positive(schema, [{"key": 1}])
        test_negative(schema, [{"key": False}])

        schema = make_schema([
            {"name": "key", "type": "int64"}],
            strict=True,
            unique_keys=False)
        test_negative(schema, [{"values": 1}])

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]

        schema = make_schema([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}],
            strict=False,
            unique_keys=False)
        test_positive(schema, rows)
        test_negative(schema, list(reversed(rows)))

        schema = make_schema([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}],
            strict=False,
            unique_keys=True)
        test_positive(schema, rows)

        rows = [{"key": 1, "value": str(i)} for i in xrange(10)]
        test_negative(schema, rows);

    def test_type_conversion(self):
        create("table", "//tmp/t",
            attributes={
                "schema": make_schema([
                    {"name": "int64", "type": "int64", "sort_order": "ascending"},
                    {"name": "uint64", "type": "uint64"},
                    {"name": "boolean", "type": "boolean"},
                    {"name": "double", "type": "double"},
                    {"name": "any", "type": "any"}],
                    strict=False)
            })

        row = '{int64=3u; uint64=42; boolean="false"; double=18; any={}; extra=qwe}'

        yson_without_type_conversion = loads("yson")
        yson_with_type_conversion = loads("<enable_type_conversion=%true>yson")

        with pytest.raises(YtError):
            write_table("//tmp/t", row, is_raw=True, input_format=yson_without_type_conversion)
        write_table("//tmp/t", row, is_raw=True, input_format=yson_with_type_conversion)

    def test_writer_config(self):
        create("table", "//tmp/t",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none"
            })

        write_table("//tmp/t", [{"value": "A"*1024} for i in xrange(10)])
        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 1
        assert get("#" + chunks[0] + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunks[0] + "/@max_block_size") < 1024 * 2

    def test_read_blob_table(self):
        create("table", "//tmp/ttt")
        write_table("<sorted_by=[key]>//tmp/ttt", [
            {"key": "x", "part_index": 0, "data": "hello "},
            {"key": "x", "part_index": 1, "data": "world!"},
            {"key": "y", "part_index": 0, "data": "AAA"},
            {"key": "z", "part_index": 0},
            {"key": "za", "part_index": 0, "data": "abacaba"},
            {"key": "za", "part_index": 1, "data": "abac"},
            {"key": "zb", "part_index": 0, "data": "abacaba"},
            {"key": "zb", "part_index": 1, "data": "abacabacab"},
            {"key": "zc", "part_index": 0, "data": "abac"},
            {"key": "zc", "part_index": 1, "data": "abacaba"}])

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

        assert "hello " == read_blob_table("//tmp/ttt[:#1]", part_size=6)
        assert "world!" == read_blob_table("//tmp/ttt[#1:#2]", part_size=6, start_part_index=1)
        assert "rld!" == read_blob_table("//tmp/ttt[#1:#2]", part_size=6, start_part_index=1, offset=2)
        assert "hello world!" == read_blob_table("//tmp/ttt[:#2]", part_size=6)
        assert "AAA" == read_blob_table("//tmp/ttt[#2]", part_size=3)

        assert "hello world!" == read_blob_table("//tmp/ttt[x]", part_size=6)
        assert "AAA" == read_blob_table("//tmp/ttt[y]", part_size=3)
        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[x:z]", part_size=3)

        assert "abacabaabac" == read_blob_table("//tmp/ttt[za]", part_size=7)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[zb]", part_size=7)

        with pytest.raises(YtError):
            read_blob_table("//tmp/ttt[zc]", part_size=7)

        write_table("//tmp/ttt", [
            {"key": "x", "index": 0, "value": "hello "},
            {"key": "x", "index": 1, "value": "world!"}])
        assert "hello world!" == read_blob_table("//tmp/ttt", part_index_column_name="index",
                                                 data_column_name="value", part_size=6)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_table_chunk_format_statistics(self, optimize_for):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        write_table("//tmp/t", [{"a": "b"}])
        chunk_format = "schemaless_horizontal" if optimize_for == "lookup" else "unversioned_columnar"
        assert get("//tmp/t/@table_chunk_format_statistics/{0}/chunk_count".format(chunk_format)) == 1
        chunk = get("//tmp/t/@chunk_ids")[0]
        assert get("#{0}/@table_chunk_format".format(chunk)) == chunk_format

    def test_get_start_row_index(self):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
            })
        write_table("//tmp/t", [{"key": 1, "value": "a"}, {"key": 1, "value": "b"}])
        write_table("<append=%true>//tmp/t", [{"key": 1, "value": "c"}, {"key": 2, "value": "a"}])
        response_parameters={}
        read_table("//tmp/t[(2)]", start_row_index_only=True, response_parameters=response_parameters)
        assert response_parameters["start_row_index"] == 3

##################################################################

def check_multicell_statistics(path, chunk_count_map):
    statistics = get(path + '/@multicell_statistics')
    assert len(statistics) == len(chunk_count_map)
    for cell_tag in statistics:
        assert statistics[cell_tag]["chunk_count"] == chunk_count_map[cell_tag]

##################################################################

class TestTablesMulticell(TestTables):
    NUM_SECONDARY_MASTER_CELLS = 3

    def test_concatenate_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        write_table("//tmp/t1", {"key": "x"})
        assert read_table("//tmp/t1") == [{"key": "x"}]

        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
        write_table("//tmp/t2", {"key": "y"})
        assert read_table("//tmp/t2") == [{"key": "y"}]

        create("table", "//tmp/union", attributes={"external": False})

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        check_multicell_statistics("//tmp/union", {"1": 1, "2": 1})

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}] * 2
        check_multicell_statistics("//tmp/union", {"1": 2, "2": 2})

    def test_concatenate_sorted_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        write_table("//tmp/t1", {"key": "x"})
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert get("//tmp/t1/@sorted", "true")

        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
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
        check_multicell_statistics("//tmp/union", {"1": 1, "2": 1})

    def test_concatenate_foreign_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
        create("table", "//tmp/t3", attributes={"external_cell_tag": 3})

        write_table("//tmp/t1", {"key": "x"})
        concatenate(["//tmp/t1", "//tmp/t1"], "//tmp/t2")
        assert read_table("//tmp/t2") == [{"key": "x"}] * 2
        check_multicell_statistics("//tmp/t2", {"1": 2})

        concatenate(["//tmp/t2", "//tmp/t2"], "//tmp/t3")
        assert read_table("//tmp/t3") == [{"key": "x"}] * 4
        check_multicell_statistics("//tmp/t3", {"1": 4})
