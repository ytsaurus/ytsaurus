import pytest

from yt_env_setup import YTEnvSetup, mark_multicell
from yt_commands import *
from yt.yson import to_yson_type

from time import sleep


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

        write_table("//tmp/table", {"b": "hello"})
        assert read_table("//tmp/table") == [{"b":"hello"}]
        assert get("//tmp/table/@row_count") == 1
        assert get("//tmp/table/@chunk_count") == 1

        write_table("<append=true>//tmp/table", [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}])
        assert read_table("//tmp/table") == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get("//tmp/table/@row_count") == 3
        assert get("//tmp/table/@chunk_count") == 2

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

        content = "some_data"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        with pytest.raises(YtError): read_table("//tmp/file")

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

        # reading key selectors from unsorted table
        with pytest.raises(YtError): read_table("//tmp/table[:a]")

    def test_chunk_index_selector(self):
        create("table", "//tmp/table")

        write_table("<append=true>//tmp/table", [{"a": 0}])
        write_table("<append=true>//tmp/table", [{"b": 1}])
        write_table("<append=true>//tmp/table", [{"c": 2}])

        assert len(get("//tmp/table/@chunk_ids")) == 3

        assert read_table("<upper_limit={chunk_index=1}>//tmp/table") == [{"a": 0}]
        assert read_table("<lower_limit={chunk_index=2}>//tmp/table") == [{"c": 2}]
        assert read_table("<lower_limit={chunk_index=1};upper_limit={chunk_index=2}>//tmp/table") == [{"b": 1}]

    def test_row_key_selector(self):
        create("table", "//tmp/table")

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

        # combination of row and key selectors
        assert read_table('//tmp/table{i}[aa: (b, 10)]') == [{'i' : 5}]

        # limits of different types
        assert read_table("//tmp/table[#0:zz]") == [v1, v2, v3, v4, v5]


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

        write_table("<append=true>//tmp/table", v3, tx=outer_tx) # this won"t be seen from inner
        assert read_table("//tmp/table", tx=outer_tx) == [v1, v3]
        assert read_table("//tmp/table", tx=inner_tx) == [v1, v2]

        write_table("<append=true>//tmp/table", v4, tx=inner_tx)
        assert read_table("//tmp/table", tx=outer_tx) == [v1, v3]
        assert read_table("//tmp/table", tx=inner_tx) == [v1, v2, v4]

        commit_transaction(inner_tx)
        self.assertItemsEqual(read_table("//tmp/table", tx=outer_tx), [v1, v2, v4, v3]) # order is not specified

        commit_transaction(outer_tx)

    def test_codec_in_writer(self):
        create("table", "//tmp/table")
        set("//tmp/table/@compression_codec", "gzip_best_compression")
        write_table("//tmp/table", {"b": "hello"})

        assert read_table("//tmp/table") == [{"b":"hello"}]

        chunk_id = get("//tmp/table/@chunk_ids/0")
        assert get("#%s/@compression_codec" % chunk_id) == "gzip_best_compression"

    def test_copy(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert read_table("//tmp/t") == [{"a" : "b"}]

        copy("//tmp/t", "//tmp/t2")
        assert read_table("//tmp/t2") == [{"a" : "b"}]

        assert get("//tmp/t2/@resource_usage") == get("//tmp/t/@resource_usage")
        assert get("//tmp/t2/@replication_factor") == get("//tmp/t/@replication_factor")

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a" : "b"}]

        remove("//tmp/t2")
        for chunk in get_chunks():
            nodes = get("#%s/@owning_nodes" % chunk)
            for t in ["//tmp/t", "//tmp/t2"]:
                assert t not in nodes

    def test_copy_to_the_same_table(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        with pytest.raises(YtError): copy("//tmp/t", "//tmp/t")

    def test_copy_tx(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        tx = start_transaction()
        assert read_table("//tmp/t", tx=tx) == [{"a" : "b"}]
        copy("//tmp/t", "//tmp/t2", tx=tx)
        assert read_table("//tmp/t2", tx=tx) == [{"a" : "b"}]

        #assert get("//tmp/@recursive_resource_usage") == {"disk_space" : 438}
        #assert get("//tmp/@recursive_resource_usage", tx=tx) == {"disk_space" : 2 * 438}
        commit_transaction(tx)

        assert read_table("//tmp/t2") == [{"a" : "b"}]

        remove("//tmp/t")
        assert read_table("//tmp/t2") == [{"a" : "b"}]

        remove("//tmp/t2")
        for chunk in get_chunks():
            nodes = get("#%s/@owning_nodes" % chunk)
            for t in ["//tmp/t", "//tmp/t2"]:
                assert t not in nodes

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

    def test_invalid_channels_in_create(self):
        with pytest.raises(YtError): create("table", "//tmp/t", attributes={"channels": "123"})

    def test_replication_factor_attr(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@replication_factor") == 3

        with pytest.raises(YtError): remove("//tmp/t/@replication_factor")
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 0)
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", {})

        tx = start_transaction()
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 2, tx=tx)

    def test_replication_factor_static(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 2)

        write_table("//tmp/t", {"foo" : "bar"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1

        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@replication_factor") == 2

    def test_recursive_resource_usage(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})
        copy("//tmp/t1", "//tmp/t2")

        assert get("//tmp/t1/@resource_usage")["disk_space"] + \
               get("//tmp/t2/@resource_usage")["disk_space"] == \
               get("//tmp/@recursive_resource_usage")["disk_space"]

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
            assert get("#" + id + "/@replication_factor") == expected_rf

    def test_vital_update(self):
        create("table", "//tmp/t")
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"})

        def check_vital_chunks(is_vital):
            chunk_ids = get("//tmp/t/@chunk_ids")
            for id in chunk_ids:
                assert get("#" + id + "/@vital") == is_vital

        assert get("//tmp/t/@vital")
        check_vital_chunks(True)

        set("//tmp/t/@vital", False)
        assert not get("//tmp/t/@vital")
        sleep(2)

        check_vital_chunks(False)

    def test_replication_factor_update1(self):
        create("table", "//tmp/t")
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"})
        set("//tmp/t/@replication_factor", 4)
        sleep(2)
        self._check_replication_factor("//tmp/t", 4)

    def test_replication_factor_update2(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 4)
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor("//tmp/t", 4)

    def test_replication_factor_update3(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)
        set("//tmp/t/@replication_factor", 2)
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor("//tmp/t", 2)

    def test_key_columns1(self):
        create("table", "//tmp/t", attributes={"key_columns": ["a", "b"]})
        assert get("//tmp/t/@sorted")
        assert get("//tmp/t/@key_columns") == ["a", "b"]

    def test_key_columns2(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        with pytest.raises(YtError): set("//tmp/t/@key_columns", ["a", "b"])

    def test_key_columns3(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@key_columns", 123)

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

    @only_linux
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

    def test_json_format(self):
        create("table", "//tmp/t")
        write_table('//tmp/t', '{"x":"0"}\n{"x":"1"}', input_format="json", is_raw=True)
        assert '{"x":"0"}\n{"x":"1"}\n' == read_table("//tmp/t", output_format="json")

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

##################################################################

@mark_multicell
class TestTablesMulticell(TestTables):
    NUM_SECONDARY_MASTER_CELLS = 2
