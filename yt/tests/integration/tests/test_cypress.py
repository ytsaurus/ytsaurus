import pytest
import time
import datetime

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type, YsonEntity
from yt.environment.helpers import assert_items_equal

from datetime import timedelta

from dateutil.tz import tzlocal

##################################################################

class TestCypress(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            # See test_map_node_children_limit
            "max_node_child_count" : 100,

            # See test_string_node_length_limit
            "max_string_node_length" : 300,

            # See test_attribute_size_limit
            "max_attribute_size" : 300,

            # See test_map_node_key_length_limit
            "max_map_node_key_length": 300,

            # To make expiration tests run faster
            "expiration_check_period": 10,
            "expiration_backoff_time": 10
        }
    }

    def test_root(self):
        # should not crash
        get("//@")

    def test_invalid_cases(self):
        # path not starting with /
        with pytest.raises(YtError): set("a", 20)

        # path starting with single /
        with pytest.raises(YtError): set("/a", 20)

        # empty path
        with pytest.raises(YtError): set("", 20)

        # empty token in path
        with pytest.raises(YtError): set("//tmp/a//b", 20)

        # change the type of root
        with pytest.raises(YtError): set("/", [])

        # remove the root
        with pytest.raises(YtError): remove("/")

        # get non existent child
        with pytest.raises(YtError): get("//tmp/b")

        # remove non existent child
        with pytest.raises(YtError): remove("//tmp/b")

        # can"t create entity node inside cypress
        with pytest.raises(YtError): set("//tmp/entity", None)

    def test_remove(self):
        with pytest.raises(YtError): remove("//tmp/x", recursive=False)
        with pytest.raises(YtError): remove("//tmp/x")
        remove("//tmp/x", force=True)

        with pytest.raises(YtError): remove("//tmp/1", recursive=False)
        with pytest.raises(YtError): remove("//tmp/1")
        remove("//tmp/1", force=True)

        create("map_node", "//tmp/x/1/y", recursive=True)
        with pytest.raises(YtError): remove("//tmp/x", recursive=False)
        with pytest.raises(YtError): remove("//tmp/x", recursive=False, force=True)
        remove("//tmp/x/1/y", recursive=False)
        remove("//tmp/x")

        set("//tmp/@test_attribute", 10)
        remove("//tmp/@test_attribute")

        for path in ["//tmp/@test_attribute", "//tmp/@test_attribute/inner",
                     "//tmp/@recursive_resource_usage/disk_space_per_medium", "//tmp/@recursive_resource_usage/missing"]:
            with pytest.raises(YtError):
                remove(path)

        for path in ["//tmp/@test_attribute", "//tmp/@test_attribute/inner"]:
            remove(path, force=True)

        for builtin_path in ["//tmp/@key", "//tmp/@key/inner"]:
            with pytest.raises(YtError):
                remove(builtin_path, force=True)

    def test_list(self):
        set("//tmp/list", [1,2,"some string"])
        assert get("//tmp/list") == [1,2,"some string"]

        set("//tmp/list/end", 100)
        assert get("//tmp/list") == [1,2,"some string",100]

        set("//tmp/list/before:0", 200)
        assert get("//tmp/list") == [200,1,2,"some string",100]

        set("//tmp/list/before:0", 500)
        assert get("//tmp/list") == [500,200,1,2,"some string",100]

        set("//tmp/list/after:2", 1000)
        assert get("//tmp/list") == [500,200,1,1000,2,"some string",100]

        set("//tmp/list/3", 777)
        assert get("//tmp/list") == [500,200,1,777,2,"some string",100]

        remove("//tmp/list/4")
        assert get("//tmp/list") == [500,200,1,777,"some string",100]

        remove("//tmp/list/4")
        assert get("//tmp/list") == [500,200,1,777,100]

        remove("//tmp/list/0")
        assert get("//tmp/list") == [200,1,777,100]

        set("//tmp/list/end", "last")
        assert get("//tmp/list") == [200,1,777,100,"last"]

        set("//tmp/list/before:0", "first")
        assert get("//tmp/list") == ["first",200,1,777,100,"last"]

        set("//tmp/list/begin", "very_first")
        assert get("//tmp/list") == ["very_first","first",200,1,777,100,"last"]

    def test_list_command(self):
        set("//tmp/map", {"a": 1, "b": 2, "c": 3})
        assert ls("//tmp/map") == ["a", "b", "c"]

        set("//tmp/map", {"a": 1, "a": 2})
        assert ls("//tmp/map", max_size=1) == ["a"]

        ls("//sys/chunks")
        ls("//sys/accounts")
        ls("//sys/transactions")

    def test_map(self):
        set("//tmp/map", {"hello": "world", "list":[0,"a",{}], "n": 1})
        assert get("//tmp/map") == {"hello":"world","list":[0,"a",{}],"n":1}

        set("//tmp/map/hello", "not_world")
        assert get("//tmp/map") == {"hello":"not_world","list":[0,"a",{}],"n":1}

        set("//tmp/map/list/2/some", "value")
        assert get("//tmp/map") == {"hello":"not_world","list":[0,"a",{"some":"value"}],"n":1}

        remove("//tmp/map/n")
        assert get("//tmp/map") ==  {"hello":"not_world","list":[0,"a",{"some":"value"}]}

        set("//tmp/map/list", [])
        assert get("//tmp/map") == {"hello":"not_world","list":[]}

        set("//tmp/map/list/end", {})
        set("//tmp/map/list/0/a", 1)
        assert get("//tmp/map") == {"hello":"not_world","list":[{"a":1}]}

        set("//tmp/map/list/begin", {})
        set("//tmp/map/list/0/b", 2)
        assert get("//tmp/map") == {"hello":"not_world","list":[{"b":2},{"a":1}]}

        remove("//tmp/map/hello")
        assert get("//tmp/map") == {"list":[{"b":2},{"a":1}]}

        remove("//tmp/map/list")
        assert get("//tmp/map") == {}

        with pytest.raises(YtError):
            set("//tmp/missing/node", {})

        set("//tmp/missing/node", {}, recursive=True)
        assert get("//tmp/missing") == {"node": {}}

    def test_attributes(self):
        set("//tmp/t", "<attr=100;mode=rw> {nodes=[1; 2]}", is_raw=True)
        assert get("//tmp/t/@attr") == 100
        assert get("//tmp/t/@mode") == "rw"

        attrs = get("//tmp/t/@")
        assert "attr" in attrs
        assert "mode" in attrs
        assert "path" in attrs
        assert isinstance(attrs["path"], YsonEntity)

        attrs = get("//tmp/t/@", attributes=["attr", "path"])
        assert sorted(attrs.keys()) == ["attr", "path"]
        assert attrs["path"] == "//tmp/t"

        remove("//tmp/t/@*")
        with pytest.raises(YtError): get("//tmp/t/@attr")
        with pytest.raises(YtError): get("//tmp/t/@mode")

        # changing attributes
        set("//tmp/t/a", "<author = ignat> []", is_raw=True)
        assert get("//tmp/t/a") == []
        assert get("//tmp/t/a/@author") == "ignat"

        set("//tmp/t/a/@author", "not_ignat")
        assert get("//tmp/t/a/@author") == "not_ignat"

        # nested attributes (actually shows <>)
        set("//tmp/t/b", "<dir = <file = <>-100> #> []", is_raw=True)
        assert get("//tmp/t/b/@dir/@") == {"file": -100}
        assert get("//tmp/t/b/@dir/@file") == -100
        assert get("//tmp/t/b/@dir/@file/@") == {}

        # set attributes directly
        set("//tmp/t/@", {"key1": "value1", "key2": "value2"})
        assert get("//tmp/t/@key1") == "value1"
        assert get("//tmp/t/@key2") == "value2"

        # error cases
        # typo (extra slash)
        with pytest.raises(YtError): get("//tmp/t/@/key1")
        # change type
        with pytest.raises(YtError): set("//tmp/t/@", 1)
        with pytest.raises(YtError): set("//tmp/t/@", "a")
        with pytest.raises(YtError): set("//tmp/t/@", [])
        with pytest.raises(YtError): set("//tmp/t/@", [1, 2, 3])

    def test_attributes_tx_read_table(self):
        set("//tmp/t", "<attr=100> 123", is_raw=True)
        assert get("//tmp/t") == 123
        assert get("//tmp/t/@attr") == 100
        assert "attr" in get("//tmp/t/@")

        tx = start_transaction()
        assert get("//tmp/t/@attr", tx = tx) == 100
        assert "attr" in get("//tmp/t/@", tx = tx)

    def test_format_json(self):
        # check input format for json
        set("//tmp/json_in", '{"list": [1,2,{"string": "this"}]}', is_raw=True, input_format="json")
        assert get("//tmp/json_in") == {"list": [1, 2, {"string": "this"}]}

        # check output format for json
        set("//tmp/json_out", {"list": [1, 2, {"string": "this"}]})
        assert get("//tmp/json_out", is_raw=True, output_format="json") == '{"list":[1,2,{"string":"this"}]}'

    def test_map_remove_all1(self):
        # remove items from map
        set("//tmp/map", {"a" : "b", "c": "d"})
        assert get("//tmp/map/@count") == 2
        remove("//tmp/map/*")
        assert get("//tmp/map") == {}
        assert get("//tmp/map/@count") == 0

    def test_map_remove_all2(self):
        set("//tmp/map", {"a" : 1})
        tx = start_transaction()
        set("//tmp/map", {"b" : 2}, tx = tx)
        remove("//tmp/map/*", tx = tx)
        assert get("//tmp/map", tx = tx) == {}
        assert get("//tmp/map/@count", tx = tx) == 0
        commit_transaction(tx)
        assert get("//tmp/map") == {}
        assert get("//tmp/map/@count") == 0

    def test_list_remove_all(self):
        # remove items from list
        set("//tmp/list", [10, 20, 30])
        assert get("//tmp/list/@count") == 3
        remove("//tmp/list/*")
        assert get("//tmp/list") == []
        assert get("//tmp/list/@count") == 0

    def test_attr_remove_all1(self):
        # remove items from attributes
        set("//tmp/attr", "<_foo=bar;_key=value>42", is_raw=True);
        remove("//tmp/attr/@*")
        with pytest.raises(YtError): get("//tmp/attr/@_foo")
        with pytest.raises(YtError): get("//tmp/attr/@_key")

    def test_attr_remove_all2(self):
        set("//tmp/@a", 1)
        tx = start_transaction()
        set("//tmp/@b", 2, tx = tx)
        remove("//tmp/@*", tx = tx)
        with pytest.raises(YtError): get("//tmp/@a", tx = tx)
        with pytest.raises(YtError): get("//tmp/@b", tx = tx)
        commit_transaction(tx)
        with pytest.raises(YtError): get("//tmp/@a")
        with pytest.raises(YtError): get("//tmp/@b")

    def test_copy_simple1(self):
        set("//tmp/a", 1)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == 1

    def test_copy_simple2(self):
        set("//tmp/a", [1, 2, 3])
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == [1, 2, 3]

    def test_copy_simple3(self):
        set("//tmp/a", "<x=y> 1", is_raw=True)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@x") == "y"

    def test_copy_simple4(self):
        set("//tmp/a", {"x1" : "y1", "x2" : "y2"})
        assert get("//tmp/a/@count") == 2

        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@count") == 2

    def test_copy_simple5(self):
        set("//tmp/a", { "b" : 1 })
        assert get("//tmp/a/b/@path") == "//tmp/a/b"

        copy("//tmp/a", "//tmp/c")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

        remove("//tmp/a")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

    def test_copy_simple6a(self):
        with pytest.raises(YtError): copy("//tmp", "//tmp/a")

    def test_copy_simple6b(self):
        tx = start_transaction()
        create("map_node", "//tmp/a", tx=tx)
        create("map_node", "//tmp/a/b", tx=tx)
        with pytest.raises(YtError): copy("//tmp/a", "//tmp/a/b/c", tx=tx)

    def test_copy_simple7(self):
        tx = start_transaction()
        with pytest.raises(YtError): copy("#" + tx, "//tmp/t")

    def test_copy_simple8(self):
        create("map_node", "//tmp/a")
        create("table", "//tmp/a/t")
        link("//tmp/a", "//tmp/b")
        copy("//tmp/b/t", "//tmp/t")

    def test_copy_recursive_success(self):
        create("map_node", "//tmp/a")
        copy("//tmp/a", "//tmp/b/c", recursive=True)

    def test_copy_recursive_fail(self):
        create("map_node", "//tmp/a")
        with pytest.raises(YtError):
            copy("//tmp/a", "//tmp/b/c", recursive=False)

        with pytest.raises(YtError):
            copy("//tmp/a", "//tmp/b/c/d/@e", recursive=True)
        assert not exists("//tmp/b/c/d")

    def test_copy_tx1(self):
        tx = start_transaction()

        set("//tmp/a", {"x1" : "y1", "x2" : "y2"}, tx=tx)
        assert get("//tmp/a/@count", tx=tx) == 2

        copy("//tmp/a", "//tmp/b", tx=tx)
        assert get("//tmp/b/@count", tx=tx) == 2

        commit_transaction(tx)

        assert get("//tmp/a/@count") == 2
        assert get("//tmp/b/@count") == 2

    def test_copy_tx2(self):
        set("//tmp/a", {"x1" : "y1", "x2" : "y2"})

        tx = start_transaction()

        remove("//tmp/a/x1", tx=tx)
        assert get("//tmp/a/@count", tx=tx) == 1

        copy("//tmp/a", "//tmp/b", tx=tx)
        assert get("//tmp/b/@count", tx=tx) == 1

        commit_transaction(tx)

        assert get("//tmp/a/@count") == 1
        assert get("//tmp/b/@count") == 1

    def test_copy_account1(self):
        create_account("a1")
        create_account("a2")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a1")
        set("//tmp/a2", {})
        set("//tmp/a2/@account", "a2")

        set("//tmp/a1/x", {"y" : "z"})
        copy("//tmp/a1/x", "//tmp/a2/x")

        assert get("//tmp/a2/@account") == "a2"
        assert get("//tmp/a2/x/@account") == "a2"
        assert get("//tmp/a2/x/y/@account") == "a2"

    def test_copy_account2(self):
        create_account("a1")
        create_account("a2")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a1")
        set("//tmp/a2", {})
        set("//tmp/a2/@account", "a2")

        set("//tmp/a1/x", {"y" : "z"})
        copy("//tmp/a1/x", "//tmp/a2/x", preserve_account=True)

        assert get("//tmp/a2/@account") == "a2"
        assert get("//tmp/a2/x/@account") == "a1"
        assert get("//tmp/a2/x/y/@account") == "a1"

    def test_copy_unexisting_path(self):
        with pytest.raises(YtError): copy("//tmp/x", "//tmp/y")

    def test_copy_cannot_have_children(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        with pytest.raises(YtError): copy("//tmp/t2", "//tmp/t1/xxx")

    def test_copy_table_compression_codec(self):
        create("table", "//tmp/t1")
        assert get("//tmp/t1/@compression_codec") == "lz4"
        set("//tmp/t1/@compression_codec", "zlib_6")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@compression_codec") == "zlib_6"

    def test_copy_from_another_tx1(self):
        tx = start_transaction()
        create("table", "//tmp/t", tx=tx)

        copy("//tmp/t", "//tmp/t2", source_transaction_id=tx)
        assert exists("//tmp/t2")
        assert exists("//tmp/t", tx=tx)
        assert not exists("//tmp/t")

        with pytest.raises(YtError): move("//tmp/t", "//tmp/t3", source_transaction_id=tx)

        commit_transaction(tx)
        assert exists("//tmp/t")
        assert exists("//tmp/t2")
        assert not exists("//tmp/t3")

    def test_copy_from_another_tx2(self):
        tx = start_transaction()
        create("table", "//tmp/t", tx=tx)

        tx2 = start_transaction()

        copy("//tmp/t", "//tmp/t2", tx=tx2, source_transaction_id=tx)
        assert exists("//tmp/t2", tx=tx2)
        assert exists("//tmp/t", tx=tx)
        assert not exists("//tmp/t")
        assert not exists("//tmp/t2")

        with pytest.raises(YtError): move("//tmp/t", "//tmp/t3", tx=tx2, source_transaction_id=tx)

        commit_transaction(tx2)
        assert exists("//tmp/t2")
        assert not exists("//tmp/t3")

        commit_transaction(tx)
        assert exists("//tmp/t")

    def test_compression_codec_in_tx(self):
        create("table", "//tmp/t", attributes={"compression_codec": "none"})
        assert get("//tmp/t/@compression_codec") == "none"
        tx = start_transaction()
        assert get("//tmp/t/@compression_codec", tx=tx) == "none"
        set("//tmp/t/@compression_codec", "lz4", tx=tx)
        assert get("//tmp/t/@compression_codec", tx=tx) == "lz4"
        assert get("//tmp/t/@compression_codec") == "none"
        locks = get("//tmp/t/@locks")
        assert len(locks) == 1
        assert locks[0]["mode"] == "shared"
        assert locks[0]["transaction_id"] == tx
        assert locks[0]["attribute_key"] == "compression_codec"
        with pytest.raises(YtError): set("//tmp/t/@compression_codec", "lz4")
        commit_transaction(tx)
        assert get("//tmp/t/@compression_codec") == "lz4"

    def test_copy_id1(self):
        set("//tmp/a", 123)
        a_id = get("//tmp/a/@id")
        copy("#" + a_id, "//tmp/b")
        assert get("//tmp/b") == 123

    def test_copy_id2(self):
        set("//tmp/a", 123)
        tmp_id = get("//tmp/@id")
        copy("#" + tmp_id + "/a", "//tmp/b")
        assert get("//tmp/b") == 123

    def test_copy_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@account") == "tmp"

    def test_copy_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    def test_copy_force1(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@a", 1)
        create("table", "//tmp/t2")
        set("//tmp/t2/@a", 2)
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@a", 1)

    def test_copy_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2", force=True)

    def test_copy_force3(self):
        create("table", "//tmp/t1")
        set("//tmp/t2", {})
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@type") == "table"

    def test_copy_force_account1(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=True)

        assert get("//tmp/a2/@account") == "a"

    def test_copy_force_account2(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=False)

        assert get("//tmp/a2/@account") == "tmp"

    def test_copy_locked(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        lock("//tmp/t1", tx=tx)
        copy("//tmp/t1", "//tmp/t2")
        commit_transaction(tx)

    def test_copy_acd(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), [])

    def test_move_acd(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        move("//tmp/t1", "//tmp/t2")
        assert not get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), acl)


    def test_move_simple1(self):
        set("//tmp/a", 1)
        move("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == 1
        with pytest.raises(YtError): get("//tmp/a")

    def test_move_simple2(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        lock("//tmp/a", tx=tx)

        with pytest.raises(YtError): move("//tmp/a", "//tmp/b")
        assert not exists("//tmp/b")

    def test_move_simple3(self):
        with pytest.raises(YtError): move("//tmp", "//tmp/a")

    def test_move_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        move("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    def test_move_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        move("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@account") == "tmp"

    def test_move_recursive_success(self):
        create("map_node", "//tmp/a")
        move("//tmp/a", "//tmp/b/c", recursive=True)

    def test_move_recursive_fail(self):
        create("map_node", "//tmp/a")
        with pytest.raises(YtError):
            move("//tmp/a", "//tmp/b/c", recursive=False)

        with pytest.raises(YtError):
            move("//tmp/a", "//tmp/b/c/d/@e", recursive=True)
        assert not exists("//tmp/b/c/d")

    def test_move_force1(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@a", 1)
        create("table", "//tmp/t2")
        set("//tmp/t2/@a", 2)
        move("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@a", 1)
        assert not exists("//tmp/t1")

    def test_move_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2", force=True)
        assert exists("//tmp/t1")

    def test_move_force3(self):
        create("table", "//tmp/t1")
        set("//tmp/t2", {})
        move("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@type") == "table"
        assert not exists("//tmp/t1")

    def test_move_force4(self):
        with pytest.raises(YtError): copy("//tmp", "/", force=True)

    def test_move_tx_commit(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert exists("//tmp/t1")
        assert not exists("//tmp/t2")
        assert not exists("//tmp/t1", tx=tx)
        assert exists("//tmp/t2", tx=tx)
        commit_transaction(tx)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2")

    def test_move_tx_abort(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert exists("//tmp/t1")
        assert not exists("//tmp/t2")
        assert not exists("//tmp/t1", tx=tx)
        assert exists("//tmp/t2", tx=tx)
        abort_transaction(tx)
        assert exists("//tmp/t1")
        assert not exists("//tmp/t2")

    def test_move_tx_nested(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        tx2 = start_transaction(tx=tx1)
        move("//tmp/t2", "//tmp/t3", tx=tx2)
        commit_transaction(tx2)
        commit_transaction(tx1)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")
        assert exists("//tmp/t3")

    def test_move_tx_locking1(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        tx2 = start_transaction()
        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t3", tx=tx2)

    def test_move_tx_locking2(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx1)
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        move("//tmp/t2", "//tmp/t3", tx=tx2)
        with pytest.raises(YtError): move("//tmp/t2", "//tmp/t4", tx=tx3)

    def test_embedded_attributes(self):
        set("//tmp/a", {})
        set("//tmp/a/@attr", {"key": "value"})
        set("//tmp/a/@attr/key/@embedded_attr", "emb")
        assert get("//tmp/a/@attr") == {"key": to_yson_type("value", attributes={"embedded_attr": "emb"})}
        assert get("//tmp/a/@attr/key") == to_yson_type("value", attributes={"embedded_attr": "emb"})
        assert get("//tmp/a/@attr/key/@embedded_attr") == "emb"

    def test_get_with_attributes(self):
        set("//tmp/a", {})
        assert get("//tmp", attributes=["type"]) == to_yson_type({"a": to_yson_type({}, {"type": "map_node"})}, {"type": "map_node"})

    def test_list_with_attributes(self):
        set("//tmp/a", {})
        assert ls("//tmp", attributes=["type"]) == [to_yson_type("a", attributes={"type": "map_node"})]

    def test_get_with_attributes_virtual_maps(self):
        tx = start_transaction()
        assert get("//sys/transactions", attributes=["type"]) == to_yson_type(\
            {tx: to_yson_type(None, attributes={"type": "transaction"})},
            attributes={"type": "transaction_map"})

    def test_move_virtual_maps1(self):
        create("tablet_map", "//tmp/t")
        move("//tmp/t", "//tmp/tt")

    def test_move_virtual_maps2(self):
        create("chunk_map", "//tmp/c")
        move("//tmp/c", "//tmp/cc")

    def test_list_with_attributes_virtual_maps(self):
        tx = start_transaction()
        assert ls("//sys/transactions", attributes=["type"]) == [to_yson_type(tx, attributes={"type": "transaction"})]

    def test_exists(self):
        assert exists("//tmp")
        assert not exists("//tmp/a")
        assert not exists("//tmp/a/f/e")
        assert not exists("//tmp/a/1/e")
        assert not exists("//tmp/a/2/1")

        set("//tmp/1", {})
        assert exists("//tmp/1")
        assert not exists("//tmp/1/2")

        set("//tmp/a", {})
        assert exists("//tmp/a")

        set("//tmp/a/@list", [10])
        assert exists("//tmp/a/@list")
        assert exists("//tmp/a/@list/0")
        assert not exists("//tmp/a/@list/1")

        assert not exists("//tmp/a/@attr")
        set("//tmp/a/@attr", {"key": "value"})
        assert exists("//tmp/a/@attr")

        assert exists("//sys/operations")
        assert exists("//sys/transactions")
        assert not exists("//sys/xxx")
        assert not exists("//sys/operations/xxx")

    def test_remove_tx1(self):
        set("//tmp/a", 1)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")
        tx = start_transaction()
        remove("//tmp/a", tx=tx)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")
        abort_transaction(tx)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")

    def test_create(self):
        create("map_node", "//tmp/some_node")

    def test_create_recursive_fail(self):
        create("map_node", "//tmp/some_node")
        with pytest.raises(YtError): create("map_node", "//tmp/a/b")

    def test_create_recursive_success(self):
        create("map_node", "//tmp/a/b", recursive=True)

    def test_create_ignore_existing_success(self):
        create("map_node", "//tmp/a/b", recursive=True)
        create("map_node", "//tmp/a/b", ignore_existing=True)

    def test_create_ignore_existing_fail(self):
        create("map_node", "//tmp/a/b", recursive=True)
        with pytest.raises(YtError): create("table", "//tmp/a/b", ignore_existing=False)

    def test_create_ignore_existing_force_fail(self):
        with pytest.raises(YtError): create("table", "//tmp/t", ignore_existing=True, force=True)

    def test_create_force(self):
        id1 = create("table", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id1
        id2 = create("table", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id2
        id3 = create("file", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id3

    def test_create_recursive(self):
        assert not exists("//tmp/a/b/c/d")
        with pytest.raises(YtError):
            create("map_node", "//tmp/a/b/c/d/@d", recursive=True)
        assert not exists("//tmp/a/b/c/d")
        create("map_node", "//tmp/a/b/c/d", recursive=True)
        assert exists("//tmp/a/b/c/d")

    def test_link1(self):
        with pytest.raises(YtError): link("//tmp/a", "//tmp/b")

    def test_link2(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2") == 1
        assert get("//tmp/t2/@type") == "int64_node"
        assert get("//tmp/t1/@id") == get("//tmp/t2/@id")
        assert get("//tmp/t2&/@type") == "link"
        assert not get("//tmp/t2&/@broken")

        set("//tmp/t1", 2)
        assert get("//tmp/t2") == 2

    def test_link3(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        remove("//tmp/t1")
        assert get("//tmp/t2&/@broken")

    def test_link4(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")

        tx = start_transaction()
        id = get("//tmp/t1/@id")
        lock("#%s" % id, mode = "snapshot", tx = tx)

        remove("//tmp/t1")

        assert get("#%s" % id, tx = tx) == 1
        assert get("//tmp/t2&/@broken")
        with pytest.raises(YtError): read_table("//tmp/t2")

    def test_link5(self):
        set("//tmp/t1", 1)
        set("//tmp/t2", 2)
        with pytest.raises(YtError): link("//tmp/t1", "//tmp/t2")

    def test_link6(self):
        create("table", "//tmp/a")
        link("//tmp/a", "//tmp/b")

        assert exists("//tmp/a")
        assert exists("//tmp/b")
        assert exists("//tmp/b&")
        assert exists("//tmp/b/@id")
        assert exists("//tmp/b/@row_count")
        assert exists("//tmp/b&/@target_path")
        assert not exists("//tmp/b/@x")
        assert not exists("//tmp/b/x")
        assert not exists("//tmp/b&/@x")
        assert not exists("//tmp/b&/x")

        remove("//tmp/a")

        assert not exists("//tmp/a")
        assert not exists("//tmp/b")
        assert exists("//tmp/b&")
        assert not exists("//tmp/b/@id")
        assert not exists("//tmp/b/@row_count")
        assert exists("//tmp/b&/@target_path")
        assert not exists("//tmp/b/@x")
        assert not exists("//tmp/b/x")
        assert not exists("//tmp/b&/@x")
        assert not exists("//tmp/b&/x")

    def test_link7(self):
        tx = start_transaction()
        set("//tmp/t1", 1, tx=tx)
        link("//tmp/t1", "//tmp/l1", tx=tx)
        assert get("//tmp/l1", tx=tx) == 1

    def test_link_existing_fail(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        assert get("//tmp/l/@id") == id1
        with pytest.raises(YtError): link("//tmp/t2", "//tmp/l")

    def test_link_ignore_existing(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", ignore_existing=True)
        assert get("//tmp/l/@id") == id1

    def test_link_force1(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/t1/@id") == id1
        assert get("//tmp/l/@id") == id2

    def test_link_force2(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        remove("//tmp/t1")
        assert get("//tmp/l&/@broken")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@id") == id2

    def test_link_ignore_existing_force_fail(self):
        id1 = create("table", "//tmp/t")
        with pytest.raises(YtError): link("//tmp/t", "//tmp/l", ignore_existing=True, force=True)

    def test_link_to_link(self):
        id = create("table", "//tmp/t")
        link("//tmp/t", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        assert get("//tmp/l2/@id") == id
        assert not get("//tmp/l2&/@broken")
        remove("//tmp/l1")
        assert get("//tmp/l2&/@broken")

    def test_link_as_copy_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError): copy("//tmp/t2", "//tmp/l")

    def test_link_as_copy_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        copy("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert get("//tmp/t1/@id") == id1

    def test_link_as_move_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError): move("//tmp/t2", "//tmp/l")

    def test_link_as_copy_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        move("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert not exists("//tmp/t2")
        assert get("//tmp/t1/@id") == id1

    def test_move_in_tx_with_link_yt_6610(self):
        create("map_node", "//tmp/a")
        link("//tmp/a", "//tmp/b")
        tx = start_transaction()
        set("//tmp/a/x", 1, tx=tx)
        set("//tmp/a/y", 2, tx=tx)
        assert get("//tmp/a/y", tx=tx) == 2
        assert get("//tmp/b/y", tx=tx) == 2
        move("//tmp/b/x", "//tmp/b/y", force=True, tx=tx)
        assert not exists("//tmp/b/x", tx=tx)
        assert get("//tmp/b/y", tx=tx) == 1
        commit_transaction(tx)
        assert not exists("//tmp/b/x")
        assert get("//tmp/b/y") == 1

    def test_resolve_suppress_via_object_id_yt_6694(self):
        create("map_node", "//tmp/a")
        link("//tmp/a", "//tmp/b")
        id = get("//tmp/b&/@id")
        assert get("//tmp/b/@type") == "map_node"
        assert get("//tmp/b&/@type") == "link"
        assert get("#{0}/@type".format(id)) == "map_node"
        assert get("#{0}&/@type".format(id)) == "link"

    def test_access_stat1(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c2 == c1

    def test_access_stat2(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        tx = start_transaction()
        lock("//tmp", mode = "snapshot", tx = tx)
        time.sleep(1)
        c2 = get("//tmp/@access_counter", tx = tx)
        assert c2 == c1 + 1

    def test_access_stat3(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        get("//tmp/@")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat4(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        assert exists("//tmp")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat5(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        assert exists("//tmp/@id")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat6(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        ls("//tmp/@")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat7(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        ls("//tmp")
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c2 == c1 + 1

    def test_access_stat8(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@access_time") == get("//tmp/t/@creation_time")

    def test_access_stat9(self):
        create("table", "//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@access_time") == get("//tmp/t2/@creation_time")

    def test_access_stat_suppress1(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        get("//tmp", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat_suppress2(self):
        time.sleep(1)
        c1 = get("//tmp/@access_counter")
        ls("//tmp", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/@access_counter")
        assert c1 == c2

    def test_access_stat_suppress3(self):
        time.sleep(1)
        create("table", "//tmp/t")
        c1 = get("//tmp/t/@access_counter")
        read_table("//tmp/t", table_reader={"suppress_access_tracking": True})
        time.sleep(1)
        c2 = get("//tmp/t/@access_counter")
        assert c1 == c2

    def test_access_stat_suppress4(self):
        time.sleep(1)
        create("file", "//tmp/f")
        c1 = get("//tmp/f/@access_counter")
        read_file("//tmp/f", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/f/@access_counter")
        assert c1 == c2

    def test_chunk_maps(self):
        gc_collect()
        assert get("//sys/chunks/@count") == 0
        assert get("//sys/underreplicated_chunks/@count") == 0
        assert get("//sys/overreplicated_chunks/@count") == 0

    def test_list_attributes(self):
        create("map_node", "//tmp/map", attributes={"user_attr1": 10})
        set("//tmp/map/@user_attr2", "abc")
        assert sorted(get("//tmp/map/@user_attribute_keys")) == sorted(["user_attr1", "user_attr2"])

        create("table", "//tmp/table")
        assert get("//tmp/table/@user_attribute_keys") == []

        create("file", "//tmp/file")
        assert get("//tmp/file/@user_attribute_keys") == []

    def test_boolean(self):
        yson_format = yson.loads("<boolean_as_string=false>yson")
        set("//tmp/boolean", "%true", is_raw=True)
        assert get("//tmp/boolean/@type") == "boolean_node"
        assert get("//tmp/boolean", output_format=yson_format)

    def test_uint64(self):
        yson_format = yson.loads("yson")
        set("//tmp/my_uint", "123456u", is_raw=True)
        assert get("//tmp/my_uint/@type") == "uint64_node"
        assert get("//tmp/my_uint", output_format=yson_format) == 123456

    def test_map_node_children_limit(self):
        create("map_node", "//tmp/test_node")
        for i in xrange(100):
            create("map_node", "//tmp/test_node/" + str(i))
        with pytest.raises(YtError):
            create("map_node", "//tmp/test_node/100")

    def test_string_node_length_limit(self):
        set("//tmp/test_node", "x" * 300)
        remove("//tmp/test_node")

        with pytest.raises(YtError):
            set("//tmp/test_node", "x" * 301)

        with pytest.raises(YtError):
            set("//tmp/test_node", {"key": "x" * 301})

        with pytest.raises(YtError):
            set("//tmp/test_node", ["x" * 301])

    def test_attribute_size_limit(self):
        set("//tmp/test_node", {})

        # The limit is 300 but this is for binary YSON.
        set("//tmp/test_node/@test_attr", "x" * 290)

        with pytest.raises(YtError):
            # This must definitely exceed the limit of 300.
            set("//tmp/test_node/@test_attr", "x" * 301)

    def test_map_node_key_length_limits(self):
        set("//tmp/" + "a" * 300, 0)
        with pytest.raises(YtError):
            set("//tmp/" + "a" * 301, 0)

    def test_invalid_external_cell_bias(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": -1.0})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": 2.0})


    def _now(self):
        return datetime.now(tzlocal())

    def test_expiration_time_validation(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@expiration_time", "hello")

    def test_expiration_time_change_requires_remove_permission_failure(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//tmp/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            set("//tmp/t/@expiration_time", str(self._now()), authenticated_user="u")

    def test_expiration_time_change_requires_recursive_remove_permission_failure(self):
        create_user("u")
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t")
        set("//tmp/m/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            set("//tmp/m/@expiration_time", str(self._now()), authenticated_user="u")

    def test_expiration_time_reset_requires_write_permission_success(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        set("//tmp/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        remove("//tmp/t/@expiration_time", authenticated_user="u")

    def test_expiration_time_reset_requires_write_permission_failure(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        set("//tmp/t/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            remove("//tmp/t/@expiration_time", authenticated_user="u")

    def test_expiration_time_change(self):
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        assert get("//tmp/t/@expiration_time") == "2030-04-03T21:25:29.000000Z"
        remove("//tmp/t/@expiration_time")
        assert not exists("//tmp/t/@expiration_time")

    def test_expiration_time_can_be_set_upon_construction1(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())}, authenticated_user="u")
        time.sleep(0.1)
        assert not exists("//tmp/t")

    def test_expiration_time_can_be_set_upon_construction2(self):
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now() + timedelta(seconds=10.0))})
        time.sleep(1)
        assert exists("//tmp/t")

    def test_expiration_time_can_be_set_upon_construction3(self):
        tx = start_transaction()
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())}, tx=tx)
        time.sleep(1)
        assert not exists("//tmp/t")
        assert exists("//tmp/t", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t")

    def test_expiration_time_removal(self):
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())})
        time.sleep(1)
        assert not exists("//tmp/t")

    def test_expiration_time_wait_for_locks_released(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx)
        set("//tmp/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert exists("//tmp/t")
        abort_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t")

    def test_expiration_time_wait_for_parent_locks_released(self):
        create("table", "//tmp/x/t", recursive=True)
        tx = start_transaction()
        lock("//tmp/x", tx=tx)
        set("//tmp/x/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert exists("//tmp/x/t")
        abort_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/x/t")

    def test_expiration_time_wait_for_locks_released_recursive(self):
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t")
        tx = start_transaction()
        lock("//tmp/m/t", tx=tx)
        set("//tmp/m/@expiration_time", str(self._now()))
        time.sleep(1)
        assert exists("//tmp/m")
        abort_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/m")

    def test_expiration_time_dont_wait_for_snapshot_locks(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx, mode="snapshot")
        set("//tmp/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert not exists("//tmp/t")

    def test_no_expiration_time_for_root(self):
        with pytest.raises(YtError): set("//@expiration_time", str(self._now()))

    def test_copy_preserve_expiration_time(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True)
        assert exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")

    def test_copy_dont_preserve_expiration_time(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        copy("//tmp/t1", "//tmp/t2")
        assert not exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2")

    def test_copy_preserve_expiration_time_in_tx(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        tx = start_transaction()
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True, tx=tx)
        assert exists("//tmp/t2/@expiration_time", tx=tx)
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t2")

    def test_expire_orphaned_node_yt_8064(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        node_id = create("table", "//tmp/t", attributes={"expiration_time": str(self._now() + timedelta(seconds=2))}, tx=tx1)
        lock("#" + node_id, tx=tx2, mode="snapshot")
        abort_transaction(tx1)
        time.sleep(2)

    def test_copy_preserve_creation_time(self):
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@creation_time")

        copy("//tmp/t1", "//tmp/t2", preserve_creation_time=True)
        assert creation_time == get("//tmp/t2/@creation_time")

        move("//tmp/t2", "//tmp/t1", force=True)
        assert creation_time == get("//tmp/t1/@creation_time")

        copy("//tmp/t1", "//tmp/t2")
        new_creation_time = get("//tmp/t2/@creation_time")
        assert creation_time != new_creation_time

        move("//tmp/t2", "//tmp/t1", force=True)
        assert new_creation_time == get("//tmp/t1/@creation_time")

    def test_ignore_ampersand1(self):
        set("//tmp/map", {})
        set("//tmp&/map&/a", "b")
        assert get("//tmp&/map&/a") == "b"
        assert get("//tmp/map&/@type") == "map_node"

    def test_ignore_ampersand2(self):
        set("//tmp/list", [])
        set("//tmp&/list&/end", "x")
        assert get("//tmp&/list&/0") == "x"
        assert get("//tmp/list&/@type") == "list_node"

    def test_ignore_ampersand3(self):
        assert get("//sys/chunks&/@type") == "chunk_map"

    def test_ignore_ampersand4(self):
        assert not exists("//tmp/missing")
        assert not exists("//tmp/missing&")
        assert exists("//tmp")
        assert exists("//tmp&")


    def test_batch_empty(self):
        assert execute_batch([]) == []

    def test_batch_error(self):
        results = execute_batch([
            make_batch_request("get", path="//nonexisting1"),
            make_batch_request("get", path="//nonexisting2")
        ])
        assert len(results) == 2
        with pytest.raises(YtError): get_batch_output(results[0])
        with pytest.raises(YtError): get_batch_output(results[1])

    def test_batch_success(self):
        set_results = execute_batch([
            make_batch_request("set", path="//tmp/a", input="a"),
            make_batch_request("set", path="//tmp/b", input="b")
        ])
        assert len(set_results) == 2
        assert get_batch_output(set_results[0]) == None
        assert get_batch_output(set_results[1]) == None

        get_results = execute_batch([
            make_batch_request("get", path="//tmp/a"),
            make_batch_request("get", path="//tmp/b")
        ])
        assert len(get_results) == 2
        assert get_batch_output(get_results[0]) == "a"
        assert get_batch_output(get_results[1]) == "b"

    def test_batch_with_concurrency_failure(self):
        with pytest.raises(YtError): execute_batch([], concurrency=-1)

    def test_batch_with_concurrency_success(self):
        for i in xrange(10):
            set("//tmp/{0}".format(i), i)
        get_results = execute_batch([
            make_batch_request("get", path="//tmp/{0}".format(i)) for i in xrange(10)
        ], concurrency=2)
        assert len(get_results) == 10
        for i in xrange(10):
            assert get_batch_output(get_results[i]) == i

    def test_recursive_resource_usage_map(self):
        create("map_node", "//tmp/m")
        for i in xrange(10):
            set("//tmp/m/" + str(i), i)
        assert get("//tmp/m/@recursive_resource_usage/node_count") == 11

    def test_recursive_resource_usage_list(self):
        create("list_node", "//tmp/l")
        for i in xrange(10):
            set("//tmp/l/end", i)
        assert get("//tmp/l/@recursive_resource_usage/node_count") == 11

    def test_prerequisite_transactions(self):
        with pytest.raises(YtError):
            set("//tmp/test_node", {}, prerequisite_transaction_ids=["a-b-c-d"])

        tx = start_transaction()
        set("//tmp/test_node", {}, prerequisite_transaction_ids=[tx])
        remove("//tmp/test_node", prerequisite_transaction_ids=[tx])

    def test_prerequisite_revisions(self):
        create("map_node", "//tmp/test_node")
        revision = get("//tmp/test_node/@revision")

        with pytest.raises(YtError):
            create("map_node", "//tmp/test_node/inner_node",
                   prerequisite_revisions=[{"path": "//tmp/test_node", "transaction_id": "0-0-0-0", "revision": revision + 1}])

        create("map_node", "//tmp/test_node/inner_node",
               prerequisite_revisions=[{"path": "//tmp/test_node", "transaction_id": "0-0-0-0", "revision": revision}])

    def test_move_preserves_creation_time1(self):
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time == get("//tmp/t2/@creation_time")

    def test_move_preserves_creation_time2(self):
        set("//tmp/t1", {"x": "y"})
        creation_time1 = get("//tmp/t1/@creation_time")
        creation_time2 = get("//tmp/t1/x/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time1 == get("//tmp/t2/@creation_time")
        assert creation_time2 == get("//tmp/t2/x/@creation_time")

    def test_document(self):
        create("document", "//tmp/d1")

        set("//tmp/d1", {})
        assert get("//tmp/d1") == {}
        assert get("//tmp/d1/@type") == "document"

        set("//tmp/d1/value", 10)
        assert get("//tmp/d1/value") == 10

        with pytest.raises(YtError):
            set("//tmp/d1/some/path", "hello")

        set("//tmp/d1/some/path", "hello", recursive=True)
        assert get("//tmp/d1") == {"value": 10, "some": {"path": "hello"}}

    def test_setting_document_node_increases_revision_yt_7829(self):
        create("document", "//tmp/d1")
        revision1 = get("//tmp/d1/@revision")

        set("//tmp/d1", {"a": {"b": ["c", "d", "e"]}})
        revision2 = get("//tmp/d1/@revision")
        assert revision2 > revision1

        set("//tmp/d1/@value", {"f": {"g": ["h"], "i": "j"}})
        revision3 = get("//tmp/d1/@revision")
        assert revision3 > revision2

        set("//tmp/d1/f/g", ["k", "l", "m"])
        revision4 = get("//tmp/d1/@revision")
        assert revision4 > revision3

        set("//tmp/d1/@value/f/g", ["n", "o", "p"])
        revision5 = get("//tmp/d1/@revision")
        assert revision5 > revision4

        remove("//tmp/d1/f/g")
        revision6 = get("//tmp/d1/@revision")
        assert revision6 > revision5

        remove("//tmp/d1/@value/f/i")
        revision7 = get("//tmp/d1/@revision")
        assert revision7 > revision6

        with pytest.raises(YtError): remove("//tmp/d1/f/i")
        revision8 = get("//tmp/d1/@revision")
        assert revision8 == revision7

        with pytest.raises(YtError): remove("//tmp/d1/@value/f/i")
        revision9 = get("//tmp/d1/@revision")
        assert revision9 == revision7

        with pytest.raises(YtError): set("//tmp/d1/f/g/h", ["q", "r", "s"])
        revision10 = get("//tmp/d1/@revision")
        assert revision10 == revision7

        with pytest.raises(YtError): set("//tmp/d1/@value/f/g/h", ["q", "r", "s"])
        revision11 = get("//tmp/d1/@revision")
        assert revision11 == revision7

    def test_node_path_map(self):
        set("//tmp/a", 123)
        assert get("//tmp/a/@path") == "//tmp/a"

    def test_node_path_list(self):
        set("//tmp/a", [1, 2, 3])
        assert get("//tmp/a/1/@path") == "//tmp/a/1"

    def test_node_path_map_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", 123, tx=tx)
        assert get("//tmp/a/@path", tx=tx) == "//tmp/a"

    def test_node_path_list_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", [1, 2, 3], tx=tx)
        assert get("//tmp/a/1/@path", tx=tx) == "//tmp/a/1"

    def test_broken_node_path1(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        lock("//tmp/a", tx=tx, mode="snapshot")
        remove("//tmp/a")
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    def test_broken_node_path2(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        remove("//tmp/a", tx=tx)
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    def test_inheritable_attributes1(self):
        # parent inheritance
        create("map_node", "//tmp/dir1")
        set("//tmp/dir1/@compression_codec", "lz4")
        create("table", "//tmp/dir1/t1")
        assert get("//tmp/dir1/t1/@compression_codec") == "lz4"

        # explicit attribute precedence
        create("table", "//tmp/dir1/t2", attributes={"compression_codec": "zstd_17"})
        assert get("//tmp/dir1/t2/@compression_codec") == "zstd_17"

        create("map_node", "//tmp/dir1/dir2")

        # ancestor inheritance
        create("table", "//tmp/dir1/dir2/t1")
        assert get("//tmp/dir1/dir2/t1/@compression_codec") == "lz4"

        # parent inheritance
        set("//tmp/dir1/dir2/@compression_codec", "zlib_6")
        create("table", "//tmp/dir1/dir2/t2")
        assert get("//tmp/dir1/dir2/t2/@compression_codec") == "zlib_6"

    def test_inheritable_attributes2(self):
        create_medium("hdd")
        create_tablet_cell_bundle("b")

        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir1/dir2")

        # inherited from parent
        set("//tmp/dir1/@compression_codec", "lz4")
        set("//tmp/dir1/@replication_factor", 5)
        set("//tmp/dir1/@commit_ordering", "strong")

        # inherited from grandparent
        set("//tmp/dir1/dir2/@erasure_codec", "reed_solomon_6_3")
        set("//tmp/dir1/dir2/@vital", False)
        set("//tmp/dir1/dir2/@in_memory_mode", "uncompressed")

        # overridden explicitly
        primary_medium = "hdd"
        tablet_cell_bundle = "b"
        optimize_for = "scan"

        # left default: "media", "atomicity"

        create("table", "//tmp/dir1/dir2/t1", attributes={"primary_medium": primary_medium, "tablet_cell_bundle": tablet_cell_bundle, "optimize_for": optimize_for})
        assert get("//tmp/dir1/dir2/t1/@compression_codec") == "lz4"
        assert get("//tmp/dir1/dir2/t1/@replication_factor") == 5
        assert get("//tmp/dir1/dir2/t1/@commit_ordering") == "strong"
        assert get("//tmp/dir1/dir2/t1/@erasure_codec") == "reed_solomon_6_3"
        assert get("//tmp/dir1/dir2/t1/@vital") == False
        assert get("//tmp/dir1/dir2/t1/@in_memory_mode") == "uncompressed"
        assert get("//tmp/dir1/dir2/t1/@primary_medium") == "hdd"
        assert get("//tmp/dir1/dir2/t1/@tablet_cell_bundle") == "b"
        assert get("//tmp/dir1/dir2/t1/@optimize_for") == "scan"
        assert get("//tmp/dir1/dir2/t1/@media") == {"hdd": {"replication_factor": 5, "data_parts_only": False}}
        assert get("//tmp/dir1/dir2/t1/@atomicity") == "full"

    def test_inheritable_attributes_with_transactions(self):
        create("map_node", "//tmp/dir1")
        tx = start_transaction()

        # Inheritable attributes cannot be changed within trasactions.
        with pytest.raises(YtError):
            set("//tmp/dir1/@compression_codec", "lz4", tx=tx)

        set("//tmp/dir1/@compression_codec", "lz4")

        create("table", "//tmp/dir1/t1", tx=tx)
        assert get("//tmp/dir1/t1/@compression_codec", tx=tx) == "lz4"

    def test_inheritable_attributes_media_validation(self):
        create_medium("ssd")

        create("map_node", "//tmp/dir1")
        assert not exists("//tmp/dir1/@media")

        # media - primary_medium - replication_factor
        with pytest.raises(YtError):
            set("//tmp/dir1/@media", {"default": {"replication_factor": 0, "data_parts_only": False}})
        with pytest.raises(YtError):
            set("//tmp/dir1/@media", {"default": {"replication_factor": 3, "data_parts_only": True}})
        set("//tmp/dir1/@media", {"default": {"replication_factor": 3, "data_parts_only": False}})
        assert get("//tmp/dir1/@media") == {"default": {"replication_factor": 3, "data_parts_only": False}}
        with pytest.raises(YtError):
            set("//tmp/dir1/@primary_medium", "ssd")
        assert not exists("//tmp/dir1/@primary_medium")
        set("//tmp/dir1/@primary_medium", "default")
        assert get("//tmp/dir1/@primary_medium") == "default"
        with pytest.raises(YtError):
            set("//tmp/dir1/@replication_factor", 5)
        assert not exists("//tmp/dir1/@replication_factor")
        set("//tmp/dir1/@replication_factor", 3)
        assert get("//tmp/dir1/@replication_factor") == 3

        # media - replication_factor - primary_medium
        create("map_node", "//tmp/dir2")
        set("//tmp/dir2/@media", {"default": {"replication_factor": 3, "data_parts_only": False}, "ssd": {"replication_factor": 5, "data_parts_only": False}})
        assert get("//tmp/dir2/@media") == {"default": {"replication_factor": 3, "data_parts_only": False}, "ssd": {"replication_factor": 5, "data_parts_only": False}}
        set("//tmp/dir2/@replication_factor", 5)
        assert get("//tmp/dir2/@replication_factor") == 5
        with pytest.raises(YtError):
            set("//tmp/dir2/@primary_medium", "default")
        set("//tmp/dir2/@primary_medium", "ssd")
        assert get("//tmp/dir2/@primary_medium") == "ssd"

        # primary_medium - media - replication_factor
        create("map_node", "//tmp/dir3")
        with pytest.raises(YtError):
            set("//tmp/dir3/@primary_medium", "non_existent")
        set("//tmp/dir3/@primary_medium", "ssd")
        assert get("//tmp/dir3/@primary_medium") == "ssd"
        with pytest.raises(YtError):
            set("//tmp/dir3/@media", {"default": {"replication_factor": 3, "data_parts_only": True}, "ssd": {"replication_factor": 3, "data_parts_only": True}})
        set("//tmp/dir3/@media", {"default": {"replication_factor": 5, "data_parts_only": True}, "ssd": {"replication_factor": 3, "data_parts_only": False}})
        assert get("//tmp/dir3/@media") == {"default": {"replication_factor": 5, "data_parts_only": True}, "ssd": {"replication_factor": 3, "data_parts_only": False}}
        with pytest.raises(YtError):
            set("//tmp/dir3/@replication_factor", 5)
        set("//tmp/dir3/@replication_factor", 3)
        assert get("//tmp/dir3/@replication_factor") == 3

        # primary_medium - replication_factor - media
        # and
        # replication_factor - primary_medium - media
        # are pretty much the same
        create("map_node", "//tmp/dir4")
        set("//tmp/dir4/@primary_medium", "ssd")
        with pytest.raises(YtError):
            set("//tmp/dir4/@replication_factor", 0)
        set("//tmp/dir4/@replication_factor", 3)
        with pytest.raises(YtError):
            set("//tmp/dir4/@media", {"default": {"replication_factor": 3, "data_parts_only": False}})
        with pytest.raises(YtError):
            set("//tmp/dir4/@media", {"default": {"replication_factor": 3, "data_parts_only": False}, "ssd": {"replication_factor": 2, "data_parts_only": False}})
        set("//tmp/dir4/@media", {"default": {"replication_factor": 3, "data_parts_only": False}, "ssd": {"replication_factor": 3, "data_parts_only": False}})

        # replication_factor - media - primary_medium
        create("map_node", "//tmp/dir5")
        set("//tmp/dir5/@replication_factor", 3)
        set("//tmp/dir5/@media", {"default": {"replication_factor": 2, "data_parts_only": False}, "ssd": {"replication_factor": 4, "data_parts_only": False}})
        with pytest.raises(YtError):
            set("//tmp/dir5/@primary_medium", "default")
        with pytest.raises(YtError):
            set("//tmp/dir5/@primary_medium", "ssd")
        set("//tmp/dir5/@media", {"default": {"replication_factor": 2, "data_parts_only": False}, "ssd": {"replication_factor": 3, "data_parts_only": False}})
        set("//tmp/dir5/@primary_medium", "ssd")

    def test_inheritable_attributes_tablet_cell_bundle(self):
        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir2")
        create("map_node", "//tmp/dir3")

        with pytest.raises(YtError):
            set("//tmp/dir1/@tablet_cell_bundle", "non_existent")

        create_tablet_cell_bundle("b1")
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 1

        set("//tmp/dir1/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir1/@tablet_cell_bundle") == "b1"
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 2

        set("//tmp/dir2/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir2/@tablet_cell_bundle") == "b1"
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 3

        set("//tmp/dir3/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir3/@tablet_cell_bundle") == "b1"
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 4

        create_tablet_cell_bundle("b2")

        tx = start_transaction()
        copy("//tmp/dir3", "//tmp/dir3_copy", tx=tx)

        assert get("//tmp/dir3_copy/@tablet_cell_bundle", tx=tx) == "b1"

        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 6 # +1 for cloned trunk, +1 for it branch
        with pytest.raises(YtError):
            set("//tmp/dir3_copy/@tablet_cell_bundle", "b2", tx=tx)
        with pytest.raises(YtError):
            remove("//tmp/dir3_copy/@tablet_cell_bundle", tx=tx)

        set("//tmp/dir1/@tablet_cell_bundle", "b2")
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 5

        remove("//tmp/dir2/@tablet_cell_bundle")
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 4

        abort_transaction(tx)
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 2

        move("//tmp/dir3", "//tmp/dir3_move")
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 2

        tx = start_transaction()
        move("//tmp/dir3_move", "//tmp/dir3", tx=tx)
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 5

        abort_transaction(tx)
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 2

        remove("//tmp/dir3_move")
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b1/@ref_counter") == 1

    def test_create_invalid_type(self):
        with pytest.raises(YtError):
            create("some_invalid_type", "//tmp/s")
        with pytest.raises(YtError):
            create("sorted_dynamic_tablet_store", "//tmp/s")

##################################################################

class TestCypressMulticell(TestCypress):
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_zero_external_cell_bias(self):
        # Unfortunately, it's difficult to actually check anything here.
        create("table", "//tmp/t", attributes={"external_cell_bias": 0.0})
        assert not exists("//tmp/t/@external_cell_bias")

##################################################################

class TestCypressWithoutSet(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "forbid_set_command": True,
        }
    }

    def test_map(self):
        with pytest.raises(YtError):
            set("//tmp/dir", {})

        create("map_node", "//tmp/dir")
        with pytest.raises(YtError):
            set("//tmp/dir", {})

    def test_attrs(self):
        create("map_node", "//tmp/dir")
        set("//tmp/dir/@my_attr", 10)
        assert get("//tmp/dir/@my_attr") == 10

        set("//tmp/dir/@acl/end", {"action": "allow", "subjects": ["root"], "permissions": ["write"]})
        assert len(get("//tmp/dir/@acl")) == 1

    def test_document(self):
        create("document", "//tmp/doc")
        set("//tmp/doc", {})
        set("//tmp/doc/value", 10)
        assert get("//tmp/doc/value") == 10

    def test_list(self):
        create("list_node", "//tmp/list")
        set("//tmp/list/end", 0)
        with pytest.raises(YtError):
            set("//tmp/list/0", 1)
        set("//tmp/list/end", 2)
        assert get("//tmp/list") == [0, 2]

    def test_scalars(self):
        with pytest.raises(YtError):
            set("//tmp/integer", 10)
        create("int64_node", "//tmp/integer")
        with pytest.raises(YtError):
            set("//tmp/integer", 20)
        remove("//tmp/integer")

        create("document", "//tmp/doc")
        set("//tmp/doc", 10)
        assert get("//tmp/doc") == 10

