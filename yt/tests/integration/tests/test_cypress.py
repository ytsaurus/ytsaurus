import pytest
import time
import datetime
from string import printable
from copy import deepcopy
from cStringIO import StringIO
from datetime import timedelta
from dateutil.tz import tzlocal

from yt_driver_bindings import Driver
from yt.yson import to_yson_type, YsonEntity
from yt.environment.helpers import assert_items_equal

from yt_env_setup import YTEnvSetup
from yt_commands import *

import __builtin__

##################################################################

class TestCypress(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    @authors("babenko")
    def test_root(self):
        # should not crash
        get("//@")

    @authors("panin", "ignat")
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

    @authors("ignat")
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

    @authors("ignat")
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

        assert exists("//tmp/list/0")
        assert exists("//tmp/list/6")
        assert exists("//tmp/list/-1")
        assert exists("//tmp/list/-7")
        assert not exists("//tmp/list/42")
        assert not exists("//tmp/list/-42")
        with pytest.raises(YtError):
            get("//tmp/list/42")
            get("//tmp/list/-42")

    @authors("babenko", "ignat")
    def test_list_command(self):
        set("//tmp/map", {"a": 1, "b": 2, "c": 3})
        assert ls("//tmp/map") == ["a", "b", "c"]

        set("//tmp/map", {"a": 1, "a": 2})
        assert ls("//tmp/map", max_size=1) == ["a"]

        ls("//sys/chunks")
        ls("//sys/accounts")
        ls("//sys/transactions")

    @authors("ignat")
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

    @authors("ignat")
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

    @authors("babenko", "ignat")
    def test_attributes_tx_read_table(self):
        set("//tmp/t", "<attr=100> 123", is_raw=True)
        assert get("//tmp/t") == 123
        assert get("//tmp/t/@attr") == 100
        assert "attr" in get("//tmp/t/@")

        tx = start_transaction()
        assert get("//tmp/t/@attr", tx = tx) == 100
        assert "attr" in get("//tmp/t/@", tx = tx)

    @authors("shakurov")
    def test_attributes_yt_11973(self):
        create("table", "//tmp/test_node")

        set("//tmp/test_node/@opaque", True)
        set("//tmp/test_node/@expiration_time", "2100-01-01T00:00:00.000000Z")
        set("//tmp/test_node/@desired_tablet_count", 42)
        assert get("//tmp/test_node/@opaque")
        assert get("//tmp/test_node/@expiration_time") == "2100-01-01T00:00:00.000000Z"
        assert get("//tmp/test_node/@desired_tablet_count") == 42

        # Mustn't throw.
        set("//tmp/test_node/@", {"foo": "bar", "baz": 1})

        assert not get("//tmp/test_node/@opaque")
        assert not exists("//tmp/test_node/@expiration_time")
        assert not exists("//tmp/test_node/@desired_tablet_count")

    @authors("panin", "ignat")
    def test_format_json(self):
        # check input format for json
        set("//tmp/json_in", '{"list": [1,2,{"string": "this"}]}', is_raw=True, input_format="json")
        assert get("//tmp/json_in") == {"list": [1, 2, {"string": "this"}]}

        # check output format for json
        set("//tmp/json_out", {"list": [1, 2, {"string": "this"}]})
        assert get("//tmp/json_out", is_raw=True, output_format="json") == '{"list":[1,2,{"string":"this"}]}'

    @authors("ignat")
    def test_map_remove_all1(self):
        # remove items from map
        set("//tmp/map", {"a" : "b", "c": "d"})
        assert get("//tmp/map/@count") == 2
        remove("//tmp/map/*")
        assert get("//tmp/map") == {}
        assert get("//tmp/map/@count") == 0

    @authors("babenko", "ignat")
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

    @authors("aleksandra-zh")
    def test_ref_count(self):
        create("map_node", "//tmp/d")
        assert get("//tmp/d/@ref_counter") == 1

        tx = start_transaction()
        create("table", "//tmp/d/t", tx=tx)
        assert get("//tmp/d/@ref_counter") == 2
        abort_transaction(tx)
        assert get("//tmp/d/@ref_counter") == 1

        tx = start_transaction()
        lock("//tmp/d", tx=tx, mode="snapshot")
        assert get("//tmp/d/@ref_counter") == 2
        commit_transaction(tx)
        assert get("//tmp/d/@ref_counter") == 1

    @authors("aleksandra-zh")
    def test_ref_count_move(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@ref_counter") == 1

        tx = start_transaction()
        move("//tmp/t", "//tmp/t1", tx=tx)
        assert get("//tmp/t/@ref_counter") == 2
        abort_transaction(tx)
        assert get("//tmp/t/@ref_counter") == 1

        tx = start_transaction()
        move("//tmp/t", "//tmp/t1", tx=tx)
        assert get("//tmp/t/@ref_counter") == 2
        commit_transaction(tx)
        # should not crash
        with pytest.raises(YtError): get("//tmp/t/@ref_counter")
        assert get("//tmp/t1/@ref_counter") == 1

    @authors("ignat")
    def test_list_remove_all(self):
        # remove items from list
        set("//tmp/list", [10, 20, 30])
        assert get("//tmp/list/@count") == 3
        remove("//tmp/list/*")
        assert get("//tmp/list") == []
        assert get("//tmp/list/@count") == 0

    @authors("ignat")
    def test_attr_remove_all1(self):
        # remove items from attributes
        set("//tmp/attr", "<_foo=bar;_key=value>42", is_raw=True)
        remove("//tmp/attr/@*")
        with pytest.raises(YtError): get("//tmp/attr/@_foo")
        with pytest.raises(YtError): get("//tmp/attr/@_key")

    @authors("babenko", "ignat")
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

    @authors("babenko", "ignat")
    def test_copy_simple1(self):
        set("//tmp/a", 1)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == 1

    @authors("babenko", "ignat")
    def test_copy_simple2(self):
        set("//tmp/a", [1, 2, 3])
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == [1, 2, 3]

    @authors("babenko", "ignat")
    def test_copy_simple3(self):
        set("//tmp/a", "<x=y> 1", is_raw=True)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@x") == "y"

    @authors("babenko", "ignat")
    def test_copy_simple4(self):
        set("//tmp/a", {"x1" : "y1", "x2" : "y2"})
        assert get("//tmp/a/@count") == 2

        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@count") == 2

    @authors("babenko", "ignat")
    def test_copy_simple5(self):
        set("//tmp/a", { "b" : 1 })
        assert get("//tmp/a/b/@path") == "//tmp/a/b"

        copy("//tmp/a", "//tmp/c")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

        remove("//tmp/a")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

    @authors("babenko", "ignat")
    def test_copy_simple6a(self):
        with pytest.raises(YtError): copy("//tmp", "//tmp/a")

    @authors("babenko")
    def test_copy_simple6b(self):
        tx = start_transaction()
        create("map_node", "//tmp/a", tx=tx)
        create("map_node", "//tmp/a/b", tx=tx)
        with pytest.raises(YtError): copy("//tmp/a", "//tmp/a/b/c", tx=tx)

    @authors("babenko")
    def test_copy_simple7(self):
        tx = start_transaction()
        with pytest.raises(YtError): copy("#" + tx, "//tmp/t")

    @authors("babenko", "ignat")
    def test_copy_simple8(self):
        create("map_node", "//tmp/a")
        create("table", "//tmp/a/t")
        link("//tmp/a", "//tmp/b")
        copy("//tmp/b/t", "//tmp/t")

    @authors("babenko")
    def test_copy_recursive_success(self):
        create("map_node", "//tmp/a")
        copy("//tmp/a", "//tmp/b/c", recursive=True)

    @authors("babenko", "ignat")
    def test_copy_recursive_fail(self):
        create("map_node", "//tmp/a")
        with pytest.raises(YtError):
            copy("//tmp/a", "//tmp/b/c", recursive=False)

        with pytest.raises(YtError):
            copy("//tmp/a", "//tmp/b/c/d/@e", recursive=True)
        assert not exists("//tmp/b/c/d")

    @authors("babenko", "ignat")
    def test_copy_tx1(self):
        tx = start_transaction()

        set("//tmp/a", {"x1" : "y1", "x2" : "y2"}, tx=tx)
        assert get("//tmp/a/@count", tx=tx) == 2

        copy("//tmp/a", "//tmp/b", tx=tx)
        assert get("//tmp/b/@count", tx=tx) == 2

        commit_transaction(tx)

        assert get("//tmp/a/@count") == 2
        assert get("//tmp/b/@count") == 2

    @authors("babenko", "ignat")
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

    @authors("babenko", "ignat")
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

    @authors("babenko", "ignat")
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

    @authors("babenko", "ignat")
    def test_copy_unexisting_path(self):
        with pytest.raises(YtError): copy("//tmp/x", "//tmp/y")

    @authors("babenko", "ignat")
    def test_copy_cannot_have_children(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        with pytest.raises(YtError): copy("//tmp/t2", "//tmp/t1/xxx")

    @authors("ignat")
    def test_copy_table_compression_codec(self):
        create("table", "//tmp/t1")
        assert get("//tmp/t1/@compression_codec") == "lz4"
        set("//tmp/t1/@compression_codec", "zlib_6")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@compression_codec") == "zlib_6"

    @authors("levysotsky")
    def test_copy_ignore_existing(self):
        create("map_node", "//tmp/a")
        create("map_node", "//tmp/b/c", recursive=True)

        copy("//tmp/b", "//tmp/a", ignore_existing=True)
        assert not exists("//tmp/a/c")

        copy("//tmp/b", "//tmp/a", force=True)
        assert exists("//tmp/a/c")

        with pytest.raises(YtError): copy("//tmp/b", "//tmp/a")
        # Two options simultaneously are forbidden.
        with pytest.raises(YtError): copy("//tmp/b", "//tmp/new", ignore_existing=True, force=True)

    @authors("babenko")
    def test_copy_removed_account(self):
        create_account("a")
        create("map_node", "//tmp/p1")
        create("map_node", "//tmp/p2")

        create("file", "//tmp/p1/f", attributes={"account": "a"})

        remove("//sys/accounts/a")
        wait(lambda: get("//sys/accounts/a/@life_stage") in ["removal_started", "removal_pre_committed"])

        with pytest.raises(YtError):
            copy("//tmp/p1/f", "//tmp/p2/f", preserve_account=True)

        remove("//tmp/p1/f")
        wait(lambda: not exists("//sys/accounts/a"))

    @authors("babenko")
    def test_copy_removed_bundle(self):
        create_tablet_cell_bundle("b")
        create("map_node", "//tmp/p1")
        create("map_node", "//tmp/p2")

        create("table", "//tmp/p1/t", attributes={"tablet_cell_bundle": "b"})

        remove("//sys/tablet_cell_bundles/b")
        wait(lambda: get("//sys/tablet_cell_bundles/b/@life_stage") in ["removal_started", "removal_pre_committed"])

        with pytest.raises(YtError):
            copy("//tmp/p1/t", "//tmp/p2/t")

        remove("//tmp/p1/t")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b"))

    @authors("babenko")
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

    @authors("babenko", "ignat")
    def test_copy_id1(self):
        set("//tmp/a", 123)
        a_id = get("//tmp/a/@id")
        copy("#" + a_id, "//tmp/b")
        assert get("//tmp/b") == 123

    @authors("babenko", "ignat")
    def test_copy_id2(self):
        set("//tmp/a", 123)
        tmp_id = get("//tmp/@id")
        copy("#" + tmp_id + "/a", "//tmp/b")
        assert get("//tmp/b") == 123

    @authors("babenko", "ignat")
    def test_copy_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@account") == "tmp"

    @authors("babenko", "ignat")
    def test_copy_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    @authors("babenko")
    def test_copy_force1(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@a", 1)
        create("table", "//tmp/t2")
        set("//tmp/t2/@a", 2)
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@a", 1)

    @authors("babenko")
    def test_copy_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2", force=True)

    @authors("babenko")
    def test_copy_force3(self):
        create("table", "//tmp/t1")
        set("//tmp/t2", {})
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@type") == "table"

    @authors("babenko")
    def test_copy_force_account1(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=True)

        assert get("//tmp/a2/@account") == "a"

    @authors("babenko")
    def test_copy_force_account2(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=False)

        assert get("//tmp/a2/@account") == "tmp"

    @authors("babenko")
    def test_copy_locked(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        lock("//tmp/t1", tx=tx)
        copy("//tmp/t1", "//tmp/t2")
        commit_transaction(tx)

    @authors("babenko")
    def test_copy_acd(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), [])

    @authors("babenko")
    def test_move_acd(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        move("//tmp/t1", "//tmp/t2")
        assert not get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), acl)


    @authors("ignat")
    def test_move_simple1(self):
        set("//tmp/a", 1)
        move("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == 1
        with pytest.raises(YtError): get("//tmp/a")

    @authors("babenko", "ignat")
    def test_move_simple2(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        lock("//tmp/a", tx=tx)

        with pytest.raises(YtError): move("//tmp/a", "//tmp/b")
        assert not exists("//tmp/b")

    @authors("babenko", "ignat")
    def test_move_simple3(self):
        with pytest.raises(YtError): move("//tmp", "//tmp/a")

    @authors("babenko", "ignat")
    def test_move_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        move("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    @authors("babenko", "ignat")
    def test_move_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        move("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@account") == "tmp"

    @authors("babenko")
    def test_move_recursive_success(self):
        create("map_node", "//tmp/a")
        move("//tmp/a", "//tmp/b/c", recursive=True)

    @authors("babenko", "ignat")
    def test_move_recursive_fail(self):
        create("map_node", "//tmp/a")
        with pytest.raises(YtError):
            move("//tmp/a", "//tmp/b/c", recursive=False)

        with pytest.raises(YtError):
            move("//tmp/a", "//tmp/b/c/d/@e", recursive=True)
        assert not exists("//tmp/b/c/d")

    @authors("babenko")
    def test_move_force1(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@a", 1)
        create("table", "//tmp/t2")
        set("//tmp/t2/@a", 2)
        move("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@a", 1)
        assert not exists("//tmp/t1")

    @authors("babenko")
    def test_move_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2", force=True)
        assert exists("//tmp/t1")

    @authors("babenko")
    def test_move_force3(self):
        create("table", "//tmp/t1")
        set("//tmp/t2", {})
        move("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@type") == "table"
        assert not exists("//tmp/t1")

    @authors("babenko")
    def test_move_force4(self):
        with pytest.raises(YtError): copy("//tmp", "/", force=True)

    @authors("babenko")
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

    @authors("babenko")
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

    @authors("babenko")
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

    @authors("babenko")
    def test_move_tx_locking1(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        tx2 = start_transaction()
        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t3", tx=tx2)

    @authors("babenko")
    def test_move_tx_locking2(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx1)
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        move("//tmp/t2", "//tmp/t3", tx=tx2)
        with pytest.raises(YtError): move("//tmp/t2", "//tmp/t4", tx=tx3)

    @authors("ignat")
    def test_embedded_attributes(self):
        set("//tmp/a", {})
        set("//tmp/a/@attr", {"key": "value"})
        set("//tmp/a/@attr/key/@embedded_attr", "emb")
        assert get("//tmp/a/@attr") == {"key": to_yson_type("value", attributes={"embedded_attr": "emb"})}
        assert get("//tmp/a/@attr/key") == to_yson_type("value", attributes={"embedded_attr": "emb"})
        assert get("//tmp/a/@attr/key/@embedded_attr") == "emb"

    @authors("babenko")
    def test_get_with_attributes(self):
        set("//tmp/a/b", {}, recursive=True, force=True)
        assert get("//tmp/a", attributes=["type"]) == to_yson_type({"b": to_yson_type({}, {"type": "map_node"})}, {"type": "map_node"})

    @authors("babenko")
    def test_list_with_attributes(self):
        set("//tmp/a/b", {}, recursive=True, force=True)
        assert ls("//tmp/a", attributes=["type"]) == [to_yson_type("b", attributes={"type": "map_node"})]

    @authors("kiselyovp")
    def test_get_with_attributes_objects(self):
        assert get("//sys/accounts/tmp", attributes=["name"]) == to_yson_type({}, {"name" : "tmp"})
        assert get("//sys/users/root", attributes=["name", "type"]) == to_yson_type(None, {"name" : "root", "type" : "user"})

    @authors("babenko")
    def test_get_with_attributes_virtual_maps(self):
        tx = start_transaction()
        assert get("//sys/transactions", attributes=["type"]) == to_yson_type( \
            {tx: to_yson_type(None, attributes={"type": "transaction"})},
            attributes={"type": "transaction_map"})

    @authors("babenko")
    def test_move_virtual_maps1(self):
        create("tablet_map", "//tmp/t")
        move("//tmp/t", "//tmp/tt")

    @authors("babenko")
    def test_move_virtual_maps2(self):
        create("chunk_map", "//tmp/c")
        move("//tmp/c", "//tmp/cc")

    @authors("babenko")
    def test_list_with_attributes_virtual_maps(self):
        tx = start_transaction()
        assert ls("//sys/transactions", attributes=["type"]) == [to_yson_type(tx, attributes={"type": "transaction"})]

    @authors("aleksandra-zh")
    def test_map_node_branch(self):
        create("map_node", "//tmp/m")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        create("table", "//tmp/m/t", tx=tx1)
        lock("//tmp/m", tx=tx2, mode="snapshot")
        assert get("//tmp/m/t/@key", tx=tx2) == "t"
        remove("//tmp/m/t", tx=tx1)
        assert get("//tmp/m/t/@key", tx=tx2) == "t"

    @authors("babenko", "ignat")
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

    @authors("babenko", "ignat")
    def test_remove_tx1(self):
        set("//tmp/a", 1)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")
        tx = start_transaction()
        remove("//tmp/a", tx=tx)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")
        abort_transaction(tx)
        assert get("//tmp/@id") == get("//tmp/a/@parent_id")

    @authors("ignat")
    def test_create(self):
        create("map_node", "//tmp/some_node")

    @authors("babenko")
    def test_create_recursive_fail(self):
        create("map_node", "//tmp/some_node")
        with pytest.raises(YtError): create("map_node", "//tmp/a/b")

    @authors("babenko", "ignat")
    def test_create_recursive_success(self):
        create("map_node", "//tmp/a/b", recursive=True)

    @authors("babenko", "ignat")
    def test_create_ignore_existing_success(self):
        create("map_node", "//tmp/a/b", recursive=True)
        create("map_node", "//tmp/a/b", ignore_existing=True)

    @authors("babenko")
    def test_create_ignore_existing_fail(self):
        create("map_node", "//tmp/a/b", recursive=True)
        with pytest.raises(YtError): create("table", "//tmp/a/b", ignore_existing=True)

    @authors("babenko")
    def test_create_ignore_existing_force_fail(self):
        with pytest.raises(YtError): create("table", "//tmp/t", ignore_existing=True, force=True)

    @authors("gritukan")
    def test_create_ignore_type_mismatch(self):
        create("map_node", "//tmp/a/b", recursive=True)
        create("map_node", "//tmp/a/b", ignore_existing=True, ignore_type_mismatch=True)
        create("table", "//tmp/a/b", ignore_existing=True, ignore_type_mismatch=True)
        assert get("//tmp/a/b/@type") == "map_node"

    @authors("gritukan")
    def test_create_ignore_type_mismatch_without_ignore_existing_fail(self):
        create("map_node", "//tmp/a/b", recursive=True)
        with pytest.raises(YtError): create("map_node", "//tmp/a/b", ignore_type_mismatch=True)

    @authors("babenko")
    def test_create_force(self):
        id1 = create("table", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id1
        id2 = create("table", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id2
        id3 = create("file", "//tmp/t", force=True)
        assert get("//tmp/t/@id") == id3

    @authors("ignat")
    def test_create_recursive(self):
        assert not exists("//tmp/a/b/c/d")
        with pytest.raises(YtError):
            create("map_node", "//tmp/a/b/c/d/@d", recursive=True)
        assert not exists("//tmp/a/b/c/d")
        create("map_node", "//tmp/a/b/c/d", recursive=True)
        assert exists("//tmp/a/b/c/d")

    @authors("kiselyovp")
    def test_create_object_ignore_existing(self):
        with pytest.raises(YtError): create_user("u", ignore_existing=True)
        with pytest.raises(YtError): create_group("g", ignore_existing=True)
        create_user("u")
        with pytest.raises(YtError): create_user("u", ignore_existing=True)

    @authors("babenko")
    def test_link1(self):
        link("//tmp/a", "//tmp/b")
        assert get("//tmp/b&/@broken")

    @authors("babenko")
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

    @authors("babenko")
    def test_link3(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        remove("//tmp/t1")
        assert get("//tmp/t2&/@broken")

    @authors("babenko")
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

    @authors("babenko")
    def test_link5(self):
        set("//tmp/t1", 1)
        set("//tmp/t2", 2)
        with pytest.raises(YtError): link("//tmp/t1", "//tmp/t2")

    @authors("babenko")
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

    @authors("ignat")
    def test_link7(self):
        tx = start_transaction()
        set("//tmp/t1", 1, tx=tx)
        link("//tmp/t1", "//tmp/l1", tx=tx)
        assert get("//tmp/l1", tx=tx) == 1

    @authors("babenko")
    def test_link_existing_fail(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        assert get("//tmp/l/@id") == id1
        with pytest.raises(YtError): link("//tmp/t2", "//tmp/l")

    @authors("babenko")
    def test_link_ignore_existing(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", ignore_existing=True)
        assert get("//tmp/l/@id") == id1

    @authors("babenko")
    def test_link_force1(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/t1/@id") == id1
        assert get("//tmp/l/@id") == id2

    @authors("babenko")
    def test_link_force2(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        remove("//tmp/t1")
        assert get("//tmp/l&/@broken")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@id") == id2

    @authors("babenko")
    def test_link_ignore_existing_force_fail(self):
        id1 = create("table", "//tmp/t")
        with pytest.raises(YtError): link("//tmp/t", "//tmp/l", ignore_existing=True, force=True)

    @authors("babenko")
    def test_link_to_link(self):
        id = create("table", "//tmp/t")
        link("//tmp/t", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        assert get("//tmp/l2/@id") == id
        assert not get("//tmp/l2&/@broken")
        remove("//tmp/l1")
        assert get("//tmp/l2&/@broken")

    @authors("babenko")
    def test_link_as_copy_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError): copy("//tmp/t2", "//tmp/l")

    @authors("babenko")
    def test_link_as_copy_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        copy("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert get("//tmp/t1/@id") == id1

    @authors("babenko")
    def test_link_as_move_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError): move("//tmp/t2", "//tmp/l")

    @authors("babenko")
    def test_link_as_copy_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        move("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert not exists("//tmp/t2")
        assert get("//tmp/t1/@id") == id1

    @authors("babenko")
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

    @authors("babenko")
    def test_resolve_suppress_via_object_id_yt_6694(self):
        create("map_node", "//tmp/a")
        link("//tmp/a", "//tmp/b")
        id = get("//tmp/b&/@id")
        assert get("//tmp/b/@type") == "map_node"
        assert get("//tmp/b&/@type") == "link"
        assert get("#{0}/@type".format(id)) == "map_node"
        assert get("#{0}&/@type".format(id)) == "link"

    @authors("kiselyovp")
    def test_escaped_symbols(self):
        with pytest.raises(YtError):
            create("map_node", "//tmp/special@&*[{symbols")
        path = r"//tmp/special\\\/\@\&\*\[\{symbols"
        create("string_node", path)
        set(path, "abacaba")
        assert exists(path)
        assert r"special\/@&*[{symbols" in ls("//tmp")
        assert get(path) == "abacaba"
        set(path + "/@attr", 42)
        assert get(path + "/@attr") == 42

        move(path, "//tmp/string_node")
        assert not exists(path)
        assert exists("//tmp/string_node")

    @authors("babenko")
    def test_access_stat1(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c2 == c1

    @authors("babenko", "ignat")
    def test_access_stat2(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        tx = start_transaction()
        lock("//tmp/d", mode = "snapshot", tx = tx)
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter", tx = tx)
        assert c2 == c1 + 1

    @authors("babenko", "ignat")
    def test_access_stat3(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        get("//tmp/d/@")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat4(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        assert exists("//tmp/d")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat5(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        assert exists("//tmp/d/@id")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat6(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        ls("//tmp/d/@")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat7(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        ls("//tmp/d")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c2 == c1 + 1

    @authors("babenko", "ignat")
    def test_access_stat8(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@access_time") == get("//tmp/t/@creation_time")

    @authors("babenko", "ignat")
    def test_access_stat9(self):
        create("table", "//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@access_time") == get("//tmp/t2/@creation_time")

    @authors("babenko", "ignat")
    def test_access_stat_suppress1(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        get("//tmp/d", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat_suppress2(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        ls("//tmp/d", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat_suppress3(self):
        time.sleep(1)
        create("table", "//tmp/t")
        c1 = get("//tmp/t/@access_counter")
        read_table("//tmp/t", table_reader={"suppress_access_tracking": True})
        time.sleep(1)
        c2 = get("//tmp/t/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    def test_access_stat_suppress4(self):
        time.sleep(1)
        create("file", "//tmp/f")
        c1 = get("//tmp/f/@access_counter")
        read_file("//tmp/f", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/f/@access_counter")
        assert c1 == c2

    @authors("babenko")
    def test_modification_suppress1(self):
        create("map_node", "//tmp/m")
        time.sleep(1)
        time1 = get("//tmp/m/@modification_time")
        set("//tmp/m/@x", 1, suppress_modification_tracking=True)
        time.sleep(1)
        time2 = get("//tmp/m/@modification_time")
        assert time1 == time2

    @authors("babenko", "ignat")
    def test_chunk_maps(self):
        gc_collect()
        assert get("//sys/chunks/@count") == 0
        assert get("//sys/underreplicated_chunks/@count") == 0
        assert get("//sys/overreplicated_chunks/@count") == 0

    @authors("ignat")
    def test_list_attributes(self):
        create("map_node", "//tmp/map", attributes={"user_attr1": 10})
        set("//tmp/map/@user_attr2", "abc")
        assert sorted(get("//tmp/map/@user_attribute_keys")) == sorted(["user_attr1", "user_attr2"])
        assert get("//tmp/map/@user_attributes") == {"user_attr1": 10, "user_attr2": "abc"}

        create("table", "//tmp/table")
        assert get("//tmp/table/@user_attribute_keys") == []
        assert get("//tmp/table/@user_attributes") == {}

        create("file", "//tmp/file")
        assert get("//tmp/file/@user_attribute_keys") == []
        assert get("//tmp/file/@user_attributes") == {}

    @authors("babenko")
    def test_opaque_attribute_keys(self):
        create("table", "//tmp/t")
        assert "compression_statistics" in get("//tmp/t/@opaque_attribute_keys")

    @authors("ignat")
    def test_boolean(self):
        yson_format = yson.loads("yson")
        set("//tmp/boolean", "%true", is_raw=True)
        assert get("//tmp/boolean/@type") == "boolean_node"
        assert get("//tmp/boolean", output_format=yson_format)

    @authors("lukyan")
    def test_uint64(self):
        yson_format = yson.loads("yson")
        set("//tmp/my_uint", "123456u", is_raw=True)
        assert get("//tmp/my_uint/@type") == "uint64_node"
        assert get("//tmp/my_uint", output_format=yson_format) == 123456

    @authors("babenko", "ignat")
    def test_map_node_children_limit(self):
        set("//sys/@config/cypress_manager/max_node_child_count", 100)
        create("map_node", "//tmp/test_node")
        for i in xrange(100):
            create("map_node", "//tmp/test_node/" + str(i))
        with pytest.raises(YtError):
            create("map_node", "//tmp/test_node/100")

    @authors("babenko")
    def test_string_node_length_limit(self):
        set("//sys/@config/cypress_manager/max_string_node_length", 300)
        set("//tmp/test_node", "x" * 300)
        remove("//tmp/test_node")

        with pytest.raises(YtError):
            set("//tmp/test_node", "x" * 301)

        with pytest.raises(YtError):
            set("//tmp/test_node", {"key": "x" * 301})

        with pytest.raises(YtError):
            set("//tmp/test_node", ["x" * 301])

    @authors("babenko")
    def test_attribute_size_limit(self):
        set("//sys/@config/cypress_manager/max_attribute_size", 300)
        set("//tmp/test_node", {})

        # The limit is 300 but this is for binary YSON.
        set("//tmp/test_node/@test_attr", "x" * 290)

        with pytest.raises(YtError):
            # This must definitely exceed the limit of 300.
            set("//tmp/test_node/@test_attr", "x" * 301)

    @authors("babenko")
    def test_map_node_key_length_limits(self):
        set("//sys/@config/cypress_manager/max_map_node_key_length", 300)
        set("//tmp/" + "a" * 300, 0)
        with pytest.raises(YtError):
            set("//tmp/" + "a" * 301, 0)

    @authors("babenko")
    def test_invalid_external_cell_bias(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": -1.0})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": 2.0})


    def _now(self):
        return datetime.now(tzlocal())

    @authors("babenko")
    def test_expiration_time_validation(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@expiration_time", "hello")

    @authors("babenko")
    def test_expiration_time_change_requires_remove_permission_failure(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//tmp/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            set("//tmp/t/@expiration_time", str(self._now()), authenticated_user="u")

    @authors("babenko")
    def test_expiration_time_change_requires_recursive_remove_permission_failure(self):
        create_user("u")
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t")
        set("//tmp/m/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            set("//tmp/m/@expiration_time", str(self._now()), authenticated_user="u")

    @authors("babenko")
    def test_expiration_time_reset_requires_write_permission_success(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        set("//tmp/t/@acl", [
            make_ace("allow", "u", "write"),
            make_ace("deny", "u", "remove")])
        remove("//tmp/t/@expiration_time", authenticated_user="u")

    @authors("babenko")
    def test_expiration_time_reset_requires_write_permission_failure(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        set("//tmp/t/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            remove("//tmp/t/@expiration_time", authenticated_user="u")

    @authors("babenko")
    def test_expiration_time_change(self):
        create("table", "//tmp/t", attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"})
        assert get("//tmp/t/@expiration_time") == "2030-04-03T21:25:29.000000Z"
        remove("//tmp/t/@expiration_time")
        assert not exists("//tmp/t/@expiration_time")

    @authors("babenko")
    def test_expiration_time_can_be_set_upon_construction1(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())}, authenticated_user="u")
        wait(lambda: not exists("//tmp/t"))

    @authors("babenko")
    def test_expiration_time_can_be_set_upon_construction2(self):
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now() + timedelta(seconds=10.0))})
        time.sleep(1)
        assert exists("//tmp/t")

    @authors("babenko")
    def test_expiration_time_can_be_set_upon_construction3(self):
        tx = start_transaction()
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())}, tx=tx)
        time.sleep(1)
        assert not exists("//tmp/t")
        assert exists("//tmp/t", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    def test_expiration_time_removal(self):
        create("table", "//tmp/t", attributes={"expiration_time": str(self._now())})
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko", "shakurov")
    def test_expiration_time_lock_conflict(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx)
        with pytest.raises(YtError): set("//tmp/t/@expiration_time", str(self._now()))
        unlock("//tmp/t", tx=tx)
        set("//tmp/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    def test_expiration_time_wait_for_parent_locks_released(self):
        create("table", "//tmp/x/t", recursive=True)
        tx = start_transaction()
        lock("//tmp/x", tx=tx)
        set("//tmp/x/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert exists("//tmp/x/t")
        abort_transaction(tx)
        time.sleep(1)
        wait(lambda: not exists("//tmp/x/t"))

    @authors("babenko")
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

    @authors("babenko")
    def test_expiration_time_dont_wait_for_snapshot_locks(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx, mode="snapshot")
        set("//tmp/t/@expiration_time", str(self._now()))
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    def test_no_expiration_time_for_root(self):
        with pytest.raises(YtError): set("//@expiration_time", str(self._now()))

    @authors("shakurov")
    def test_expiration_time_versioning1(self):
        create("table", "//tmp/t1", attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"})

        tx = start_transaction()

        set("//tmp/t1/@expiration_time", "2031-03-07T13:18:55.000000Z", tx=tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert get("//tmp/t1/@expiration_time", tx=tx) == "2031-03-07T13:18:55.000000Z"

        commit_transaction(tx)

        assert get("//tmp/t1/@expiration_time") == "2031-03-07T13:18:55.000000Z"

    @authors("shakurov")
    def test_expiration_time_versioning2(self):
        create("table", "//tmp/t1")

        tx = start_transaction()

        set("//tmp/t1/@expiration_time", "2030-03-07T13:18:55.000000Z", tx=tx)

        assert get("//tmp/t1/@expiration_time", tx=tx) == "2030-03-07T13:18:55.000000Z"
        assert not exists("//tmp/t1/@expiration_time")

        commit_transaction(tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"

    @authors("shakurov")
    def test_expiration_time_versioning3(self):
        create("table", "//tmp/t1", attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"})

        tx = start_transaction()
        set("//tmp/t1/@expiration_time", "2031-03-07T13:18:55.000000Z", tx=tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert get("//tmp/t1/@expiration_time", tx=tx) == "2031-03-07T13:18:55.000000Z"

        remove("//tmp/t1/@expiration_time", tx=tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert not exists("//tmp/t1/@expiration_time", tx=tx)

        commit_transaction(tx)

        assert not exists("//tmp/t1/@expiration_time")

    @authors("shakurov")
    def test_expiration_time_versioning4(self):
        create("table", "//tmp/t1", attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"})

        tx1 = start_transaction()
        set("//tmp/t1/@expiration_time", "2031-03-07T13:18:55.000000Z", tx=tx1)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert get("//tmp/t1/@expiration_time", tx=tx1) == "2031-03-07T13:18:55.000000Z"

        remove("//tmp/t1/@expiration_time", tx=tx1)

        tx2 = start_transaction(tx=tx1)
        set("//tmp/t1/@expiration_time", "2032-03-07T13:18:55.000000Z", tx=tx2)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert not exists("//tmp/t1/@expiration_time", tx=tx1)
        assert get("//tmp/t1/@expiration_time", tx=tx2) == "2032-03-07T13:18:55.000000Z"

        remove("//tmp/t1/@expiration_time", tx=tx2)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert not exists("//tmp/t1/@expiration_time", tx=tx1)
        assert not exists("//tmp/t1/@expiration_time", tx=tx2)

        commit_transaction(tx2)
        commit_transaction(tx1)

        assert not exists("//tmp/t1/@expiration_time")

    @authors("shakurov")
    def test_expiration_time_versioning5(self):
        tx = start_transaction()
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now())}, tx=tx)
        time.sleep(1)
        assert exists("//tmp/t1", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t1")

    @authors("babenko")
    def test_copy_preserve_expiration_time(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True)
        assert exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")

    @authors("babenko")
    def test_copy_dont_preserve_expiration_time(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        copy("//tmp/t1", "//tmp/t2")
        assert not exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2")

    @authors("babenko")
    def test_copy_preserve_expiration_time_in_tx1(self):
        create("table", "//tmp/t1", attributes={"expiration_time": str(self._now() + timedelta(seconds=1))})
        tx = start_transaction()
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True, tx=tx)
        assert exists("//tmp/t2/@expiration_time", tx=tx)
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2", tx=tx)
        commit_transaction(tx)
        wait(lambda: not exists("//tmp/t2"))

    @authors("babenko")
    def test_copy_preserve_expiration_time_in_tx2(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        set("//tmp/t1/@expiration_time", str(self._now() + timedelta(seconds=1)), tx=tx)
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True, tx=tx)
        assert exists("//tmp/t1/@expiration_time", tx=tx)
        assert exists("//tmp/t2/@expiration_time", tx=tx)
        assert get("//tmp/t1/@expiration_time", tx=tx) == get("//tmp/t2/@expiration_time", tx=tx)
        time.sleep(2)
        assert exists("//tmp/t1")
        assert exists("//tmp/t1", tx=tx)
        assert exists("//tmp/t2", tx=tx)
        commit_transaction(tx)
        wait(lambda: not exists("//tmp/t1") and not exists("//tmp/t2"))

    @authors("babenko")
    def test_expire_orphaned_node_yt_8064(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        node_id = create("table", "//tmp/t", attributes={"expiration_time": str(self._now() + timedelta(seconds=2))}, tx=tx1)
        lock("#" + node_id, tx=tx2, mode="snapshot")
        abort_transaction(tx1)
        time.sleep(2)

    @authors("ignat")
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

    @authors("babenko")
    def test_ignore_ampersand1(self):
        set("//tmp/map", {})
        set("//tmp/map&/a", "b")
        assert get("//tmp/map&/a") == "b"
        assert get("//tmp/map&/@type") == "map_node"

    @authors("babenko")
    def test_ignore_ampersand2(self):
        set("//tmp/list", [])
        set("//tmp/list&/end", "x")
        assert get("//tmp/list&/0") == "x"
        assert get("//tmp/list&/@type") == "list_node"

    @authors("babenko", "ignat")
    def test_ignore_ampersand3(self):
        assert get("//sys/chunks&/@type") == "chunk_map"

    @authors("ignat")
    def test_ignore_ampersand4(self):
        assert not exists("//tmp/missing")
        assert not exists("//tmp/missing&")
        assert exists("//tmp")
        assert exists("//tmp&")


    @authors("babenko")
    def test_batch_empty(self):
        assert execute_batch([]) == []

    @authors("babenko")
    def test_batch_error(self):
        results = execute_batch([
            make_batch_request("get", path="//nonexisting1"),
            make_batch_request("get", path="//nonexisting2")
        ])
        assert len(results) == 2
        with pytest.raises(YtError): get_batch_output(results[0])
        with pytest.raises(YtError): get_batch_output(results[1])

    @authors("babenko")
    def test_batch_success(self):
        set_results = execute_batch([
            make_batch_request("set", path="//tmp/a", input="a"),
            make_batch_request("set", path="//tmp/b", input="b")
        ])
        assert len(set_results) == 2
        assert not get_batch_output(set_results[0])
        assert not get_batch_output(set_results[1])

        get_results = execute_batch([
            make_batch_request("get", return_only_value=True, path="//tmp/a"),
            make_batch_request("get", return_only_value=True, path="//tmp/b")
        ])
        assert len(get_results) == 2
        assert get_batch_output(get_results[0]) == "a"
        assert get_batch_output(get_results[1]) == "b"

    @authors("babenko")
    def test_batch_with_concurrency_failure(self):
        with pytest.raises(YtError): execute_batch([], concurrency=-1)

    @authors("babenko")
    def test_batch_with_concurrency_success(self):
        for i in xrange(10):
            set("//tmp/{0}".format(i), i)
        get_results = execute_batch([
            make_batch_request("get", return_only_value=True, path="//tmp/{0}".format(i)) for i in xrange(10)
        ], concurrency=2)
        assert len(get_results) == 10
        for i in xrange(10):
            assert get_batch_output(get_results[i]) == i

    @authors("babenko")
    def test_recursive_resource_usage_map(self):
        create("map_node", "//tmp/m")
        for i in xrange(10):
            set("//tmp/m/" + str(i), i)
        assert get("//tmp/m/@recursive_resource_usage/node_count") == 11

    @authors("babenko")
    def test_recursive_resource_usage_list(self):
        create("list_node", "//tmp/l")
        for i in xrange(10):
            set("//tmp/l/end", i)
        assert get("//tmp/l/@recursive_resource_usage/node_count") == 11

    @authors("ignat")
    def test_prerequisite_transactions(self):
        with pytest.raises(YtError):
            set("//tmp/test_node", {}, prerequisite_transaction_ids=["a-b-c-d"])

        tx = start_transaction()
        set("//tmp/test_node", {}, prerequisite_transaction_ids=[tx])
        remove("//tmp/test_node", prerequisite_transaction_ids=[tx])

    @authors("ignat")
    def test_prerequisite_revisions(self):
        create("map_node", "//tmp/test_node")
        revision = get("//tmp/test_node/@revision")

        with pytest.raises(YtError):
            create("map_node", "//tmp/test_node/inner_node",
                   prerequisite_revisions=[{"path": "//tmp/test_node", "transaction_id": "0-0-0-0", "revision": revision + 1}])

        create("map_node", "//tmp/test_node/inner_node",
               prerequisite_revisions=[{"path": "//tmp/test_node", "transaction_id": "0-0-0-0", "revision": revision}])

    @authors("babenko")
    def test_move_preserves_creation_time1(self):
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time == get("//tmp/t2/@creation_time")

    @authors("babenko")
    def test_move_preserves_creation_time2(self):
        set("//tmp/t1", {"x": "y"})
        creation_time1 = get("//tmp/t1/@creation_time")
        creation_time2 = get("//tmp/t1/x/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time1 == get("//tmp/t2/@creation_time")
        assert creation_time2 == get("//tmp/t2/x/@creation_time")

    @authors("ignat")
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

    @authors("shakurov")
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

    @authors("babenko")
    def test_node_path_map(self):
        set("//tmp/a", 123)
        assert get("//tmp/a/@path") == "//tmp/a"

    @authors("babenko")
    def test_node_path_list(self):
        set("//tmp/a", [1, 2, 3])
        assert get("//tmp/a/1/@path") == "//tmp/a/1"

    @authors("babenko")
    def test_node_path_map_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", 123, tx=tx)
        assert get("//tmp/a/@path", tx=tx) == "//tmp/a"

    @authors("babenko")
    def test_node_path_list_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", [1, 2, 3], tx=tx)
        assert get("//tmp/a/1/@path", tx=tx) == "//tmp/a/1"

    @authors("babenko")
    def test_broken_node_path1(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        lock("//tmp/a", tx=tx, mode="snapshot")
        remove("//tmp/a")
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    @authors("ignat")
    def test_node_path_with_slash(self):
        set("//tmp/dir", {"my\\t": {}})
        assert ls("//tmp/dir") == ["my\\t"]
        # It is double quoted since ypath syntax additionally quote backslash.
        assert get("//tmp/dir/my\\\\t") == {}
        assert get("//tmp/dir/my\\\\t/@path") == "//tmp/dir/my\\\\t"

        set("//tmp/dir", {"my\t": {}})
        assert ls("//tmp/dir") == ["my\t"]
        # Non-ascii symbols are expressed in hex format in ypath.
        assert get("//tmp/dir/my\\x09") == {}
        assert get("//tmp/dir/my\\x09/@path") == "//tmp/dir/my\\x09"

    @authors("babenko")
    def test_broken_node_path2(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        remove("//tmp/a", tx=tx)
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    @authors("shakurov")
    def test_inheritable_attributes1(self):
        # parent inheritance
        create("map_node", "//tmp/dir1")
        set("//tmp/dir1/@compression_codec", "lz4")
        create("table", "//tmp/dir1/t1")
        assert get("//tmp/dir1/t1/@compression_codec") == "lz4"

        # explicit attribute precedence
        create("table", "//tmp/dir1/t2", attributes={"compression_codec": "zstd_17"})
        assert get("//tmp/dir1/t2/@compression_codec") == "zstd_17"

        # Composite nodes don't inherit...
        create("map_node", "//tmp/dir1/dir2")
        assert not exists("//tmp/dir1/dir2/@compression_codec")

        # ...and neither do other types of nodes.
        create("document", "//tmp/dir1/doc")
        assert not exists("//tmp/dir1/doc/@compression_codec")

        # ancestor inheritance
        create("table", "//tmp/dir1/dir2/t1")
        assert get("//tmp/dir1/dir2/t1/@compression_codec") == "lz4"

        # parent inheritance
        set("//tmp/dir1/dir2/@compression_codec", "zlib_6")
        create("table", "//tmp/dir1/dir2/t2")
        assert get("//tmp/dir1/dir2/t2/@compression_codec") == "zlib_6"

    @authors("shakurov")
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

    @authors("shakurov")
    def test_inheritable_attributes_with_transactions(self):
        create("map_node", "//tmp/dir1")
        tx = start_transaction()

        # Inheritable attributes cannot be changed within trasactions.
        with pytest.raises(YtError):
            set("//tmp/dir1/@compression_codec", "lz4", tx=tx)

        set("//tmp/dir1/@compression_codec", "lz4")

        create("table", "//tmp/dir1/t1", tx=tx)
        assert get("//tmp/dir1/t1/@compression_codec", tx=tx) == "lz4"

    @authors("shakurov")
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

    @authors("shakurov")
    def test_inheritable_attributes_tablet_cell_bundle(self):
        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir2")
        create("map_node", "//tmp/dir3")

        with pytest.raises(YtError):
            set("//tmp/dir1/@tablet_cell_bundle", "non_existent")

        create_tablet_cell_bundle("b1")

        set("//tmp/dir1/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir1/@tablet_cell_bundle") == "b1"

        set("//tmp/dir2/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir2/@tablet_cell_bundle") == "b1"

        set("//tmp/dir3/@tablet_cell_bundle", "b1")
        assert get("//tmp/dir3/@tablet_cell_bundle") == "b1"

        create_tablet_cell_bundle("b2")

        tx = start_transaction()
        copy("//tmp/dir3", "//tmp/dir3_copy", tx=tx)

        assert get("//tmp/dir3_copy/@tablet_cell_bundle", tx=tx) == "b1"

        with pytest.raises(YtError):
            set("//tmp/dir3_copy/@tablet_cell_bundle", "b2", tx=tx)
        with pytest.raises(YtError):
            remove("//tmp/dir3_copy/@tablet_cell_bundle", tx=tx)

        set("//tmp/dir1/@tablet_cell_bundle", "b2")

        remove("//tmp/dir2/@tablet_cell_bundle")

        abort_transaction(tx)

        move("//tmp/dir3", "//tmp/dir3_move")

        tx = start_transaction()
        move("//tmp/dir3_move", "//tmp/dir3", tx=tx)

        abort_transaction(tx)

        remove("//tmp/dir3_move")

        remove("//sys/tablet_cell_bundles/b1")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b1"))

        remove("//sys/tablet_cell_bundles/b2")
        wait(lambda: get("//sys/tablet_cell_bundles/b2/@life_stage") in ["removal_started", "removal_pre_committed"])

        remove("//tmp/dir1")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b1"))

    @authors("shakurov")
    def test_inheritable_attributes_no_extraneous_inheritance(self):
        create("map_node", "//tmp/dir1")

        # Most inheritable attributes are inheritable by all chunk owners.
        # These, however, are only inheritable by tables (values don't matter):
        attrs = (("commit_ordering", "strong"), ("in_memory_mode", "uncompressed"), ("optimize_for", "scan"), ("tablet_cell_bundle", "b"))
        create_tablet_cell_bundle("b")
        for attr_name, attr_val in attrs:
            set("//tmp/dir1/@" + attr_name, attr_val)

        # Inheritable by all.
        set("//tmp/dir1/@compression_codec", "lz4")

        create("table", "//tmp/dir1/t1")
        create("file", "//tmp/dir1/f1")
        create("journal", "//tmp/dir1/j1")

        for node in ("t1", "f1", "j1"):
            assert get("//tmp/dir1/{0}/@compression_codec".format(node)) == "lz4"

        for attr_name, attr_val in attrs:
            assert get("//tmp/dir1/t1/@" + attr_name) == attr_val
            assert not exists("//tmp/dir1/f1/@" + attr_name)
            assert not exists("//tmp/dir1/j1/@" + attr_name)

    @authors("savrus")
    def test_create_invalid_type(self):
        with pytest.raises(YtError):
            create("some_invalid_type", "//tmp/s")
        with pytest.raises(YtError):
            create("sorted_dynamic_tablet_store", "//tmp/s")

    @authors("aozeritsky")
    def test_attributes_content_revision(self):
        create("map_node", "//tmp/test_node")
        revision = get("//tmp/test_node/@revision")
        attributes_revision = get("//tmp/test_node/@attributes_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision == attributes_revision
        assert revision == content_revision

        set("//tmp/test_node/@user_attribute1", "value1")
        revision = get("//tmp/test_node/@revision")
        attributes_revision = get("//tmp/test_node/@attributes_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision == attributes_revision
        assert revision > content_revision

        set("//tmp/test_node", {"hello": "world", "list":[0,"a",{}], "n": 1})
        revision = get("//tmp/test_node/@revision")
        attributes_revision = get("//tmp/test_node/@attributes_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision > attributes_revision
        assert revision == content_revision

    @authors("babenko")
    def test_user_attribute_removal1_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")
        set("//tmp/test_node/@user_attribute", "some_string", tx=tx1)

        remove("//tmp/test_node/@user_attribute", tx=tx2)

        assert not exists("//tmp/test_node/@user_attribute", tx=tx2)
        assert exists("//tmp/test_node/@user_attribute", tx=tx1)

    @authors("babenko")
    def test_user_attribute_removal2_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")
        set("//tmp/test_node/@user_attribute", "some_string1", tx=tx1)
        set("//tmp/test_node/@user_attribute", "some_string2", tx=tx2)
        remove("//tmp/test_node/@user_attribute", tx=tx2)
        assert not exists("//tmp/test_node/@user_attribute", tx=tx2)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/@user_attribute")

        copy("//tmp/test_node", "//tmp/copy1", tx=tx1)
        assert get("//tmp/copy1/@user_attribute", tx=tx1) == "some_string1"

        copy("//tmp/test_node", "//tmp/copy2", tx=tx2)
        assert not exists("//tmp/copy0/@user_attribute", tx=tx2)

    @authors("babenko")
    def test_user_attribute_removal3_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx2)

        create("map_node", "//tmp/test_node")
        set("//tmp/test_node/@user_attribute", "some_string1", tx=tx1)
        remove("//tmp/test_node/@user_attribute", tx=tx2)
        with pytest.raises(YtError):
            remove("//tmp/test_node/@user_attribute", tx=tx3)
        assert not exists("//tmp/test_node/@user_attribute", tx=tx3)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/@user_attribute")

        copy("//tmp/test_node", "//tmp/copy1", tx=tx1)
        assert get("//tmp/copy1/@user_attribute", tx=tx1) == "some_string1"

        copy("//tmp/test_node", "//tmp/copy2", tx=tx2)
        assert not exists("//tmp/copy0/@user_attribute", tx=tx2)

        copy("//tmp/test_node", "//tmp/copy3", tx=tx3)
        assert not exists("//tmp/copy0/@user_attribute", tx=tx3)

    @authors("babenko")
    def test_user_attribute_removal4_yt_10192(self):
        tx1 = start_transaction()

        create("map_node", "//tmp/test_node")
        set("//tmp/test_node/@user_attribute", "some_string1", tx=tx1)
        remove("//tmp/test_node/@user_attribute", tx=tx1)
        assert not exists("//tmp/test_node/@user_attribute", tx=tx1)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/@user_attribute")

        copy("//tmp/test_node", "//tmp/copy2", tx=tx1)
        assert not exists("//tmp/copy0/@user_attribute", tx=tx1)

    # The four tests below are the same as the four above except the
    # latter are for attributes and the former are for map children.
    # The only difference is that map node children cannot be directly
    # created via the 'set' method.

    @authors("shakurov")
    def test_map_child_removal1_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")
        create("table", "//tmp/test_node/child", tx=tx1)

        remove("//tmp/test_node/child", tx=tx2)

        assert not exists("//tmp/test_node/child", tx=tx2)
        assert exists("//tmp/test_node/child", tx=tx1)

    @authors("shakurov")
    def test_map_child_removal2_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")

        create("table", "//tmp/test_node/child", tx=tx1)
        # NB: this isn't exactly equivalent to
        # test_user_attribute_removal2_yt_10192, but since one can't
        # directly create a map node's child via 'set', this is the
        # closest we can get.
        set("//tmp/test_node", {"child": "some_string"}, tx=tx2)
        remove("//tmp/test_node/child", tx=tx2)
        assert not exists("//tmp/test_node/child", tx=tx2)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/child")

        copy("//tmp/test_node", "//tmp/copy1", tx=tx1)
        assert exists("//tmp/copy1/child", tx=tx1)

        copy("//tmp/test_node", "//tmp/copy2", tx=tx2)
        assert not exists("//tmp/copy0/child", tx=tx2)

    @authors("shakurov")
    def test_map_child_removal3_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx2)

        create("map_node", "//tmp/test_node")
        create("table", "//tmp/test_node/child", tx=tx1)
        remove("//tmp/test_node/child", tx=tx2)
        with pytest.raises(YtError):
            remove("//tmp/test_node/child", tx=tx3)
        assert not exists("//tmp/test_node/child", tx=tx3)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/child")

        copy("//tmp/test_node", "//tmp/copy1", tx=tx1)
        assert exists("//tmp/copy1/child", tx=tx1)

        copy("//tmp/test_node", "//tmp/copy2", tx=tx2)
        assert not exists("//tmp/copy0/child", tx=tx2)

        copy("//tmp/test_node", "//tmp/copy3", tx=tx3)
        assert not exists("//tmp/copy0/child", tx=tx3)

    @authors("shakurov")
    def test_map_child_removal4_yt_10192(self):
        tx1 = start_transaction()

        create("map_node", "//tmp/test_node")
        create("table", "//tmp/test_node/child", tx=tx1)
        remove("//tmp/test_node/child", tx=tx1)
        assert not exists("//tmp/test_node/child", tx=tx1)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/child")

        copy("//tmp/test_node", "//tmp/copy2", tx=tx1)
        assert not exists("//tmp/copy0/child", tx=tx1)

    @authors("shakurov")
    def test_builtin_versioned_attributes(self):
        create("table", "//tmp/t1", attributes={"optimize_for": "lookup", "compression_codec": "zlib_6", "erasure_codec": "reed_solomon_6_3"})

        assert get("//tmp/t1/@optimize_for") == "lookup"
        assert get("//tmp/t1/@compression_codec") == "zlib_6"
        assert get("//tmp/t1/@erasure_codec") == "reed_solomon_6_3"

        tx = start_transaction()

        set("//tmp/t1/@optimize_for", "scan", tx=tx)
        set("//tmp/t1/@compression_codec", "lz4", tx=tx)
        set("//tmp/t1/@erasure_codec", "lrc_12_2_2", tx=tx)

        assert get("//tmp/t1/@optimize_for") == "lookup"
        assert get("//tmp/t1/@compression_codec") == "zlib_6"
        assert get("//tmp/t1/@erasure_codec") == "reed_solomon_6_3"
        assert get("//tmp/t1/@optimize_for", tx=tx) == "scan"
        assert get("//tmp/t1/@compression_codec", tx=tx) == "lz4"
        assert get("//tmp/t1/@erasure_codec", tx=tx) == "lrc_12_2_2"

        commit_transaction(tx)

        assert get("//tmp/t1/@optimize_for") == "scan"
        assert get("//tmp/t1/@compression_codec") == "lz4"
        assert get("//tmp/t1/@erasure_codec") == "lrc_12_2_2"

    @authors("avmatrosov")
    def test_annotation_attribute(self):
        create("map_node", "//tmp/test_node")
        create("map_node", "//tmp/test_node/child")
        set("//tmp/test_node/@annotation", "test_node")
        assert get("//tmp/test_node/@annotation") == "test_node"
        assert get("//tmp/test_node/child/@annotation") == "test_node"
        assert get("//tmp/test_node/child/@annotation_path") == "//tmp/test_node"

        create("map_node", "//tmp/empty_node")
        set("//tmp/@annotation", "tmp")

        assert get("//tmp/test_node/@annotation") == get("//tmp/test_node/child/@annotation") == "test_node"
        assert get("//tmp/empty_node/@annotation") == "tmp"
        assert get("//tmp/empty_node/@annotation_path") == "//tmp"

        remove("//tmp/@annotation")
        assert get("//tmp/empty_node/@annotation") == None

        set("//tmp/@annotation", "test")
        set("//tmp/@annotation", None)
        assert get("//tmp/@annotation") == None

    @authors("avmatrosov")
    def test_annotation_errors(self):
        create("map_node", "//tmp/test_node")
        assert get("//tmp/test_node/@annotation") == None
        assert get("//tmp/test_node/@annotation_path") == None
        with pytest.raises(YtError): set("//tmp/test_node/@annotation", "a" * 1025)
        with pytest.raises(YtError): set("//tmp/test_node/@annotation", unichr(255))
        set("//tmp/test_node/@annotation", printable)

    @authors("avmatrosov")
    def test_annotation_transaction(self):
        create("map_node", "//tmp/test_node")
        create("map_node", "//tmp/test_node/child")

        def exists(path_to_attribute):
            return get(path_to_attribute) not in [None, ""]

        tx = start_transaction()
        set("//tmp/test_node/@annotation", "test", tx=tx)
        assert not exists("//tmp/test_node/@annotation")
        assert not exists("//tmp/test_node/child/@annotation")
        commit_transaction(tx)
        assert exists("//tmp/test_node/@annotation")
        assert exists("//tmp/test_node/child/@annotation")

        tx = start_transaction()
        remove("//tmp/test_node/@annotation", tx=tx)
        assert exists("//tmp/test_node/@annotation")
        assert exists("//tmp/test_node/child/@annotation")
        commit_transaction(tx)
        assert not exists("//tmp/test_node/@annotation")
        assert not exists("//tmp/test_node/child/@annotation")

    @authors("avmatrosov")
    def test_annotation_clone(self):
        create("map_node", "//tmp/test_node")
        create("map_node", "//tmp/test_node/child")
        set("//tmp/test_node/@annotation", "test")
        move("//tmp/test_node", "//test")
        assert get("//test/@annotation") == get("//test/child/@annotation") == "test"

    @authors("shakurov")
    def test_recursive_copy_sets_parent_on_branched_node(self):
        create_user("u")

        tx = start_transaction(authenticated_user="u")

        create('table', "//tmp/d1/d2/src/t", tx=tx, recursive=True, authenticated_user="u")
        copy("//tmp/d1/d2/src", "//tmp/d1/d2/dst", tx=tx, recursive=True, authenticated_user="u")

        tx2 = start_transaction(tx=tx, authenticated_user="u")
        # Must not throw.
        lock("//tmp/d1/d2/dst/t", tx=tx2, mode="snapshot", authenticated_user="u")

    @authors("avmatrosov")
    def test_preserve_owner(self):
        create("map_node", "//tmp/x")
        create_user("u1")
        create("table", "//tmp/x/1", authenticated_user="u1")

        copy("//tmp/x/1", "//tmp/x/2", preserve_owner=True)
        assert get("//tmp/x/1/@owner") == "u1"
        assert get("//tmp/x/2/@owner") == "u1"

        move("//tmp/x/1", "//tmp/x/3", preserve_owner=True)
        assert get("//tmp/x/3/@owner") == "u1"

    @authors("avmatrosov")
    def test_preserve_owner_transaction(self):
        create("map_node", "//tmp/x")
        create_user("u1")
        create("table", "//tmp/x/1", authenticated_user="u1")
        tx = start_transaction()

        copy("//tmp/x/1", "//tmp/x/2", tx=tx, preserve_owner=True)
        assert get("//tmp/x/2/@owner", tx=tx) == "u1"

        commit_transaction(tx)
        assert get("//tmp/x/2/@owner") == "u1"

    @authors("avmatrosov")
    def test_preserve_acl(self):
        create("table", "//tmp/t1")

        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/t2", preserve_acl=True)

        assert not get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), acl)

    @authors("avmatrosov")
    def test_preserve_acl_without_rights(self):
        create_user("u")
        create("table", "//tmp/t1", authenticated_user="u")
        create("map_node", "//tmp/test")

        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("allow", "u", "read")]
        set("//tmp/t1/@acl", acl)

        with pytest.raises(YtError):
            copy("//tmp/t1", "//tmp/test/t2", preserve_acl=True, authenticated_user="u")

    @authors("avmatrosov")
    def test_lock_existing_create(self):
        tx = start_transaction()
        create("table", "//tmp/x")
        create("table", "//tmp/x", tx=tx, ignore_existing=True, lock_existing=True)
        assert len(get("//tmp/x/@locks")) == 1
        commit_transaction(tx)
        assert len(get("//tmp/x/@locks")) == 0

    @authors("avmatrosov")
    def test_lock_existing_copy(self):
        tx = start_transaction()
        create("table", "//tmp/x")
        create("table", "//tmp/m")
        copy("//tmp/m", "//tmp/x", tx=tx, ignore_existing=True, lock_existing=True)
        assert len(get("//tmp/x/@locks")) == 1
        commit_transaction(tx)
        assert len(get("//tmp/x/@locks")) == 0

    @authors("avmatrosov")
    def test_lock_existing_errors(self):
        create("table", "//tmp/x")
        create("table", "//tmp/x1")
        with pytest.raises(YtError):
            create("map_node", "//tmp/x", lock_existing=True)
        with pytest.raises(YtError):
            move("//tmp/x", "//tmp/x1", ignore_existing=True, lock_existing=True)


    @authors("babenko")
    def test_malformed_clone_src(self):
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t1")
        copy("//tmp/m&/t1", "//tmp/t2", force=True)
        copy("//tmp/m/t1&", "//tmp/t2", force=True)
        with pytest.raises(YtError):
            copy("//tmp/m//t1", "//tmp/t2", force=True)
        with pytest.raises(YtError):
            copy("//tmp/m/t1/", "//tmp/t2", force=True)
        with pytest.raises(YtError):
            copy("//tmp/m/t1/@attr", "//tmp/t2", force=True)

##################################################################

class TestCypressMulticell(TestCypress):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko", "shakurov")
    def test_zero_external_cell_bias(self):
        # Unfortunately, it's difficult to actually check anything here.
        create("table", "//tmp/t", attributes={"external_cell_bias": 0.0})
        assert not exists("//tmp/t/@external_cell_bias")

##################################################################

class TestCypressPortal(TestCypressMulticell):
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3

    def setup(self):
        set("//tmp/@annotation", "")

    @authors("shakurov")
    def test_cross_shard_copy_w_tx(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 2})

        tx = start_transaction()
        create("table", "//tmp/p/t1", tx=tx, attributes={"external_cell_tag": 3})
        # Must not crash.
        move("//tmp/p/t1", "//tmp/t1_copy", tx=tx)

        create("table", "//tmp/t2", tx=tx, attributes={"external_cell_tag": 3})
        # Must not crash.
        move("//tmp/t2", "//tmp/p/t2_copy", tx=tx)

    @authors("avmatrosov")
    def test_annotation_portal(self):
        set("//tmp/@annotation", "test")

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 2})
        create("map_node", "//tmp/p/test")

        assert get("//tmp/p/test/@annotation") == "test"

        set("//tmp/@annotation", "")
        assert get("//tmp/p/test/@annotation") == "test"

        with pytest.raises(YtError):
            remove("//tmp/@annotation")

    @authors("avmatrosov")
    def test_annotation_attribute(self):
        pass

    @authors("avmatrosov")
    def test_annotation_errors(self):
        pass

    @authors("avmatrosov")
    def test_preserve_owner(self):
        create_user("u1")
        create("document", "//tmp/doc", authenticated_user="u1")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 2})

        # test cross-cell copy
        copy("//tmp/doc", "//tmp/p/doc", preserve_owner=True)
        assert get("//tmp/doc/@owner") == get("//tmp/p/doc/@owner") == "u1"

    @authors("avmatrosov")
    def test_preserve_acl(self):
        create("document", "//tmp/t1")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 2})

        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/p/t2", preserve_acl=True)

        assert not get("//tmp/p/t2/@inherit_acl")
        assert_items_equal(get("//tmp/p/t2/@acl"), acl)

##################################################################

class TestCypressRpcProxy(TestCypress):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

class TestCypressMulticellRpcProxy(TestCypressMulticell, TestCypressRpcProxy):
    pass

##################################################################

class TestCypressForbidSet(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def setup(self):
        set("//sys/@config/cypress_manager/forbid_set_command", True)

    @authors("shakurov")
    def test_map(self):
        create("map_node", "//tmp/d")
        with pytest.raises(YtError):
            set("//tmp/d", {})

    @authors("shakurov")
    def test_map_recursive(self):
        set("//tmp/d0", {})
        set("//tmp/d1", {"d2": {}})

    @authors("shakurov")
    def test_attrs(self):
        create("map_node", "//tmp/dir")
        set("//tmp/dir/@my_attr", 10)
        assert get("//tmp/dir/@my_attr") == 10

        set("//tmp/dir/@acl/end", {"action": "allow", "subjects": ["root"], "permissions": ["write"]})
        assert len(get("//tmp/dir/@acl")) == 1

    @authors("shakurov")
    def test_document(self):
        create("document", "//tmp/doc")
        set("//tmp/doc", {})
        set("//tmp/doc/value", 10)
        assert get("//tmp/doc/value") == 10

    @authors("shakurov")
    def test_list(self):
        create("list_node", "//tmp/list")
        set("//tmp/list/end", 0)
        set("//tmp/list/0", 1)
        set("//tmp/list/end", 2)
        assert get("//tmp/list") == [1, 2]

    @authors("shakurov")
    def test_list_recursive(self):
        set("//tmp/l", [])

    @authors("shakurov")
    def test_scalars(self):
        set("//tmp/i0", 10)
        assert get("//tmp/i0") == 10
        create("int64_node", "//tmp/i1")
        set("//tmp/i1", 20)
        assert get("//tmp/i1") == 20

        create("document", "//tmp/doc")
        set("//tmp/doc", 10)
        assert get("//tmp/doc") == 10

    @authors("levysotsky")
    def test_force_set(self):
        create("int64_node", "//tmp/integer")
        set("//tmp/integer", 20, force=True)

##################################################################

class TestCypressApiVersion4(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    @classmethod
    def setup_class(cls):
        super(TestCypressApiVersion4, cls).setup_class()
        config = deepcopy(cls.Env.configs["driver"])
        config["api_version"] = 4
        cls.driver = Driver(config)

    @classmethod
    def teardown_class(cls):
        cls.driver = None
        super(TestCypressApiVersion4, cls).teardown_class()

    def _execute(self, command, **kwargs):
        kwargs["driver"] = self.driver
        input_stream = StringIO(kwargs.pop("input")) if "input" in kwargs else None
        return yson.loads(execute_command(command, kwargs, input_stream=input_stream))

    @authors("levysotsky")
    def test_create(self):
        assert "node_id" in self._execute("create", type="map_node", path="//tmp/dir")

    @authors("levysotsky")
    def test_set_and_get(self):
        assert self._execute("set", path="//tmp/a", input='{"b"= 3}') == {}
        assert self._execute("get", path="//tmp/a") == {"value": {"b": 3}}

    @authors("levysotsky")
    def test_list(self):
        self._execute("set", path="//tmp/map", input='{"a"= 1; "b"= 2; "c"= 3}')
        assert self._execute("list", path="//tmp/map") == {"value": ["a", "b", "c"]}

    @authors("levysotsky")
    def test_trasaction(self):
        tx_result = self._execute("start_transaction")
        assert "transaction_id" in tx_result

    @authors("levysotsky", "shakurov")
    def test_lock_unlock(self):
        self._execute("set", path="//tmp/a", input='{"a"= 1}')
        result = self._execute("start_transaction")
        tx = result["transaction_id"]
        lock_result = self._execute("lock", path="//tmp/a", transaction_id=tx)
        assert "lock_id" in lock_result and "node_id" in lock_result
        assert len(get("//tmp/a/@locks")) == 1
        with pytest.raises(YtError): self._execute("set", path="//tmp/a", input='{"a"=2}')
        self._execute("unlock", path="//tmp/a", transaction_id=tx)
        assert len(get("//tmp/a/@locks")) == 0
        self._execute("set", path="//tmp/a", input='{"a"=3}')
