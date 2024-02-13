from yt_env_setup import YTEnvSetup

from yt_type_helpers import (
    make_schema
)

from yt_commands import (
    authors, wait, create, ls, get, set, copy, move, remove, link, exists,
    multiset_attributes, create_account, create_user, create_group, create_domestic_medium,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_dynamic_table, make_ace,
    remove_user, make_batch_request, execute_batch, start_transaction, abort_transaction,
    commit_transaction, lock, unlock, read_file, read_table, raises_yt_error, create_access_control_object,
    gc_collect, execute_command, get_batch_output, switch_leader, is_active_primary_master_leader,
    is_active_primary_master_follower, get_active_primary_master_leader_address,
    get_active_primary_master_follower_address, sync_mount_table, sync_create_cells, check_permission,
    get_driver, create_access_control_object_namespace)

from yt_helpers import get_current_time

from yt.common import YtError, YtResponseError
from yt.environment.helpers import assert_items_equal
import yt.yson as yson

from yt_driver_bindings import Driver

from flaky import flaky
import pytest

import requests

from copy import deepcopy
import decorator
from io import BytesIO
import json
from datetime import timedelta
import functools
from string import printable
import time

##################################################################


class TestCypressRootCreationTime(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_CELLS = 1
    NUM_NODES = 0

    @authors("kvk1920")
    def test_root_creation_time(self):
        creation_time = get("//@creation_time")
        tx = start_transaction()
        create("map_node", "//a", tx=tx)
        commit_transaction(tx)
        assert creation_time == get("//@creation_time")


def not_implemented_in_sequoia(func):
    def wrapper(func, self, *args, **kwargs):
        if isinstance(self, TestSequoia):
            pytest.skip("Not implemented in Sequoia")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


class TestCypress(YTEnvSetup):
    NUM_TEST_PARTITIONS = 12

    NUM_MASTERS = 3
    NUM_NODES = 0

    @authors("babenko")
    def test_root(self):
        # should not crash
        get("//@")

    @authors("panin", "ignat")
    def test_invalid_cases(self):
        # path not starting with /
        with raises_yt_error("Path \"a\" does not start with a valid root-designator"):
            set("a", 20)

        # path starting with single /
        with raises_yt_error("Expected \"slash\" in YPath but found \"literal\""):
            set("/a", 20)

        # empty path
        with raises_yt_error("YPath cannot be empty"):
            set("", 20)

        # empty token in path
        # TODO(h0pless): Maybe make sure the error doesn't change.
        if self.USE_SEQUOIA:
            error_message = "Expected \"literal\" in YPath but found \"slash\" token \"/\""
        else:
            error_message = "Unexpected \"slash\" token \"/\" in YPath"
        with raises_yt_error(error_message):
            set("//tmp//a/b", 20)

        # change the type of root
        with raises_yt_error("\"set\" command without \"force\" flag is forbidden"):
            set("/", [])

        # remove the root
        with raises_yt_error("Node / cannot be removed"):
            remove("/")

        # get non existent child
        with raises_yt_error("Node //tmp has no child with key \"b\""):
            get("//tmp/b")

        # remove non existent child
        with raises_yt_error("Node //tmp has no child with key \"b\""):
            remove("//tmp/b")

        # can"t create entity node inside cypress
        with raises_yt_error("Entity nodes cannot be created"):
            set("//tmp/entity", None)

    @authors("ignat")
    def test_remove(self):
        with raises_yt_error("Node //tmp has no child with key \"x\""):
            remove("//tmp/x", recursive=False)
        with raises_yt_error("Node //tmp has no child with key \"x\""):
            remove("//tmp/x")
        remove("//tmp/x", force=True)

        with raises_yt_error("Node //tmp has no child with key \"1\""):
            remove("//tmp/1", recursive=False)
        with raises_yt_error("Node //tmp has no child with key \"1\""):
            remove("//tmp/1")
        remove("//tmp/1", force=True)

        create("map_node", "//tmp/x/1/y", recursive=True)
        with raises_yt_error("Cannot remove non-empty composite node"):
            remove("//tmp/x", recursive=False)
        with raises_yt_error("Cannot remove non-empty composite node"):
            remove("//tmp/x", recursive=False, force=True)
        remove("//tmp/x/1/y", recursive=False)
        remove("//tmp/x")

        set("//tmp/@test_attribute", 10)
        remove("//tmp/@test_attribute")

        for path in [
            "//tmp/@test_attribute",
            "//tmp/@test_attribute/inner",
            "//tmp/@recursive_resource_usage/disk_space_per_medium",
            "//tmp/@recursive_resource_usage/missing",
        ]:
            with pytest.raises(YtError):
                remove(path)

        for path in ["//tmp/@test_attribute", "//tmp/@test_attribute/inner"]:
            remove(path, force=True)

        for builtin_path in ["//tmp/@key", "//tmp/@key/inner"]:
            with raises_yt_error("Attribute \"key\" cannot be removed"):
                remove(builtin_path, force=True)

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_list(self):
        set("//tmp/list", [1, 2, "some string"])
        assert get("//tmp/list") == [1, 2, "some string"]

        set("//tmp/list/end", 100)
        assert get("//tmp/list") == [1, 2, "some string", 100]

        set("//tmp/list/before:0", 200)
        assert get("//tmp/list") == [200, 1, 2, "some string", 100]

        set("//tmp/list/before:0", 500)
        assert get("//tmp/list") == [500, 200, 1, 2, "some string", 100]

        set("//tmp/list/after:2", 1000)
        assert get("//tmp/list") == [500, 200, 1, 1000, 2, "some string", 100]

        set("//tmp/list/3", 777)
        assert get("//tmp/list") == [500, 200, 1, 777, 2, "some string", 100]

        remove("//tmp/list/4")
        assert get("//tmp/list") == [500, 200, 1, 777, "some string", 100]

        remove("//tmp/list/4")
        assert get("//tmp/list") == [500, 200, 1, 777, 100]

        remove("//tmp/list/0")
        assert get("//tmp/list") == [200, 1, 777, 100]

        set("//tmp/list/end", "last")
        assert get("//tmp/list") == [200, 1, 777, 100, "last"]

        set("//tmp/list/before:0", "first")
        assert get("//tmp/list") == ["first", 200, 1, 777, 100, "last"]

        set("//tmp/list/begin", "very_first")
        assert get("//tmp/list") == ["very_first", "first", 200, 1, 777, 100, "last"]

        assert exists("//tmp/list/0")
        assert exists("//tmp/list/6")
        assert exists("//tmp/list/-1")
        assert exists("//tmp/list/-7")
        assert not exists("//tmp/list/42")
        assert not exists("//tmp/list/-42")
        with pytest.raises(YtError):
            get("//tmp/list/42")
            get("//tmp/list/-42")

    @authors("kvk1920")
    @not_implemented_in_sequoia
    def test_list_node_deprecation(self):
        set("//tmp/old_list", [1, 2, "string"])
        set("//tmp/another_old_list", [1, 2, "string"])

        set("//sys/@config/cypress_manager/forbid_list_node_creation", True)
        with raises_yt_error("List nodes are deprecated"):
            set("//tmp/list", [1, 2, "some string"])

        with raises_yt_error("List nodes are deprecated"):
            create("list_node", "//tmp/list")

        assert get("//tmp/old_list") == [1, 2, "string"]

        copy("//tmp/old_list", "//tmp/list")
        assert get("//tmp/list") == get("//tmp/old_list")

        set("//tmp/old_list/end", 123)
        assert get("//tmp/old_list", [1, 2, "string", 123])

        remove("//tmp/another_old_list")
        assert not exists("//tmp/another_old_list")

    @authors("babenko", "ignat")
    def test_list_command(self):
        set("//tmp/map", {"a": 1, "b": 2, "c": 3}, force=True)
        assert ls("//tmp/map") == ["a", "b", "c"]

        set("//tmp/map", {"a": 1}, force=True)
        assert ls("//tmp/map", max_size=1) == ["a"]

        ls("//sys/chunks")
        ls("//sys/accounts")
        ls("//sys/transactions")

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_map(self):
        set("//tmp/map", {"hello": "world", "list": [0, "a", {}], "n": 1})
        assert get("//tmp/map") == {"hello": "world", "list": [0, "a", {}], "n": 1}

        set("//tmp/map/hello", "not_world")
        assert get("//tmp/map") == {"hello": "not_world", "list": [0, "a", {}], "n": 1}

        set("//tmp/map/list/2/some", "value")
        assert get("//tmp/map") == {
            "hello": "not_world",
            "list": [0, "a", {"some": "value"}],
            "n": 1,
        }

        remove("//tmp/map/n")
        assert get("//tmp/map") == {
            "hello": "not_world",
            "list": [0, "a", {"some": "value"}],
        }

        set("//tmp/map/list", [], force=True)
        assert get("//tmp/map") == {"hello": "not_world", "list": []}

        set("//tmp/map/list/end", {})
        set("//tmp/map/list/0/a", 1)
        assert get("//tmp/map") == {"hello": "not_world", "list": [{"a": 1}]}

        set("//tmp/map/list/begin", {})
        set("//tmp/map/list/0/b", 2)
        assert get("//tmp/map") == {"hello": "not_world", "list": [{"b": 2}, {"a": 1}]}

        remove("//tmp/map/hello")
        assert get("//tmp/map") == {"list": [{"b": 2}, {"a": 1}]}

        remove("//tmp/map/list")
        assert get("//tmp/map") == {}

        with pytest.raises(YtError):
            set("//tmp/missing/node", {})

        set("//tmp/missing/node", {}, recursive=True)
        assert get("//tmp/missing") == {"node": {}}

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_attributes(self):
        set("//tmp/t", b"<attr=100;mode=rw> {nodes=[1; 2]}", is_raw=True)
        assert get("//tmp/t/@attr") == 100
        assert get("//tmp/t/@mode") == "rw"

        attrs = get("//tmp/t/@")
        assert "attr" in attrs
        assert "mode" in attrs
        assert "path" in attrs
        assert isinstance(attrs["path"], yson.YsonEntity)

        attrs = get("//tmp/t/@", attributes=["attr", "path"])
        assert sorted(attrs.keys()) == ["attr", "path"]
        assert attrs["path"] == "//tmp/t"

        remove("//tmp/t/@*")
        with pytest.raises(YtError):
            get("//tmp/t/@attr")
        with pytest.raises(YtError):
            get("//tmp/t/@mode")

        # changing attributes
        set("//tmp/t/a", b"<author = ignat> []", is_raw=True)
        assert get("//tmp/t/a") == []
        assert get("//tmp/t/a/@author") == "ignat"

        set("//tmp/t/a/@author", "not_ignat")
        assert get("//tmp/t/a/@author") == "not_ignat"

        # nested attributes (actually shows <>)
        set("//tmp/t/b", b"<dir = <file = <>-100> #> []", is_raw=True)
        assert get("//tmp/t/b/@dir/@") == {"file": -100}
        assert get("//tmp/t/b/@dir/@file") == -100
        assert get("//tmp/t/b/@dir/@file/@") == {}

        # set attributes directly
        set("//tmp/t/@", {"key1": "value1", "key2": "value2"})
        assert get("//tmp/t/@key1") == "value1"
        assert get("//tmp/t/@key2") == "value2"

        # error cases
        # typo (extra slash)
        with pytest.raises(YtError):
            get("//tmp/t/@/key1")
        # change type
        with pytest.raises(YtError):
            set("//tmp/t/@", 1)
        with pytest.raises(YtError):
            set("//tmp/t/@", "a")
        with pytest.raises(YtError):
            set("//tmp/t/@", [])
        with pytest.raises(YtError):
            set("//tmp/t/@", [1, 2, 3])

    @authors("ifsmirnov")
    def test_reserved_attributes(self):
        set(
            "//sys/@config/object_manager/reserved_attributes/table",
            {"cool_feature": "my error message"},
        )
        create("table", "//tmp/t")
        with raises_yt_error("my error message"):
            set("//tmp/t/@cool_feature", 123)

        # Setting the attribute onto the object of different type is allowed.
        create("map_node", "//tmp/m")
        set("//tmp/m/@cool_feature", "foobar")

        # Interned attributes cannot be reserved.
        set("//sys/@config/object_manager/reserved_attributes/table/account", "message")
        set("//tmp/t/@account", "tmp")

        with raises_yt_error("Error parsing EObjectType value \"bad_object_name\""):
            set(
                "//sys/@config/object_manager/reserved_attributes/bad_object_name",
                {"foo": "bar"},
            )

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_attributes_tx_read_table(self):
        set("//tmp/t", b"<attr=100> 123", is_raw=True)
        assert get("//tmp/t") == 123
        assert get("//tmp/t/@attr") == 100
        assert "attr" in get("//tmp/t/@")

        tx = start_transaction()
        assert get("//tmp/t/@attr", tx=tx) == 100
        assert "attr" in get("//tmp/t/@", tx=tx)

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
    @not_implemented_in_sequoia
    def test_format_json(self):
        # check input format for json
        set(
            "//tmp/json_in",
            b'{"list": [1,2,{"string": "this"}]}',
            is_raw=True,
            input_format="json",
        )
        assert get("//tmp/json_in") == {"list": [1, 2, {"string": "this"}]}

        # check output format for json
        set("//tmp/json_out", {"list": [1, 2, {"string": "this"}]})
        assert get(b"//tmp/json_out", is_raw=True, output_format="json") == b'{"list":[1,2,{"string":"this"}]}'

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_map_remove_all1(self):
        # remove items from map
        set("//tmp/map", {"a": "b", "c": "d"}, force=True)
        assert get("//tmp/map/@count") == 2
        remove("//tmp/map/*")
        assert get("//tmp/map") == {}
        assert get("//tmp/map/@count") == 0

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_map_remove_all2(self):
        set("//tmp/map", {"a": 1}, force=True)
        tx = start_transaction()
        set("//tmp/map", {"b": 2}, tx=tx, force=True)
        remove("//tmp/map/*", tx=tx)
        assert get("//tmp/map", tx=tx) == {}
        assert get("//tmp/map/@count", tx=tx) == 0
        commit_transaction(tx)
        assert get("//tmp/map") == {}
        assert get("//tmp/map/@count") == 0

    @authors("aleksandra-zh")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
        with pytest.raises(YtError):
            get("//tmp/t/@ref_counter")
        assert get("//tmp/t1/@ref_counter") == 1

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_list_remove_all(self):
        # remove items from list
        set("//tmp/list", [10, 20, 30])
        assert get("//tmp/list/@count") == 3
        remove("//tmp/list/*")
        assert get("//tmp/list") == []
        assert get("//tmp/list/@count") == 0

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_attr_remove_all1(self):
        # remove items from attributes
        set("//tmp/attr", b"<_foo=bar;_key=value>42", is_raw=True)
        remove("//tmp/attr/@*")
        with pytest.raises(YtError):
            get("//tmp/attr/@_foo")
        with pytest.raises(YtError):
            get("//tmp/attr/@_key")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_attr_remove_all2(self):
        set("//tmp/@a", 1)
        tx = start_transaction()
        set("//tmp/@b", 2, tx=tx)
        remove("//tmp/@*", tx=tx)
        with pytest.raises(YtError):
            get("//tmp/@a", tx=tx)
        with pytest.raises(YtError):
            get("//tmp/@b", tx=tx)
        commit_transaction(tx)
        with pytest.raises(YtError):
            get("//tmp/@a")
        with pytest.raises(YtError):
            get("//tmp/@b")

    @authors("babenko", "ignat")
    def test_copy_simple1(self):
        set("//tmp/a", 1)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == 1

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_simple2(self):
        set("//tmp/a", [1, 2, 3])
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b") == [1, 2, 3]

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_simple3(self):
        set("//tmp/a", b"<x=y> 1", is_raw=True)
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@x") == "y"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_simple4(self):
        set("//tmp/a", {"x1": "y1", "x2": "y2"})
        assert get("//tmp/a/@count") == 2

        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@count") == 2

    @authors("babenko", "ignat")
    def test_copy_simple5(self):
        set("//tmp/a", {"b": 1})
        assert get("//tmp/a/b/@path") == "//tmp/a/b"

        copy("//tmp/a", "//tmp/c")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

        remove("//tmp/a")
        assert get("//tmp/c/b/@path") == "//tmp/c/b"

    @authors("babenko", "ignat")
    def test_copy_simple6a(self):
        error_message = "Scion cannot be cloned" if self.USE_SEQUOIA else "Cannot copy or move a node to itself"
        with raises_yt_error(error_message):
            copy("//tmp", "//tmp/a")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_simple6b(self):
        tx = start_transaction()
        create("map_node", "//tmp/a", tx=tx)
        create("map_node", "//tmp/a/b", tx=tx)
        with pytest.raises(YtError):
            copy("//tmp/a", "//tmp/a/b/c", tx=tx)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_simple7(self):
        tx = start_transaction()
        with pytest.raises(YtError):
            copy("#" + tx, "//tmp/t")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
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
        with raises_yt_error("Node //tmp has no child with key \"b\""):
            copy("//tmp/a", "//tmp/b/c", recursive=False)

        with raises_yt_error("Expected \"literal\" in YPath but found \"at\" token \"@\""):
            copy("//tmp/a", "//tmp/b/c/d/@e", recursive=True)
        assert not exists("//tmp/b/c/d")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_tx1(self):
        tx = start_transaction()

        set("//tmp/a", {"x1": "y1", "x2": "y2"}, tx=tx)
        assert get("//tmp/a/@count", tx=tx) == 2

        copy("//tmp/a", "//tmp/b", tx=tx)
        assert get("//tmp/b/@count", tx=tx) == 2

        commit_transaction(tx)

        assert get("//tmp/a/@count") == 2
        assert get("//tmp/b/@count") == 2

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_tx2(self):
        set("//tmp/a", {"x1": "y1", "x2": "y2"})

        tx = start_transaction()

        remove("//tmp/a/x1", tx=tx)
        assert get("//tmp/a/@count", tx=tx) == 1

        copy("//tmp/a", "//tmp/b", tx=tx)
        assert get("//tmp/b/@count", tx=tx) == 1

        commit_transaction(tx)

        assert get("//tmp/a/@count") == 1
        assert get("//tmp/b/@count") == 1

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_copy_tx_builtin_versioned_attributes(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "optimize_for": "lookup",
                "compression_codec": "zlib_6",
                "erasure_codec": "reed_solomon_6_3",
                "chunk_merger_mode": "auto",
            },
        )

        assert get("//tmp/t1/@optimize_for") == "lookup"
        assert get("//tmp/t1/@compression_codec") == "zlib_6"
        assert get("//tmp/t1/@erasure_codec") == "reed_solomon_6_3"
        assert not get("//tmp/t1/@enable_striped_erasure")
        assert not get("//tmp/t1/@enable_skynet_sharing")
        assert get("//tmp/t1/@chunk_merger_mode") == "auto"

        tx = start_transaction()
        copy("//tmp/t1", "//tmp/t2", tx=tx)
        copy("//tmp/t2", "//tmp/t3", tx=tx)

        for table in ("t2", "t3"):
            table_path = f"//tmp/{table}"
            assert get(f"{table_path}/@optimize_for", tx=tx) == "lookup"
            assert get(f"{table_path}/@compression_codec", tx=tx) == "zlib_6"
            assert get(f"{table_path}/@erasure_codec", tx=tx) == "reed_solomon_6_3"
            assert not get(f"{table_path}/@enable_striped_erasure", tx=tx)
            assert not get(f"{table_path}/@enable_skynet_sharing", tx=tx)
            assert get(f"{table_path}/@chunk_merger_mode", tx=tx) == "auto"

        commit_transaction(tx)

        for table in ("t2", "t3"):
            table_path = f"//tmp/{table}"
            assert get(f"{table_path}/@optimize_for") == "lookup"
            assert get(f"{table_path}/@compression_codec") == "zlib_6"
            assert get(f"{table_path}/@erasure_codec") == "reed_solomon_6_3"
            assert not get(f"{table_path}/@enable_striped_erasure")
            assert not get(f"{table_path}/@enable_skynet_sharing")
            assert get(f"{table_path}/@chunk_merger_mode") == "auto"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_account1(self):
        create_account("a1")
        create_account("a2")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a1")
        set("//tmp/a2", {})
        set("//tmp/a2/@account", "a2")

        set("//tmp/a1/x", {"y": "z"})
        copy("//tmp/a1/x", "//tmp/a2/x")

        assert get("//tmp/a2/@account") == "a2"
        assert get("//tmp/a2/x/@account") == "a2"
        assert get("//tmp/a2/x/y/@account") == "a2"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_account2(self):
        create_account("a1")
        create_account("a2")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a1")
        set("//tmp/a2", {})
        set("//tmp/a2/@account", "a2")

        set("//tmp/a1/x", {"y": "z"})
        copy("//tmp/a1/x", "//tmp/a2/x", preserve_account=True)

        assert get("//tmp/a2/@account") == "a2"
        assert get("//tmp/a2/x/@account") == "a1"
        assert get("//tmp/a2/x/y/@account") == "a1"

    @authors("babenko", "ignat")
    def test_copy_unexisting_path(self):
        if self.USE_SEQUOIA:
            error_message = "Scion cannot be cloned"
        else:
            error_message = "Node //tmp has no child with key \"x\""
        with raises_yt_error(error_message):
            copy("//tmp/x", "//tmp/y")

    @authors("babenko", "ignat")
    def test_copy_cannot_have_children(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        with raises_yt_error("//tmp/t1 cannot have children"):
            copy("//tmp/t2", "//tmp/t1/xxx")

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

        with raises_yt_error("Node //tmp/a already exists"):
            copy("//tmp/b", "//tmp/a")
        # Two options simultaneously are forbidden.
        with raises_yt_error("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously"):
            copy("//tmp/b", "//tmp/new", ignore_existing=True, force=True)

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_copy_removed_bundle(self):
        create_tablet_cell_bundle("b")
        create("map_node", "//tmp/p1")
        create("map_node", "//tmp/p2")

        create("table", "//tmp/p1/t", attributes={"tablet_cell_bundle": "b"})

        remove_tablet_cell_bundle("b")
        wait(lambda: get("//sys/tablet_cell_bundles/b/@life_stage") in ["removal_started", "removal_pre_committed"])

        with pytest.raises(YtError):
            copy("//tmp/p1/t", "//tmp/p2/t")

        remove("//tmp/p1/t")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b"))

    @authors("babenko")
    @not_implemented_in_sequoia
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
        with pytest.raises(YtError):
            set("//tmp/t/@compression_codec", "lz4")
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
    @not_implemented_in_sequoia
    def test_copy_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@account") == "tmp"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_copy_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        copy("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_force1(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@a", 1)
        create("table", "//tmp/t2")
        set("//tmp/t2/@a", 2)
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@a", 1)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError):
            copy("//tmp/t1", "//tmp/t2", force=True)

    @authors("babenko")
    def test_copy_force3(self):
        create("table", "//tmp/t1")
        set("//tmp/t2", {})
        copy("//tmp/t1", "//tmp/t2", force=True)
        assert get("//tmp/t2/@type") == "table"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_force_account1(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=True)

        assert get("//tmp/a2/@account") == "a"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_force_account2(self):
        create_account("a")

        set("//tmp/a1", {})
        set("//tmp/a1/@account", "a")
        set("//tmp/a2", {})

        copy("//tmp/a1", "//tmp/a2", force=True, preserve_account=False)

        assert get("//tmp/a2/@account") == "tmp"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_locked(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        lock("//tmp/t1", tx=tx)
        copy("//tmp/t1", "//tmp/t2")
        commit_transaction(tx)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_acd(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), [])

    @authors("babenko")
    @not_implemented_in_sequoia
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
        with raises_yt_error("Node //tmp has no child with key \"a\""):
            get("//tmp/a")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_move_simple2(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        lock("//tmp/a", tx=tx)

        with pytest.raises(YtError):
            move("//tmp/a", "//tmp/b")
        assert not exists("//tmp/b")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_move_simple3(self):
        with raises_yt_error("Cannot copy or move a node to itself"):
            move("//tmp", "//tmp/a")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_move_dont_preserve_account(self):
        create_account("max")
        create("table", "//tmp/t1")
        set("//tmp/t1/@account", "max")
        move("//tmp/t1", "//tmp/t2", preserve_account=True)
        assert get("//tmp/t2/@account") == "max"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
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
        with raises_yt_error("Node //tmp has no child with key \"b\""):
            move("//tmp/a", "//tmp/b/c", recursive=False)

        with raises_yt_error("Expected \"literal\" in YPath but found \"at\" token \"@\""):
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
    @not_implemented_in_sequoia
    def test_move_force2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        tx = start_transaction()
        lock("//tmp/t2", tx=tx)
        with pytest.raises(YtError):
            move("//tmp/t1", "//tmp/t2", force=True)
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
        error_message = "//tmp is not a local object" if self.USE_SEQUOIA else "Node / cannot be replaced"
        with raises_yt_error(error_message):
            copy("//tmp", "/", force=True)

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_move_tx_locking1(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        tx2 = start_transaction()
        with pytest.raises(YtError):
            move("//tmp/t1", "//tmp/t3", tx=tx2)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_move_tx_locking2(self):
        create("table", "//tmp/t1")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx1)
        move("//tmp/t1", "//tmp/t2", tx=tx1)
        move("//tmp/t2", "//tmp/t3", tx=tx2)
        with pytest.raises(YtError):
            move("//tmp/t2", "//tmp/t4", tx=tx3)

    @authors("ignat")
    def test_embedded_attributes(self):
        set("//tmp/a", {})
        set("//tmp/a/@attr", {"key": "value"})
        set("//tmp/a/@attr/key/@embedded_attr", "emb")
        assert get("//tmp/a/@attr") == {"key": yson.to_yson_type("value", attributes={"embedded_attr": "emb"})}
        assert get("//tmp/a/@attr/key") == yson.to_yson_type("value", attributes={"embedded_attr": "emb"})
        assert get("//tmp/a/@attr/key/@embedded_attr") == "emb"

    @authors("babenko")
    def test_get_with_attributes(self):
        set("//tmp/a/b", {}, recursive=True, force=True)
        expected = yson.to_yson_type(
            {"b": yson.to_yson_type({}, {"type": "map_node"})}, {"type": "map_node"}
        )
        assert get("//tmp/a", attributes=["type"]) == expected
        assert get("//tmp/a", attributes={"keys": ["type"]}) == expected

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_list_with_attributes(self):
        set("//tmp/a/b", {}, recursive=True, force=True)
        expected = [yson.to_yson_type("b", attributes={"type": "map_node"})]
        assert ls("//tmp/a", attributes=["type"]) == expected
        assert ls("//tmp/a", attributes={"keys": ["type"]}) == expected

    @authors("kiselyovp")
    def test_get_with_attributes_objects(self):
        assert get("//sys/accounts/tmp", attributes=["name"]) == yson.to_yson_type({}, {"name": "tmp"})
        assert get("//sys/users/root", attributes=["name", "type"]) == yson.to_yson_type(
            None, {"name": "root", "type": "user"}
        )

    @authors("max42")
    @not_implemented_in_sequoia
    def test_attribute_path_filtering(self):
        schema = [{"name": "x", "type": "int64"}, {"name": "y", "type": "int64"}]
        attributes = {"schema": schema, "custom": {"foo": 42, "bar": 57}}
        create("table", "//tmp/a/b1", recursive=True, attributes=attributes)
        create("table", "//tmp/a/b2", recursive=True, attributes=attributes)

        paths = [
            # A sync user attribute.
            "/custom/bar",
            # A sync builtin attribute.
            "/resource_usage/node_count",
            # An async builtin attribute.
            "/schema/1/name",
        ]

        expected_table_attributes = {
            "custom": {"bar": 57},
            "resource_usage": {"node_count": 1},
            "schema": [yson.YsonEntity(), {"name": "y"}],
        }
        expected_map_node_attributes = {
            "resource_usage": {"node_count": 1},
        }

        assert_items_equal(
            ls("//tmp/a", attributes={"paths": paths}),
            [
                yson.to_yson_type("b1", attributes=expected_table_attributes),
                yson.to_yson_type("b2", attributes=expected_table_attributes),
            ])

        assert \
            get("//tmp/a", attributes={"paths": paths}) == \
            yson.to_yson_type({
                "b1": yson.to_yson_type(yson.YsonEntity(), attributes=expected_table_attributes),
                "b2": yson.to_yson_type(yson.YsonEntity(), attributes=expected_table_attributes),
            }, attributes=expected_map_node_attributes)

    @authors("max42")
    @not_implemented_in_sequoia
    def test_get_with_attributes_path_filtering_for_virtual_objects(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i_am_column_name", "type": "int64"}]})
        our_schema_id = get("//tmp/t/@schema_id")
        schemas = get("//sys/master_table_schemas", attributes={"keys": ["ref_counter"], "paths": ["/value/0/name"]})
        schemas = [(schema_id, schema) for schema_id, schema in schemas.items() if schema_id == our_schema_id]
        assert len(schemas) == 1
        assert schemas[0][1].attributes["ref_counter"] == 1
        assert schemas[0][1].attributes["value"][0]["name"] == "i_am_column_name"

    @authors("babenko")
    def test_get_with_attributes_virtual_maps(self):
        tx = start_transaction()
        txs = get("//sys/transactions", attributes=["type"])
        assert txs.attributes["type"] == "transaction_map"
        assert txs[tx] == yson.to_yson_type(None, attributes={"type": "transaction"})

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_move_virtual_maps1(self):
        create("tablet_map", "//tmp/t")
        move("//tmp/t", "//tmp/tt")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_move_virtual_maps2(self):
        create("chunk_map", "//tmp/c")
        move("//tmp/c", "//tmp/cc")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_list_with_attributes_virtual_maps(self):
        tx = start_transaction()
        txs = ls("//sys/transactions", attributes=["type"])
        assert yson.to_yson_type(tx, attributes={"type": "transaction"}) in txs

    @authors("aleksandra-zh")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
        with raises_yt_error("Node //tmp has no child with key \"a\""):
            create("map_node", "//tmp/a/b")

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
        existing_type = "sequoia_map_node" if self.USE_SEQUOIA else "map_node"
        with raises_yt_error(f"//tmp/a/b already exists and has type \"{existing_type}\" while node of \"table\" type is about to be created"):
            create("table", "//tmp/a/b", ignore_existing=True)

    @authors("babenko")
    def test_create_ignore_existing_force_fail(self):
        with raises_yt_error("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously"):
            create("table", "//tmp/t", ignore_existing=True, force=True)

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_create_ignore_type_mismatch(self):
        create("map_node", "//tmp/a/b", recursive=True)
        create("map_node", "//tmp/a/b", ignore_existing=True, ignore_type_mismatch=True)
        create("table", "//tmp/a/b", ignore_existing=True, ignore_type_mismatch=True)
        assert get("//tmp/a/b/@type") == "map_node"

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_create_ignore_type_mismatch_without_ignore_existing_fail(self):
        create("map_node", "//tmp/a/b", recursive=True)
        with pytest.raises(YtError):
            create("map_node", "//tmp/a/b", ignore_type_mismatch=True)

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
        with raises_yt_error("Expected \"literal\" in YPath but found \"at\" token \"@\""):
            create("map_node", "//tmp/a/b/c/d/@d", recursive=True)
        assert not exists("//tmp/a/b/c/d")
        create("map_node", "//tmp/a/b/c/d", recursive=True)
        assert exists("//tmp/a/b/c/d")

    @authors("kvk1920")
    @not_implemented_in_sequoia
    def test_create_object_ignore_existing(self):
        user_u = create_user("u", ignore_existing=True)
        group_g = create_group("g", ignore_existing=True)
        assert create_user("u", ignore_existing=True) == user_u
        assert create_group("g", ignore_existing=True) == group_g

    @authors("kiselyovp")
    @not_implemented_in_sequoia
    def test_remove_from_virtual_map(self):
        create_user("u")
        with pytest.raises(YtError):
            remove("//sys/users/*")
        assert exists("//sys/users/u")
        remove_user("u")
        assert not exists("//sys/users/u")
        with pytest.raises(YtError):
            remove_user("u", sync=False)
        remove_user("u", force=True)

    @authors("babenko", "s-v-m")
    @not_implemented_in_sequoia
    def test_link1(self):
        set("//tmp/a", 1)
        link("//tmp/a", "//tmp/b")
        assert not get("//tmp/b&/@broken")
        remove("//tmp/a")
        assert get("//tmp/b&/@broken")

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_link3(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        remove("//tmp/t1")
        assert get("//tmp/t2&/@broken")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link4(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")

        tx = start_transaction()
        id = get("//tmp/t1/@id")
        lock("#%s" % id, mode="snapshot", tx=tx)

        remove("//tmp/t1")

        assert get("#%s" % id, tx=tx) == 1
        assert get("//tmp/t2&/@broken")
        with pytest.raises(YtError):
            read_table("//tmp/t2")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link5(self):
        set("//tmp/t1", 1)
        set("//tmp/t2", 2)
        with pytest.raises(YtError):
            link("//tmp/t1", "//tmp/t2")

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_link7(self):
        tx = start_transaction()
        set("//tmp/t1", 1, tx=tx)
        link("//tmp/t1", "//tmp/l1", tx=tx)
        assert get("//tmp/l1", tx=tx) == 1

    @authors("s-v-m")
    @not_implemented_in_sequoia
    def test_link_dst_doesnt_exist(self):
        tx = start_transaction()
        set("//tmp/t", 1, tx=tx)
        with pytest.raises(YtError):
            link("//tmp/t", "//tmp/link1")
        link("//tmp/t", "//tmp/link1", tx=tx)
        link("//tmp/t", "//tmp/link2", force=True)
        assert get("//tmp/link2&/@broken")
        commit_transaction(tx)
        assert not get("//tmp/link1&/@broken")
        assert not get("//tmp/link2&/@broken")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_existing_fail(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        assert get("//tmp/l/@id") == id1
        with pytest.raises(YtError):
            link("//tmp/t2", "//tmp/l")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_ignore_existing(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", ignore_existing=True)
        assert get("//tmp/l/@id") == id1

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_force1(self):
        id1 = create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/t1/@id") == id1
        assert get("//tmp/l/@id") == id2

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_force2(self):
        create("table", "//tmp/t1")
        id2 = create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        remove("//tmp/t1")
        assert get("//tmp/l&/@broken")
        link("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@id") == id2

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_ignore_existing_force_fail(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            link("//tmp/t", "//tmp/l", ignore_existing=True, force=True)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_to_link(self):
        id = create("table", "//tmp/t")
        link("//tmp/t", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        assert get("//tmp/l2/@id") == id
        assert not get("//tmp/l2&/@broken")
        remove("//tmp/l1")
        assert get("//tmp/l2&/@broken")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_as_copy_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError):
            copy("//tmp/t2", "//tmp/l")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_as_copy_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        copy("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert get("//tmp/t1/@id") == id1

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_as_move_target_fail(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        with pytest.raises(YtError):
            move("//tmp/t2", "//tmp/l")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_link_as_move_target_success(self):
        id1 = create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        link("//tmp/t1", "//tmp/l")
        move("//tmp/t2", "//tmp/l", force=True)
        assert get("//tmp/l/@type") == "table"
        assert not exists("//tmp/t2")
        assert get("//tmp/t1/@id") == id1

    @authors("h0pless")
    @not_implemented_in_sequoia
    def test_cyclic_link(self):
        create("map_node", "//tmp/a/b/c", recursive=True)
        link("//tmp/a/b/c", "//tmp/a/l1")
        create("map_node", "//tmp/r")
        link("//tmp/r", "//tmp/r/l2")
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/a/l1", "//tmp/a/b/c", force=True)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/r/l2", "//tmp/r/l2", force=True)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/r/l2/l2/l2", "//tmp/r/l2", force=True)
        with raises_yt_error("Failed to create link: link is cyclic"):
            create("link", "//tmp/a/b/c/d", attributes={"target_path": "//tmp/a/b/c/d"})
        with raises_yt_error("Failed to create link: link is cyclic"):
            create("link", "//tmp/a/b/c/d/e", attributes={"target_path": "//tmp/a/b/c/d/e"}, recursive=True)
        with raises_yt_error("Failed to create link: link is cyclic"):
            create("link", "//tmp/a/b/c/d/e", attributes={"target_path": "//tmp/a/b/c/d/e"}, recursive=True, force=True)

    # Test for YTADMINREQ-29192 issue.
    @authors("h0pless")
    @not_implemented_in_sequoia
    def test_non_cyclic_link_to_link(self):
        create("table", "//tmp/t1")
        link("//tmp/t1", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        link("//tmp/l1", "//tmp/l2", force=True)

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_access_stat2(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        tx = start_transaction()
        lock("//tmp/d", mode="snapshot", tx=tx)
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter", tx=tx)
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
    @not_implemented_in_sequoia
    def test_access_stat6(self):
        create("map_node", "//tmp/d")
        time.sleep(1)
        c1 = get("//tmp/d/@access_counter")
        ls("//tmp/d/@")
        time.sleep(1)
        c2 = get("//tmp/d/@access_counter")
        assert c1 == c2

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
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

    @authors("cherepashka")
    @not_implemented_in_sequoia
    def test_access_time_in_copy(self):
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@access_time")
        copy("//tmp/t1", "//tmp/t2")
        time.sleep(1)
        assert get("//tmp/t1/@access_time") > creation_time

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

    @authors("babenko", "ignat", "h0pless")
    @not_implemented_in_sequoia
    def test_access_stat_suppress3(self):
        create("table", "//tmp/t")
        time.sleep(1)
        c1 = get("//tmp/t/@access_counter")

        read_table("//tmp/t", table_reader={"suppress_access_tracking": True})
        time.sleep(1)
        assert c1 == get("//tmp/t/@access_counter")

        read_table("//tmp/t", suppress_access_tracking=True)
        time.sleep(1)
        assert c1 == get("//tmp/t/@access_counter")

        read_table("//tmp/t")
        time.sleep(1)
        assert c1 != get("//tmp/t/@access_counter")

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_access_stat_suppress4(self):
        time.sleep(1)
        create("file", "//tmp/f")
        c1 = get("//tmp/f/@access_counter")
        read_file("//tmp/f", suppress_access_tracking=True)
        time.sleep(1)
        c2 = get("//tmp/f/@access_counter")
        assert c1 == c2

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_modification_suppress1(self):
        create("map_node", "//tmp/m")
        time.sleep(1)
        time1 = get("//tmp/m/@modification_time")
        set("//tmp/m/@x", 1, suppress_modification_tracking=True)
        time.sleep(1)
        time2 = get("//tmp/m/@modification_time")
        assert time1 == time2

    @authors("babenko", "ignat", "danilalexeev")
    @not_implemented_in_sequoia
    def test_chunk_maps(self):
        gc_collect()
        assert get("//sys/chunks/@count") == 0
        assert get("//sys/underreplicated_chunks/@count") == 0
        assert get("//sys/overreplicated_chunks/@count") == 0
        assert get("//sys/unexpected_overreplicated_chunks/@count") == 0
        assert get("//sys/replica_temporarily_unavailable_chunks/@count") == 0

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_list_attributes(self):
        create("map_node", "//tmp/map", attributes={"user_attr1": 10})
        set("//tmp/map/@user_attr2", "abc")
        assert sorted(get("//tmp/map/@user_attribute_keys")) == sorted(["user_attr1", "user_attr2"])
        assert get("//tmp/map/@user_attributes") == {
            "user_attr1": 10,
            "user_attr2": "abc",
        }

        create("table", "//tmp/table")
        assert get("//tmp/table/@user_attribute_keys") == []
        assert get("//tmp/table/@user_attributes") == {}

        create("file", "//tmp/file")
        assert get("//tmp/file/@user_attribute_keys") == []
        assert get("//tmp/file/@user_attributes") == {}

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_opaque_attribute_keys(self):
        create("table", "//tmp/t")
        assert "compression_statistics" in get("//tmp/t/@opaque_attribute_keys")

    @authors("ignat")
    def test_boolean(self):
        yson_format = yson.loads(b"yson")
        set("//tmp/boolean", b"%true", is_raw=True)
        assert get("//tmp/boolean/@type") == "boolean_node"
        assert get("//tmp/boolean", output_format=yson_format)

    @authors("lukyan")
    def test_uint64(self):
        yson_format = yson.loads(b"yson")
        set("//tmp/my_uint", b"123456u", is_raw=True)
        assert get("//tmp/my_uint/@type") == "uint64_node"
        assert get("//tmp/my_uint", output_format=yson_format) == 123456

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_map_node_children_limit(self):
        set("//sys/@config/cypress_manager/max_node_child_count", 100)
        create("map_node", "//tmp/test_node")
        for i in range(100):
            create("map_node", "//tmp/test_node/" + str(i))
        with pytest.raises(YtError):
            create("map_node", "//tmp/test_node/100")

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_attribute_size_limit(self):
        set("//sys/@config/cypress_manager/max_attribute_size", 300)
        set("//tmp/test_node", {})

        # The limit is 300 but this is for binary YSON.
        set("//tmp/test_node/@test_attr", "x" * 290)

        with pytest.raises(YtError):
            # This must definitely exceed the limit of 300.
            set("//tmp/test_node/@test_attr", "x" * 301)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_map_node_key_length_limits(self):
        set("//sys/@config/cypress_manager/max_map_node_key_length", 300)
        set("//tmp/" + "a" * 300, 0)
        with pytest.raises(YtError):
            set("//tmp/" + "a" * 301, 0)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_invalid_external_cell_bias(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": -1.0})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external_cell_bias": 100.0})

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_validation(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@expiration_time", "hello")

    @authors("babenko")
    @pytest.mark.parametrize("expiration", [("expiration_time", str(get_current_time())), ("expiration_timeout", 3600000)])
    @not_implemented_in_sequoia
    def test_expiration_change_requires_remove_permission_failure(self, expiration):
        create_user("u")
        create("table", "//tmp/t")
        set(
            "//tmp/t/@acl",
            [make_ace("allow", "u", "write"), make_ace("deny", "u", "remove")],
        )
        with pytest.raises(YtError):
            set(
                "//tmp/t/@" + expiration[0],
                expiration[1],
                authenticated_user="u",
            )

    @authors("babenko")
    @pytest.mark.parametrize("expiration", [("expiration_time", str(get_current_time())), ("expiration_timeout", 3600000)])
    @not_implemented_in_sequoia
    def test_expiration_change_requires_recursive_remove_permission_failure(self, expiration):
        create_user("u")
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t")
        set(
            "//tmp/m/t/@acl",
            [make_ace("allow", "u", "write"), make_ace("deny", "u", "remove")],
        )
        with pytest.raises(YtError):
            set(
                "//tmp/m/@" + expiration[0],
                expiration[1],
                authenticated_user="u",
            )

    @authors("babenko")
    @pytest.mark.parametrize("expiration", [("expiration_time", str(get_current_time() + timedelta(days=1))), ("expiration_timeout", 3600000)])
    @not_implemented_in_sequoia
    def test_expiration_reset_requires_write_permission_success(self, expiration):
        create_user("u")
        create(
            "table",
            "//tmp/t",
            attributes={expiration[0]: expiration[1]},
        )
        set(
            "//tmp/t/@acl",
            [make_ace("allow", "u", "write"), make_ace("deny", "u", "remove")],
        )
        remove("//tmp/t/@" + expiration[0], authenticated_user="u")

    @authors("babenko")
    @pytest.mark.parametrize("expiration", [("expiration_time", str(get_current_time() + timedelta(days=1))), ("expiration_timeout", 3600000)])
    @not_implemented_in_sequoia
    def test_expiration_reset_requires_write_permission_failure(self, expiration):
        create_user("u")
        create(
            "table",
            "//tmp/t",
            attributes={expiration[0]: expiration[1]},
        )
        set("//tmp/t/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            remove("//tmp/t/@" + expiration[0], authenticated_user="u")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_change(self):
        create(
            "table",
            "//tmp/t",
            attributes={"expiration_time": "2030-04-03T21:25:29.000000Z"},
        )
        assert get("//tmp/t/@expiration_time") == "2030-04-03T21:25:29.000000Z"
        remove("//tmp/t/@expiration_time")
        assert not exists("//tmp/t/@expiration_time")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_can_be_set_upon_construction1(self):
        create_user("u")
        create(
            "table",
            "//tmp/t",
            attributes={"expiration_time": str(get_current_time())},
            authenticated_user="u",
        )
        wait(lambda: not exists("//tmp/t"))

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_can_be_set_upon_construction2(self):
        create(
            "table",
            "//tmp/t",
            attributes={"expiration_time": str(get_current_time() + timedelta(seconds=10.0))},
        )
        time.sleep(1)
        assert exists("//tmp/t")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_can_be_set_upon_construction3(self):
        tx = start_transaction()
        create(
            "table",
            "//tmp/t",
            attributes={"expiration_time": str(get_current_time())},
            tx=tx,
        )
        time.sleep(1)
        assert not exists("//tmp/t")
        assert exists("//tmp/t", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_removal(self):
        create("table", "//tmp/t", attributes={"expiration_time": str(get_current_time())})
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko", "shakurov")
    @not_implemented_in_sequoia
    def test_expiration_time_lock_conflict(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx)
        with pytest.raises(YtError):
            set("//tmp/t/@expiration_time", str(get_current_time()))
        unlock("//tmp/t", tx=tx)
        set("//tmp/t/@expiration_time", str(get_current_time()))
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_wait_for_parent_locks_released(self):
        create("table", "//tmp/x/t", recursive=True)
        tx = start_transaction()
        lock("//tmp/x", tx=tx)
        set("//tmp/x/t/@expiration_time", str(get_current_time()))
        time.sleep(1)
        assert exists("//tmp/x/t")
        abort_transaction(tx)
        time.sleep(1)
        wait(lambda: not exists("//tmp/x/t"))

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_wait_for_locks_released_recursive(self):
        create("map_node", "//tmp/m")
        create("table", "//tmp/m/t")
        tx = start_transaction()
        lock("//tmp/m/t", tx=tx)
        set("//tmp/m/@expiration_time", str(get_current_time()))
        time.sleep(1)
        assert exists("//tmp/m")
        abort_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/m")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_expiration_time_dont_wait_for_snapshot_locks(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", tx=tx, mode="snapshot")
        set("//tmp/t/@expiration_time", str(get_current_time()))
        time.sleep(1)
        assert not exists("//tmp/t")

    @authors("babenko")
    def test_no_expiration_time_for_root(self):
        with raises_yt_error("Cannot set \"expiration_time\" for the root"):
            set("//@expiration_time", str(get_current_time()))

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_expiration_time_versioning1(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"},
        )

        tx = start_transaction()

        set("//tmp/t1/@expiration_time", "2031-03-07T13:18:55.000000Z", tx=tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"
        assert get("//tmp/t1/@expiration_time", tx=tx) == "2031-03-07T13:18:55.000000Z"

        commit_transaction(tx)

        assert get("//tmp/t1/@expiration_time") == "2031-03-07T13:18:55.000000Z"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_expiration_time_versioning2(self):
        create("table", "//tmp/t1")

        tx = start_transaction()

        set("//tmp/t1/@expiration_time", "2030-03-07T13:18:55.000000Z", tx=tx)

        assert get("//tmp/t1/@expiration_time", tx=tx) == "2030-03-07T13:18:55.000000Z"
        assert not exists("//tmp/t1/@expiration_time")

        commit_transaction(tx)

        assert get("//tmp/t1/@expiration_time") == "2030-03-07T13:18:55.000000Z"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_expiration_time_versioning3(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"},
        )

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
    @not_implemented_in_sequoia
    def test_expiration_time_versioning4(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": "2030-03-07T13:18:55.000000Z"},
        )

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
    @not_implemented_in_sequoia
    def test_expiration_time_versioning5(self):
        tx = start_transaction()
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": str(get_current_time())},
            tx=tx,
        )
        time.sleep(1)
        assert exists("//tmp/t1", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert not exists("//tmp/t1")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_preserve_expiration_time(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": str(get_current_time() + timedelta(seconds=1))},
        )
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True)
        assert exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_dont_preserve_expiration_time(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": str(get_current_time() + timedelta(seconds=1))},
        )
        copy("//tmp/t1", "//tmp/t2")
        assert not exists("//tmp/t2/@expiration_time")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2")

    @authors("egor-gutrov")
    @not_implemented_in_sequoia
    def test_copy_preserve_expiration_timeout(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_timeout": 1000},
        )
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_timeout=True)
        assert exists("//tmp/t2/@expiration_timeout")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")

    @authors("egor-gutrov")
    @not_implemented_in_sequoia
    def test_copy_dont_preserve_expiration_timeout(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_timeout": 1000},
        )
        copy("//tmp/t1", "//tmp/t2")
        assert not exists("//tmp/t2/@expiration_timeout")
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_preserve_expiration_time_in_tx1(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"expiration_time": str(get_current_time() + timedelta(seconds=1))},
        )
        tx = start_transaction()
        copy("//tmp/t1", "//tmp/t2", preserve_expiration_time=True, tx=tx)
        assert exists("//tmp/t2/@expiration_time", tx=tx)
        time.sleep(2)
        assert not exists("//tmp/t1")
        assert exists("//tmp/t2", tx=tx)
        commit_transaction(tx)
        wait(lambda: not exists("//tmp/t2"))

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_copy_preserve_expiration_time_in_tx2(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        set(
            "//tmp/t1/@expiration_time",
            str(get_current_time() + timedelta(seconds=1)),
            tx=tx,
        )
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
    @not_implemented_in_sequoia
    def test_expire_orphaned_node_yt_8064(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        node_id = create(
            "table",
            "//tmp/t",
            attributes={"expiration_time": str(get_current_time() + timedelta(seconds=2))},
            tx=tx1,
        )
        lock("#" + node_id, tx=tx2, mode="snapshot")
        abort_transaction(tx1)
        time.sleep(2)

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout1(self):
        create("table", "//tmp/t", attributes={"expiration_timeout": 1000})
        # Accessing a node may affect its lifetime. Hence no waiting here.
        time.sleep(1.5)
        assert not exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout2(self):
        set("//sys/@config/cypress_manager/expiration_check_period", 200)
        set("//sys/@config/cypress_manager/statistics_flush_period", 200)

        create("table", "//tmp/t", attributes={"expiration_timeout": 2000})
        time.sleep(1.0)
        set("//tmp/t/@expiration_timeout", 1000, suppress_expiration_timeout_renewal=True)
        time.sleep(0.6)
        assert not exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout3(self):
        create("table", "//tmp/t1", attributes={"expiration_timeout": 2000})
        create("table", "//tmp/t2", attributes={"expiration_timeout": 2000})
        time.sleep(0.5)
        set("//tmp/t2/@some_attr", "some_value")
        wait(
            lambda: not exists("//tmp/t1", suppress_expiration_timeout_renewal=True)
            and exists("//tmp/t2", suppress_expiration_timeout_renewal=True)
        )
        wait(lambda: not exists("//tmp/t2", suppress_expiration_timeout_renewal=True))

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout4(self):
        create("table", "//tmp/t1", attributes={"expiration_timeout": 4000})
        for i in range(10):
            # NB: asking if whether the node exists prolongs its life.
            assert exists("//tmp/t1")
            time.sleep(1.0)

        time.sleep(3.5)
        assert not exists("//tmp/t1")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout5(self):
        tx = start_transaction()

        create("table", "//tmp/t")
        set("//tmp/t/@expiration_timeout", 1000)

        lock("//tmp/t", tx=tx, mode="snapshot")

        time.sleep(1.5)
        assert exists("//tmp/t")

        abort_transaction(tx)

        time.sleep(1.5)
        assert not exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout6(self):
        tx = start_transaction()

        create("table", "//tmp/t")
        lock("//tmp/t", tx=tx, mode="snapshot")

        set("//tmp/t/@expiration_timeout", 1000)

        time.sleep(1.5)
        assert exists("//tmp/t")

        abort_transaction(tx)

        time.sleep(1.5)
        assert not exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout7(self):
        create("table", "//tmp/t", attributes={"expiration_timeout": 1000})
        set("//tmp/t/@expiration_timeout", 3000)

        time.sleep(1.5)
        assert exists("//tmp/t")

        remove("//tmp/t/@expiration_timeout")

        time.sleep(2.0)
        assert exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout8(self):
        create("table", "//tmp/t")

        tx = start_transaction(timeout=60000)
        set("//tmp/t/@expiration_timeout", 1000, tx=tx)
        assert get("//tmp/t/@expiration_timeout", tx=tx) == 1000

        # Uncommitted expiration timeout has no effect.
        time.sleep(1.5)
        assert exists("//tmp/t")

        commit_transaction(tx)

        assert get("//tmp/t/@expiration_timeout") == 1000

        time.sleep(1.5)
        assert not exists("//tmp/t")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_timeout_zero(self):
        # Very small - including zero timeouts - should be handled normally.
        create("table", "//tmp/t1", attributes={"expiration_timeout": 0})
        create("table", "//tmp/t2", attributes={"expiration_timeout": 10})
        create("table", "//tmp/t3")
        set("//tmp/t3/@expiration_timeout", 0)
        create("table", "//tmp/t4")
        set("//tmp/t4/@expiration_timeout", 10)
        # Accessing a node may affect its lifetime. Hence no waiting here.
        time.sleep(1.5)
        assert not exists("//tmp/t1")
        assert not exists("//tmp/t2")
        assert not exists("//tmp/t3")
        assert not exists("//tmp/t4")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_time_and_timeout1(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "expiration_timeout": 2000,
                "expiration_time": str(get_current_time() + timedelta(milliseconds=500)),
            },
        )
        time.sleep(1.0)
        assert not exists("//tmp/t1")

        create(
            "table",
            "//tmp/t2",
            attributes={
                "expiration_timeout": 500,
                "expiration_time": str(get_current_time() + timedelta(seconds=2)),
            },
        )
        time.sleep(1.0)
        assert not exists("//tmp/t2")

    @authors("shakurov")
    @not_implemented_in_sequoia
    @flaky(max_runs=3)
    def test_expiration_time_and_timeout2(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "expiration_timeout": 400,
                "expiration_time": str(get_current_time() + timedelta(milliseconds=1500)),
            },
        )
        remove("//tmp/t1/@expiration_timeout")
        time.sleep(0.6)
        assert exists("//tmp/t1")
        time.sleep(1.5)
        assert not exists("//tmp/t1")

        create(
            "table",
            "//tmp/t2",
            attributes={
                "expiration_timeout": 1500,
                "expiration_time": str(get_current_time() + timedelta(milliseconds=400)),
            },
        )
        remove("//tmp/t2/@expiration_time")
        time.sleep(0.6)
        assert exists("//tmp/t2", suppress_expiration_timeout_renewal=True)
        time.sleep(1.5)
        assert not exists("//tmp/t2")

    @authors("danilalexeev")
    @not_implemented_in_sequoia
    def test_effective_expiration_time_and_timeout(self):
        assert get("//tmp/@effective_expiration") == {"time": yson.YsonEntity(), "timeout": yson.YsonEntity()}

        create("map_node", "//tmp/m1", attributes={"expiration_time": "2044-01-01"})
        create("table", "//tmp/m1/t1")
        assert get("//tmp/m1/t1/@effective_expiration")["time"] == {"value": "2044-01-01T00:00:00.000000Z", "path": "//tmp/m1"}

        create("map_node", "//tmp/m2", attributes={"expiration_time": "2030-01-01"})
        create("map_node", "//tmp/m2/m2")
        create("table", "//tmp/m2/m2/t2", attributes={"expiration_time": "2044-01-01"})
        assert get("//tmp/m2/m2/t2/@effective_expiration")["time"] == {"value": "2030-01-01T00:00:00.000000Z", "path": "//tmp/m2"}

        create("map_node", "//tmp/m3", attributes={"expiration_timeout": 10000})
        create("table", "//tmp/m3/t3", attributes={"expiration_timeout": 20000})
        assert get("//tmp/m3/t3/@effective_expiration")["timeout"] == {"value": 10000, "path": "//tmp/m3"}

        create("map_node", "//tmp/m4")
        create("table", "//tmp/m4/t4", attributes={"expiration_timeout": 20000})
        assert get("//tmp/m4/t4/@effective_expiration")["timeout"] == {"value": 20000, "path": "//tmp/m4/t4"}

    @authors("babenko")
    @pytest.mark.parametrize("preserve", [False, True])
    @not_implemented_in_sequoia
    def test_preserve_creation_time(self, preserve):
        create("table", "//tmp/t1")

        creation_time = get("//tmp/t1/@creation_time")
        copy("//tmp/t1", "//tmp/t2", preserve_creation_time=preserve)
        assert preserve == (creation_time == get("//tmp/t2/@creation_time"))

        creation_time = get("//tmp/t1/@creation_time")
        move("//tmp/t2", "//tmp/t1", preserve_creation_time=preserve, force=True)
        assert preserve == (creation_time == get("//tmp/t1/@creation_time"))

    @authors("babenko")
    @pytest.mark.parametrize("preserve", [False, True])
    @not_implemented_in_sequoia
    def test_preserve_modification_time(self, preserve):
        create("table", "//tmp/t1")

        modification_time = get("//tmp/t1/@modification_time")
        copy("//tmp/t1", "//tmp/t2", preserve_modification_time=preserve)
        assert preserve == (modification_time == get("//tmp/t2/@modification_time"))

        modification_time = get("//tmp/t1/@modification_time")
        move("//tmp/t2", "//tmp/t1", preserve_modification_time=preserve, force=True)
        assert preserve == (modification_time == get("//tmp/t1/@modification_time"))

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_ignore_ampersand1(self):
        set("//tmp/map", {})
        set("//tmp/map&/a", "b")
        assert get("//tmp/map&/a") == "b"
        assert get("//tmp/map&/@type") == "map_node"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_ignore_ampersand2(self):
        set("//tmp/list", [])
        set("//tmp/list&/end", "x")
        assert get("//tmp/list&/0") == "x"
        assert get("//tmp/list&/@type") == "list_node"

    @authors("babenko", "ignat")
    @not_implemented_in_sequoia
    def test_ignore_ampersand3(self):
        assert get("//sys/chunks&/@type") == "chunk_map"

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_ignore_ampersand4(self):
        assert not exists("//tmp/missing")
        assert not exists("//tmp/missing&")
        assert exists("//tmp")
        assert exists("//tmp&")

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_batch_empty(self):
        assert execute_batch([]) == []

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_batch_error(self):
        results = execute_batch(
            [
                make_batch_request("get", path="//nonexisting1"),
                make_batch_request("get", path="//nonexisting2"),
            ]
        )
        assert len(results) == 2
        with pytest.raises(YtError):
            get_batch_output(results[0])
        with pytest.raises(YtError):
            get_batch_output(results[1])

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_batch_success(self):
        set_results = execute_batch(
            [
                make_batch_request("set", path="//tmp/a", input="a"),
                make_batch_request("set", path="//tmp/b", input="b"),
            ]
        )
        assert len(set_results) == 2
        assert not get_batch_output(set_results[0])
        assert not get_batch_output(set_results[1])

        get_results = execute_batch(
            [
                make_batch_request("get", return_only_value=True, path="//tmp/a"),
                make_batch_request("get", return_only_value=True, path="//tmp/b"),
            ]
        )
        assert len(get_results) == 2
        assert get_batch_output(get_results[0]) == "a"
        assert get_batch_output(get_results[1]) == "b"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_batch_with_concurrency_failure(self):
        with pytest.raises(YtError):
            execute_batch([], concurrency=-1)

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_batch_with_concurrency_success(self):
        for i in range(10):
            set("//tmp/{0}".format(i), i)
        get_results = execute_batch(
            [make_batch_request("get", return_only_value=True, path="//tmp/{0}".format(i)) for i in range(10)],
            concurrency=2,
        )
        assert len(get_results) == 10
        for i in range(10):
            assert get_batch_output(get_results[i]) == i

    @authors("babenko", "shakurov")
    @not_implemented_in_sequoia
    def test_recursive_resource_usage_map(self):
        create("map_node", "//tmp/m")
        for i in range(10):
            set("//tmp/m/" + str(i), i)
        assert get("//tmp/m/@recursive_resource_usage/node_count") == 11
        tx = start_transaction()
        assert get("//tmp/m/@recursive_resource_usage/node_count", tx=tx) == 11

    @authors("babenko", "shakurov")
    @not_implemented_in_sequoia
    def test_recursive_resource_usage_list(self):
        create("list_node", "//tmp/l")
        for i in range(10):
            set("//tmp/l/end", i)
        assert get("//tmp/l/@recursive_resource_usage/node_count") == 11
        tx = start_transaction()
        assert get("//tmp/l/@recursive_resource_usage/node_count", tx=tx) == 11

    @authors("aleksandra-zh")
    @not_implemented_in_sequoia
    def test_master_memory_resource_usage(self):
        create("map_node", "//tmp/m")
        for i in range(10):
            set("//tmp/m/" + str(i), i)

        wait(lambda: get("//tmp/m/@resource_usage/master_memory") > 0)
        wait(lambda: get("//tmp/m/@recursive_resource_usage/master_memory") > 0)

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_prerequisite_transactions(self):
        with pytest.raises(YtError):
            set("//tmp/test_node", {}, prerequisite_transaction_ids=["a-b-c-d"])

        tx = start_transaction()
        set("//tmp/test_node", {}, prerequisite_transaction_ids=[tx])
        remove("//tmp/test_node", prerequisite_transaction_ids=[tx])

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_prerequisite_revisions(self):
        create("map_node", "//tmp/test_node")
        revision = get("//tmp/test_node/@revision")

        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/test_node/inner_node",
                prerequisite_revisions=[
                    {
                        "path": "//tmp/test_node",
                        "transaction_id": "0-0-0-0",
                        "revision": revision + 1,
                    }
                ],
            )

        create(
            "map_node",
            "//tmp/test_node/inner_node",
            prerequisite_revisions=[
                {
                    "path": "//tmp/test_node",
                    "transaction_id": "0-0-0-0",
                    "revision": revision,
                }
            ],
        )

    @authors("babenko")
    # COMPAT(babenko): YT-11903
    @not_implemented_in_sequoia
    def test_move_preserves_creation_time_by_default1(self):
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time == get("//tmp/t2/@creation_time")

    @authors("babenko")
    # COMPAT(babenko): YT-11903
    @not_implemented_in_sequoia
    def test_move_preserves_creation_time_by_default2(self):
        set("//tmp/t1", {"x": "y"})
        creation_time1 = get("//tmp/t1/@creation_time")
        creation_time2 = get("//tmp/t1/x/@creation_time")
        move("//tmp/t1", "//tmp/t2")
        assert creation_time1 == get("//tmp/t2/@creation_time")
        assert creation_time2 == get("//tmp/t2/x/@creation_time")

    @authors("ignat")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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

        with pytest.raises(YtError):
            remove("//tmp/d1/f/i")
        revision8 = get("//tmp/d1/@revision")
        assert revision8 == revision7

        with pytest.raises(YtError):
            remove("//tmp/d1/@value/f/i")
        revision9 = get("//tmp/d1/@revision")
        assert revision9 == revision7

        with pytest.raises(YtError):
            set("//tmp/d1/f/g/h", ["q", "r", "s"])
        revision10 = get("//tmp/d1/@revision")
        assert revision10 == revision7

        with pytest.raises(YtError):
            set("//tmp/d1/@value/f/g/h", ["q", "r", "s"])
        revision11 = get("//tmp/d1/@revision")
        assert revision11 == revision7

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_node_path_map(self):
        set("//tmp/a", 123)
        assert get("//tmp/a/@path") == "//tmp/a"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_node_path_list(self):
        set("//tmp/a", [1, 2, 3])
        assert get("//tmp/a/1/@path") == "//tmp/a/1"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_node_path_map_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", 123, tx=tx)
        assert get("//tmp/a/@path", tx=tx) == "//tmp/a"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_node_path_list_in_tx(self):
        tx = start_transaction()
        set("//tmp/a", [1, 2, 3], tx=tx)
        assert get("//tmp/a/1/@path", tx=tx) == "//tmp/a/1"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_broken_node_path1(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        lock("//tmp/a", tx=tx, mode="snapshot")
        remove("//tmp/a")
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_node_path_with_slash(self):
        set("//tmp/dir", {"my\\t": {}}, force=True)
        assert ls("//tmp/dir") == ["my\\t"]
        # It is double quoted since ypath syntax additionally quote backslash.
        assert get("//tmp/dir/my\\\\t") == {}
        assert get("//tmp/dir/my\\\\t/@path") == "//tmp/dir/my\\\\t"

        set("//tmp/dir", {"my\t": {}}, force=True)
        assert ls("//tmp/dir") == ["my\t"]
        # Non-ascii symbols are expressed in hex format in ypath.
        assert get("//tmp/dir/my\\x09") == {}
        assert get("//tmp/dir/my\\x09/@path") == "//tmp/dir/my\\x09"

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_broken_node_path2(self):
        set("//tmp/a", 123)
        tx = start_transaction()
        node_id = get("//tmp/a/@id")
        remove("//tmp/a", tx=tx)
        assert get("#{}/@path".format(node_id), tx=tx) == "#{}".format(node_id)

    @authors("shakurov")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_inheritable_attributes2(self):
        create_domestic_medium("hdd")
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

        create(
            "table",
            "//tmp/dir1/dir2/t1",
            attributes={
                "primary_medium": primary_medium,
                "tablet_cell_bundle": tablet_cell_bundle,
                "optimize_for": optimize_for,
            },
        )
        assert get("//tmp/dir1/dir2/t1/@compression_codec") == "lz4"
        assert get("//tmp/dir1/dir2/t1/@replication_factor") == 5
        assert get("//tmp/dir1/dir2/t1/@commit_ordering") == "strong"
        assert get("//tmp/dir1/dir2/t1/@erasure_codec") == "reed_solomon_6_3"
        assert not get("//tmp/dir1/dir2/t1/@vital")
        assert get("//tmp/dir1/dir2/t1/@in_memory_mode") == "uncompressed"
        assert get("//tmp/dir1/dir2/t1/@primary_medium") == "hdd"
        assert get("//tmp/dir1/dir2/t1/@tablet_cell_bundle") == "b"
        assert get("//tmp/dir1/dir2/t1/@optimize_for") == "scan"
        assert get("//tmp/dir1/dir2/t1/@media") == {"hdd": {"replication_factor": 5, "data_parts_only": False}}
        assert get("//tmp/dir1/dir2/t1/@atomicity") == "full"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_inheritable_attributes_with_transactions(self):
        create("map_node", "//tmp/dir1")

        tx = start_transaction(timeout=60000)

        set("//tmp/dir1/@compression_codec", "zlib_9", tx=tx)
        assert not exists("//tmp/dir1/@compression_codec")

        create("table", "//tmp/dir1/t1", tx=tx)
        assert get("//tmp/dir1/t1/@compression_codec", tx=tx) == "zlib_9"

        create("table", "//tmp/dir1/t2")
        assert get("//tmp/dir1/t2/@compression_codec") == "lz4"

        with pytest.raises(YtError):
            set("//tmp/dir1/@compression_codec", "snappy")

        set("//tmp/dir1/@erasure_codec", "reed_solomon_6_3")
        assert not exists("//tmp/dir1/t4/@erasure_codec", tx=tx)

        commit_transaction(tx)

        assert get("//tmp/dir1/@compression_codec") == "zlib_9"
        assert get("//tmp/dir1/@erasure_codec") == "reed_solomon_6_3"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_inheritable_attributes_with_nested_transactions(self):
        tx1 = start_transaction(timeout=60000)
        tx2 = start_transaction(tx=tx1, timeout=60000)
        tx3 = start_transaction(tx=tx2, timeout=60000)

        def prepare_dir(dir_base_name, tx):
            dir_name = "//tmp/" + dir_base_name
            create("map_node", dir_name)
            set(dir_name + "/@compression_codec", "lz4", tx=tx)
            with pytest.raises(YtError):
                set(dir_name + "/@erasure_codec", None, tx=tx)
            remove(dir_name + "/@erasure_codec", tx=tx)

        # Non-transactional changes.
        prepare_dir("dir1", "0-0-0-0")
        assert get("//tmp/dir1/@compression_codec", tx=tx3) == "lz4"
        assert not exists("//tmp/dir1/@erasure_codec", tx=tx3)

        # Ancestor transaction changes.
        prepare_dir("dir2", tx1)
        assert get("//tmp/dir2/@compression_codec", tx=tx3) == "lz4"
        assert not exists("//tmp/dir2/@erasure_codec", tx=tx3)

        # Parent transaction changes.
        prepare_dir("dir3", tx2)
        assert get("//tmp/dir3/@compression_codec", tx=tx3) == "lz4"
        assert not exists("//tmp/dir3/@erasure_codec", tx=tx3)

        # Immediate transaction changes.
        prepare_dir("dir4", tx3)
        assert get("//tmp/dir4/@compression_codec", tx=tx3) == "lz4"
        assert not exists("//tmp/dir4/@erasure_codec", tx=tx3)

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_inheritable_attributes_media_validation(self):
        create_domestic_medium("ssd")

        create("map_node", "//tmp/dir1")
        assert not exists("//tmp/dir1/@media")

        # media - primary_medium - replication_factor
        with pytest.raises(YtError):
            set(
                "//tmp/dir1/@media",
                {"default": {"replication_factor": 0, "data_parts_only": False}},
            )
        with pytest.raises(YtError):
            set(
                "//tmp/dir1/@media",
                {"default": {"replication_factor": 3, "data_parts_only": True}},
            )
        set(
            "//tmp/dir1/@media",
            {"default": {"replication_factor": 3, "data_parts_only": False}},
        )
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
        set(
            "//tmp/dir2/@media",
            {
                "default": {"replication_factor": 3, "data_parts_only": False},
                "ssd": {"replication_factor": 5, "data_parts_only": False},
            },
        )
        assert get("//tmp/dir2/@media") == {
            "default": {"replication_factor": 3, "data_parts_only": False},
            "ssd": {"replication_factor": 5, "data_parts_only": False},
        }
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
            set(
                "//tmp/dir3/@media",
                {
                    "default": {"replication_factor": 3, "data_parts_only": True},
                    "ssd": {"replication_factor": 3, "data_parts_only": True},
                },
            )
        set(
            "//tmp/dir3/@media",
            {
                "default": {"replication_factor": 5, "data_parts_only": True},
                "ssd": {"replication_factor": 3, "data_parts_only": False},
            },
        )
        assert get("//tmp/dir3/@media") == {
            "default": {"replication_factor": 5, "data_parts_only": True},
            "ssd": {"replication_factor": 3, "data_parts_only": False},
        }
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
            set(
                "//tmp/dir4/@media",
                {"default": {"replication_factor": 3, "data_parts_only": False}},
            )
        with pytest.raises(YtError):
            set(
                "//tmp/dir4/@media",
                {
                    "default": {"replication_factor": 3, "data_parts_only": False},
                    "ssd": {"replication_factor": 2, "data_parts_only": False},
                },
            )
        set(
            "//tmp/dir4/@media",
            {
                "default": {"replication_factor": 3, "data_parts_only": False},
                "ssd": {"replication_factor": 3, "data_parts_only": False},
            },
        )

        # replication_factor - media - primary_medium
        create("map_node", "//tmp/dir5")
        set("//tmp/dir5/@replication_factor", 3)
        set(
            "//tmp/dir5/@media",
            {
                "default": {"replication_factor": 2, "data_parts_only": False},
                "ssd": {"replication_factor": 4, "data_parts_only": False},
            },
        )
        with pytest.raises(YtError):
            set("//tmp/dir5/@primary_medium", "default")
        with pytest.raises(YtError):
            set("//tmp/dir5/@primary_medium", "ssd")
        set(
            "//tmp/dir5/@media",
            {
                "default": {"replication_factor": 2, "data_parts_only": False},
                "ssd": {"replication_factor": 3, "data_parts_only": False},
            },
        )
        set("//tmp/dir5/@primary_medium", "ssd")

    @authors("shakurov")
    @not_implemented_in_sequoia
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

        remove_tablet_cell_bundle("b1")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b1"))

        remove_tablet_cell_bundle("b2")
        wait(lambda: get("//sys/tablet_cell_bundles/b2/@life_stage") in ["removal_started", "removal_pre_committed"])

        remove("//tmp/dir1")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b1"))

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_inheritable_attributes_no_extraneous_inheritance(self):
        create("map_node", "//tmp/dir1")

        # Most inheritable attributes are inheritable by all chunk owners.
        # These, however, are only inheritable by tables (values don't matter):
        attrs = (
            ("commit_ordering", "strong"),
            ("in_memory_mode", "uncompressed"),
            ("optimize_for", "scan"),
            ("tablet_cell_bundle", "b"),
        )
        create_tablet_cell_bundle("b")
        for attr_name, attr_val in attrs:
            set("//tmp/dir1/@" + attr_name, attr_val)

        # Inheritable by all.
        set("//tmp/dir1/@compression_codec", "lz4")

        create("table", "//tmp/dir1/t1")
        create("file", "//tmp/dir1/f1")
        create("journal", "//tmp/dir1/j1")

        for node in ("t1", "f1"):
            assert get("//tmp/dir1/{0}/@compression_codec".format(node)) == "lz4"

        for attr_name, attr_val in attrs:
            assert get("//tmp/dir1/t1/@" + attr_name) == attr_val
            assert not exists("//tmp/dir1/f1/@" + attr_name)
            assert not exists("//tmp/dir1/j1/@" + attr_name)

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_inheritable_attributes_yt_14207(self):
        create_domestic_medium("m1")
        create("map_node", "//tmp/d1", attributes={"primary_medium": "m1"})
        create("map_node", "//tmp/d1/d2/d3", recursive=True, attributes={"primary_medium": "default"})

        tx = start_transaction()

        create("table", "//tmp/d1/d2/d3/t1", tx=tx)
        create("table", "//tmp/d1/d2/d3/t2", tx=tx)

        assert get("//tmp/d1/d2/d3/t1/@primary_medium", tx=tx) == "default"
        assert get("//tmp/d1/d2/d3/t2/@primary_medium", tx=tx) == "default"

    @authors("savrus")
    @not_implemented_in_sequoia
    def test_create_invalid_type(self):
        with pytest.raises(YtError):
            create("some_invalid_type", "//tmp/s")
        with pytest.raises(YtError):
            create("sorted_dynamic_tablet_store", "//tmp/s")

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_attribute_content_revision(self):
        create("map_node", "//tmp/test_node")
        revision = get("//tmp/test_node/@revision")
        attribute_revision = get("//tmp/test_node/@attribute_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision == attribute_revision
        assert revision == content_revision

        set("//tmp/test_node/@user_attribute1", "value1")
        revision = get("//tmp/test_node/@revision")
        attribute_revision = get("//tmp/test_node/@attribute_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision == attribute_revision
        assert revision > content_revision

        set("//tmp/test_node", {"hello": "world", "list": [0, "a", {}], "n": 1}, force=True)
        revision = get("//tmp/test_node/@revision")
        attribute_revision = get("//tmp/test_node/@attribute_revision")
        content_revision = get("//tmp/test_node/@content_revision")

        assert revision > attribute_revision
        assert revision == content_revision

    @authors("babenko")
    @not_implemented_in_sequoia
    def test_user_attribute_removal1_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")
        set("//tmp/test_node/@user_attribute", "some_string", tx=tx1)

        remove("//tmp/test_node/@user_attribute", tx=tx2)

        assert not exists("//tmp/test_node/@user_attribute", tx=tx2)
        assert exists("//tmp/test_node/@user_attribute", tx=tx1)

    @authors("babenko")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_map_child_removal1_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")
        create("table", "//tmp/test_node/child", tx=tx1)

        remove("//tmp/test_node/child", tx=tx2)

        assert not exists("//tmp/test_node/child", tx=tx2)
        assert exists("//tmp/test_node/child", tx=tx1)

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_map_child_removal2_yt_10192(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        create("map_node", "//tmp/test_node")

        create("table", "//tmp/test_node/child", tx=tx1)
        # NB: this isn't exactly equivalent to
        # test_user_attribute_removal2_yt_10192, but since one can't
        # directly create a map node's child via 'set', this is the
        # closest we can get.
        set("//tmp/test_node", {"child": "some_string"}, tx=tx2, force=True)
        remove("//tmp/test_node/child", tx=tx2)
        assert not exists("//tmp/test_node/child", tx=tx2)

        copy("//tmp/test_node", "//tmp/copy0")
        assert not exists("//tmp/copy0/child")

        copy("//tmp/test_node", "//tmp/copy1", tx=tx1)
        assert exists("//tmp/copy1/child", tx=tx1)

        copy("//tmp/test_node", "//tmp/copy2", tx=tx2)
        assert not exists("//tmp/copy0/child", tx=tx2)

    @authors("shakurov")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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

    @authors("aleksandra-zh")
    @not_implemented_in_sequoia
    def test_preserve_owner_under_tx_yt_15292(self):
        create_user("u")

        tx = start_transaction(authenticated_user="u")

        create("table", "//tmp/t", authenticated_user="u", tx=tx)
        copy("//tmp/t", "//tmp/t1", authenticated_user="u", tx=tx)
        copy("//tmp/t", "//tmp/t2", authenticated_user="u", tx=tx, preserve_owner=True)

        commit_transaction(tx)

        assert get("//tmp/t/@owner") == "u"
        assert get("//tmp/t1/@owner") == "u"
        assert get("//tmp/t2/@owner") == "u"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_builtin_versioned_attributes(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "optimize_for": "lookup",
                "compression_codec": "zlib_6",
                "erasure_codec": "reed_solomon_6_3",
            },
        )

        assert get("//tmp/t1/@optimize_for") == "lookup"
        assert get("//tmp/t1/@compression_codec") == "zlib_6"
        assert get("//tmp/t1/@erasure_codec") == "reed_solomon_6_3"
        assert not get("//tmp/t1/@enable_striped_erasure")
        assert not get("//tmp/t1/@enable_skynet_sharing")
        assert get("//tmp/t1/@chunk_merger_mode") == "none"

        tx = start_transaction()

        set("//tmp/t1/@optimize_for", "scan", tx=tx)
        set("//tmp/t1/@compression_codec", "lz4", tx=tx)
        set("//tmp/t1/@erasure_codec", "lrc_12_2_2", tx=tx)
        set("//tmp/t1/@enable_striped_erasure", True, tx=tx)
        set("//tmp/t1/@enable_skynet_sharing", True, tx=tx)
        set("//tmp/t1/@chunk_merger_mode", "deep", tx=tx)

        assert get("//tmp/t1/@optimize_for") == "lookup"
        assert get("//tmp/t1/@compression_codec") == "zlib_6"
        assert get("//tmp/t1/@erasure_codec") == "reed_solomon_6_3"
        assert not get("//tmp/t1/@enable_striped_erasure")
        assert not get("//tmp/t1/@enable_skynet_sharing")
        assert get("//tmp/t1/@chunk_merger_mode") == "none"

        assert get("//tmp/t1/@optimize_for", tx=tx) == "scan"
        assert get("//tmp/t1/@compression_codec", tx=tx) == "lz4"
        assert get("//tmp/t1/@erasure_codec", tx=tx) == "lrc_12_2_2"
        assert get("//tmp/t1/@enable_striped_erasure", tx=tx)
        assert get("//tmp/t1/@enable_skynet_sharing", tx=tx)
        assert get("//tmp/t1/@chunk_merger_mode", tx=tx) == "deep"

        commit_transaction(tx)

        assert get("//tmp/t1/@optimize_for") == "scan"
        assert get("//tmp/t1/@compression_codec") == "lz4"
        assert get("//tmp/t1/@erasure_codec") == "lrc_12_2_2"
        assert get("//tmp/t1/@enable_striped_erasure")
        assert get("//tmp/t1/@enable_skynet_sharing")
        assert get("//tmp/t1/@chunk_merger_mode") == "deep"

    @authors("avmatrosov")
    @not_implemented_in_sequoia
    def test_annotation_attribute(self):
        create("map_node", "//tmp/test_node")
        create("map_node", "//tmp/test_node/child")
        set("//tmp/test_node/@annotation", "test_node")
        assert get("//tmp/test_node/@annotation") == "test_node"
        assert get("//tmp/test_node/child/@annotation") == "test_node"
        assert get("//tmp/test_node/child/@annotation_path") == "//tmp/test_node"

        create("map_node", "//tmp/parent")
        create("map_node", "//tmp/parent/empty_node")
        set("//tmp/parent/@annotation", "tmp")

        assert get("//tmp/test_node/@annotation") == get("//tmp/test_node/child/@annotation") == "test_node"
        assert get("//tmp/parent/empty_node/@annotation") == "tmp"
        assert get("//tmp/parent/empty_node/@annotation_path") == "//tmp/parent"

        remove("//tmp/parent/@annotation")
        assert get("//tmp/parent/empty_node/@annotation") == yson.YsonEntity()

        set("//tmp/parent/@annotation", "test")
        set("//tmp/parent/@annotation", None)
        assert get("//tmp/parent/@annotation") == yson.YsonEntity()

    @authors("avmatrosov")
    @not_implemented_in_sequoia
    def test_annotation_errors(self):
        create("map_node", "//tmp/test_node")
        assert get("//tmp/test_node/@annotation") == yson.YsonEntity()
        assert get("//tmp/test_node/@annotation_path") == yson.YsonEntity()
        with pytest.raises(YtError):
            set("//tmp/test_node/@annotation", "a" * 1025)
        with pytest.raises(YtError):
            set("//tmp/test_node/@annotation", chr(255))
        set("//tmp/test_node/@annotation", printable)

    @authors("avmatrosov")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_annotation_clone(self):
        create("map_node", "//tmp/test_node")
        create("map_node", "//tmp/test_node/child")
        set("//tmp/test_node/@annotation", "test")
        move("//tmp/test_node", "//test")
        assert get("//test/@annotation") == get("//test/child/@annotation") == "test"

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_recursive_copy_sets_parent_on_branched_node(self):
        create_user("u")

        tx = start_transaction(authenticated_user="u")

        create("table", "//tmp/d1/d2/src/t", tx=tx, recursive=True, authenticated_user="u")
        copy(
            "//tmp/d1/d2/src",
            "//tmp/d1/d2/dst",
            tx=tx,
            recursive=True,
            authenticated_user="u",
        )

        tx2 = start_transaction(tx=tx, authenticated_user="u")
        # Must not throw.
        lock("//tmp/d1/d2/dst/t", tx=tx2, mode="snapshot", authenticated_user="u")

    @authors("avmatrosov")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_preserve_acl(self):
        create("table", "//tmp/t1")

        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//tmp/t2", preserve_acl=True)

        assert not get("//tmp/t2/@inherit_acl")
        assert_items_equal(get("//tmp/t2/@acl"), acl)

    @authors("avmatrosov")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
    def test_lock_existing_create(self):
        tx = start_transaction()
        create("table", "//tmp/x")
        create("table", "//tmp/x", tx=tx, ignore_existing=True, lock_existing=True)
        assert len(get("//tmp/x/@locks")) == 1
        commit_transaction(tx)
        assert len(get("//tmp/x/@locks")) == 0

    @authors("avmatrosov")
    @not_implemented_in_sequoia
    def test_lock_existing_copy(self):
        tx = start_transaction()
        create("table", "//tmp/x")
        create("table", "//tmp/m")
        copy("//tmp/m", "//tmp/x", tx=tx, ignore_existing=True, lock_existing=True)
        assert len(get("//tmp/x/@locks")) == 1
        commit_transaction(tx)
        assert len(get("//tmp/x/@locks")) == 0

    @authors("avmatrosov")
    @not_implemented_in_sequoia
    def test_lock_existing_errors(self):
        create("table", "//tmp/x")
        create("table", "//tmp/x1")
        with pytest.raises(YtError):
            create("map_node", "//tmp/x", lock_existing=True)
        with pytest.raises(YtError):
            move("//tmp/x", "//tmp/x1", ignore_existing=True, lock_existing=True)

    @authors("babenko")
    @not_implemented_in_sequoia
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

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes(self):
        multiset_attributes("//tmp/@", {"a": 1, "b": 2})
        assert get("//tmp/@a") == 1
        assert get("//tmp/@b") == 2
        multiset_attributes("//tmp/@", {"b": 3, "c": 4})
        assert get("//tmp/@a") == 1
        assert get("//tmp/@b") == 3
        assert get("//tmp/@c") == 4

        set("//tmp/@m", {})

        multiset_attributes("//tmp/@m", {"x": 5, "y": 6})
        assert get("//tmp/@m/x", 5)
        assert get("//tmp/@m/y", 6)

        multiset_attributes("//tmp/@", {"m/y": 7, "m/z": 8})
        assert get("//tmp/@m/x", 5)
        assert get("//tmp/@m/y", 7)
        assert get("//tmp/@m/y", 8)

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes_invalid(self):
        with pytest.raises(YtError):
            multiset_attributes("//tmp", {"a": 1})
        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"": 2})
        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"a/b": 3})
        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"ref_counter": 5})

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes_nonatomicity(self):
        # This test relies on subrequests execution in lexicographic sorted order.
        # It might be changed in the future.

        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"a": 1, "b/x": 2, "c": 3})
        attributes = get("//tmp/@")
        assert attributes["a"] == 1
        assert "b" not in attributes
        assert "c" not in attributes

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes_tricky(self):
        # Result of these commands depends on order of subrequests execution.
        # We assume that it's undefined, so check that result is consistent
        # with some ordering of subrequests and that master doesn't crash.

        try:
            multiset_attributes("//tmp/@", {"a": {}, "a/b": 1})
            assert get("//tmp/@a") == {"b": 1}
        except YtError:
            assert "a" not in get("//tmp/@a")

        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"a": 1, "id": 2, "z": 3})

        attributes = get("//tmp/@")
        if "a" in attributes:
            assert attributes["a"] == 1
        if "c" in attributes:
            assert attributes["c"] == 3

        set("//tmp/@a", {})
        multiset_attributes("//tmp/@", {"a": {"b": 2}, "a/b": 3})
        assert get("//tmp/@a/b") == 2 or get("//tmp/@a/b") == 3

        multiset_attributes("//tmp/@", {"a": 5})
        assert get("//tmp/@a") == 4 or get("//tmp/@a") == 5

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes_permissions(self):
        create_user("u1")
        create_user("u2")
        create("document", "//tmp/doc", authenticated_user="u1")
        set("//tmp/doc/@acl", [make_ace("deny", "u2", "write")])

        multiset_attributes("//tmp/doc/@", {"a": 1}, authenticated_user="u1")
        assert get("//tmp/doc/@a") == 1

        with pytest.raises(YtError):
            multiset_attributes("//tmp/doc/@", {"a": 2}, authenticated_user="u2")
        assert get("//tmp/doc/@a") == 1

        multiset_attributes("//tmp/doc/@", {"a": 3})
        assert get("//tmp/doc/@a") == 3

    @authors("gritukan")
    @not_implemented_in_sequoia
    def test_multiset_attributes_transaction(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        multiset_attributes("//tmp/@", {"a": 1}, tx=tx1)
        multiset_attributes("//tmp/@", {"b": 2}, tx=tx2)
        commit_transaction(tx1)
        commit_transaction(tx2)
        assert get("//tmp/@a") == 1
        assert get("//tmp/@b") == 2

        tx3 = start_transaction()
        tx4 = start_transaction()
        multiset_attributes("//tmp/@", {"a": 3}, tx=tx3)
        with pytest.raises(YtError):
            multiset_attributes("//tmp/@", {"a": 4}, tx=tx4)
        commit_transaction(tx3)
        assert get("//tmp/@a") == 3
        assert get("//tmp/@b") == 2

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_deprecated_compression_codec(self):
        create("table", "//tmp/t1")
        with pytest.raises(YtError):
            set("//tmp/t1/@compression_codec", "gzip_normal")
        remove("//tmp/t1")

        set("//sys/@config/chunk_manager/deprecated_codec_ids", [])
        create("table", "//tmp/t1")
        remove("//tmp/t1")

        set("//sys/@config/chunk_manager/deprecated_codec_name_to_alias", {})
        create("table", "//tmp/t1", attributes={"compression_codec": "gzip_normal"})
        remove("//tmp/t1")

        create("table", "//tmp/t1")
        set("//tmp/t1/@compression_codec", "gzip_normal")
        remove("//tmp/t1")

    @authors("cookiedoth")
    @pytest.mark.parametrize(
        "create_object,object_map",
        [(create_account, "//sys/accounts"), (create_tablet_cell_bundle, "//sys/tablet_cell_bundles")])
    @not_implemented_in_sequoia
    def test_abc(self, create_object, object_map):
        create_object("sample")
        with pytest.raises(YtError):
            set("{}/sample/@abc".format(object_map), {"id": 42})
        set("{}/sample/@abc".format(object_map), {"id": 42, "slug": "text"})
        with pytest.raises(YtError):
            remove("{}/sample/@abc/id".format(object_map))
        with pytest.raises(YtError):
            remove("{}/sample/@abc/slug".format(object_map))
        with pytest.raises(YtError):
            set("{}/sample/@abc/id".format(object_map), -1)
        with pytest.raises(YtError):
            set("{}/sample/@abc/id".format(object_map), 0)
        with pytest.raises(YtError):
            set("{}/sample/@abc/name".format(object_map), "")
        set("{}/sample/@abc/name".format(object_map), "abacaba")
        remove("{}/sample/@abc/name".format(object_map))
        set("{}/sample/@abc/id".format(object_map), 43)
        remove("{}/sample/@abc".format(object_map))

    @authors("cookiedoth")
    @pytest.mark.parametrize(
        "create_object,object_map",
        [(create_account, "//sys/accounts"), (create_tablet_cell_bundle, "//sys/tablet_cell_bundles")])
    @not_implemented_in_sequoia
    def test_abc_other_fields(self, create_object, object_map):
        create_object("sample")
        set("{}/sample/@abc".format(object_map), {"id": 42, "slug": "text"})
        set("{}/sample/@abc/foo".format(object_map), "bar")
        assert not exists("{}/sample/@abc/foo".format(object_map))

    @authors("cookiedoth")
    @pytest.mark.parametrize(
        "create_object,object_map",
        [(create_account, "//sys/accounts"), (create_tablet_cell_bundle, "//sys/tablet_cell_bundles")])
    @not_implemented_in_sequoia
    def test_folder_id(self, create_object, object_map):
        create_object("sample")
        with pytest.raises(YtError):
            set("{}/sample/@folder_id".format(object_map), "abacaba" * 42)
        set("{}/sample/@folder_id".format(object_map), "b7189bb3-fcf3-46da-accf-52be0d4148f0")
        with pytest.raises(YtError):
            set("{}/sample/@folder_id".format(object_map), 0)

    @authors("cookiedoth")
    @pytest.mark.parametrize(
        "create_object,object_map",
        [(create_account, "//sys/accounts"), (create_tablet_cell_bundle, "//sys/tablet_cell_bundles")])
    @not_implemented_in_sequoia
    def test_abc_remove(self, create_object, object_map):
        create_object("sample")
        assert not exists("{}/sample/@abc".format(object_map))
        set("{}/sample/@abc".format(object_map), {"id": 42, "slug": "text"})
        assert exists("{}/sample/@abc".format(object_map))
        remove("{}/sample/@abc".format(object_map))
        assert not exists("{}/sample/@abc".format(object_map))

    @authors("cookiedoth")
    @pytest.mark.parametrize(
        "create_object,object_map",
        [(create_account, "//sys/accounts"), (create_tablet_cell_bundle, "//sys/tablet_cell_bundles")])
    @not_implemented_in_sequoia
    def test_folder_id_remove(self, create_object, object_map):
        create_object("sample")
        assert not exists("{}/sample/@folder_id".format(object_map))
        set("{}/sample/@folder_id".format(object_map), "b7189bb3-fcf3-46da-accf-52be0d4148f0")
        assert exists("{}/sample/@folder_id".format(object_map))
        remove("{}/sample/@folder_id".format(object_map))
        assert not exists("{}/sample/@folder_id".format(object_map))

    @authors("cookiedoth")
    @not_implemented_in_sequoia
    def test_force(self):
        create("map_node", "//tmp/a")
        with pytest.raises(YtError):
            set("//tmp/a", {"x": "y"})
        set("//tmp/a", {"x": "y"}, force=True)

    @authors("kvk1920")
    @not_implemented_in_sequoia
    def test_cluster_connection_attribute(self):
        with raises_yt_error("Cannot parse"):
            set("//sys/@cluster_connection", {"default_input_row_limit": "abacaba"})
        set("//sys/@cluster_connection", {"default_input_row_limit": 1024})
        assert {"default_input_row_limit": 1024} == get("//sys/@cluster_connection")
        set("//sys/@cluster_connection", yson.YsonEntity())
        assert isinstance(get("//sys/@cluster_connection"), yson.YsonEntity)

    @authors("kvk1920")
    @not_implemented_in_sequoia
    def test_cluster_name(self):
        with raises_yt_error("too long"):
            set("//sys/@cluster_name", "a" * 129)
        set("//sys/@cluster_name", "a" * 128)
        assert "a" * 128 == get("//sys/@cluster_name")
        with raises_yt_error("ASCII"):
            set("//sys/@cluster_name", "кириллица")

    @authors("h0pless")
    @not_implemented_in_sequoia
    def test_error_on_transaction_abort(self):
        set("//tmp/node", 42)

        tx = start_transaction()
        lock("//tmp/node", mode="snapshot", tx=tx)
        object_id = get("//tmp/node/@id")

        remove("//tmp/node")

        # Can't access node by path
        with raises_yt_error("Node //tmp has no child with key \"node\""):
            get("//tmp/node")

        # But it's possible using object_id
        assert get("#{}".format(object_id)) == 42
        assert get("#{}".format(object_id), tx=tx) == 42

        abort_transaction(tx)

        # Check that we always receive correct errors
        with raises_yt_error("No such transaction"):
            get("//tmp/node", tx=tx) == 42
        with raises_yt_error("No such object"):
            get("#{}".format(object_id))
        with raises_yt_error("No such transaction"):
            get("#{}".format(object_id), tx=tx)


##################################################################


class TestCypressMulticell(TestCypress):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_multiset_attributes_external(self):
        set("//sys/accounts/tmp/@resource_limits/tablet_count", 10)
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )
        table_id = create_dynamic_table("//tmp/t", schema=schema, external_cell_tag=11)

        assert get("//tmp/t/@vital")
        assert get("//tmp/t/@enable_dynamic_store_read")

        multiset_attributes("//tmp/t/@", {"enable_dynamic_store_read": False, "vital": False})

        assert not get("//tmp/t/@vital")
        assert not get("//tmp/t/@enable_dynamic_store_read")

        assert not get(f"#{table_id}/@vital", driver=get_driver(1))
        # The attribute is external but let's check anyway.
        assert not get(f"#{table_id}/@enable_dynamic_store_read", driver=get_driver(1))

    @authors("babenko", "shakurov")
    @not_implemented_in_sequoia
    def test_zero_external_cell_bias(self):
        # Unfortunately, it's difficult to actually check anything here.
        create("table", "//tmp/t", attributes={"external_cell_bias": 0.0})
        assert not exists("//tmp/t/@external_cell_bias")

    @authors("babenko", "shakurov")
    @not_implemented_in_sequoia
    def test_nonzero_external_cell_bias(self):
        # Unfortunately, it's difficult to actually check anything here.
        create("table", "//tmp/t", attributes={"external_cell_bias": 3.0})
        assert not exists("//tmp/t/@external_cell_bias")


##################################################################


class TestCypressPortal(TestCypressMulticell):
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("h0pless")
    def test_cyclic_link_through_portal(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        create("map_node", "//portals/p/a/b/c", recursive=True)
        link("//portals/p/a/b/c", "//portals/p/a/l1")
        create("map_node", "//portals/p/r")
        link("//portals/p/r", "//portals/p/r/l2")
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//portals/p/a/l1", "//portals/p/a/b/c", force=True)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//portals/p/r/l2", "//portals/p/r/l2", force=True)

    @authors("h0pless")
    def test_node_copy_rollback(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})
        create("table", "//tmp/t", attributes={"external_cell_tag": 13})

        # This will provoke a rollback.
        set("//sys/@config/cypress_manager/max_locks_per_transaction_subtree", 0)
        with raises_yt_error("Cannot create \"exclusive\" lock for node"):
            copy("//tmp/t", "//portals/p/t")

        remove("//sys/@config/cypress_manager/max_locks_per_transaction_subtree")

    @authors("shakurov")
    def test_cross_shard_copy_inhertible_attributes(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        create_tablet_cell_bundle("b")
        create("map_node", "//tmp/d", attributes={
            "compression_codec": "snappy",
            "tablet_cell_bundle": "b"
        })

        copy("//tmp/d", "//portals/p/d1")
        assert get("//portals/p/d1/@compression_codec") == "snappy"
        assert get("//portals/p/d1/@tablet_cell_bundle") == "b"

        tx = start_transaction()

        copy("//tmp/d", "//portals/p/d2", tx=tx)
        assert get("//portals/p/d2/@compression_codec", tx=tx) == "snappy"
        assert get("//portals/p/d2/@tablet_cell_bundle", tx=tx) == "b"

        commit_transaction(tx)

        assert get("//portals/p/d2/@compression_codec") == "snappy"
        assert get("//portals/p/d2/@tablet_cell_bundle") == "b"

    @authors("aleksandra-zh")
    def test_cross_shard_copy_builtin_attributes(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        create(
            "table",
            "//tmp/t1",
            attributes={
                "optimize_for": "lookup",
                "compression_codec": "zlib_6",
                "erasure_codec": "reed_solomon_6_3",
                "enable_striped_erasure": True,
                "enable_skynet_sharing": True,
                "external_cell_tag": 13
            },
        )

        copy("//tmp/t1", "//portals/p/t1")

        assert get("//portals/p/t1/@optimize_for") == "lookup"
        assert get("//portals/p/t1/@compression_codec") == "zlib_6"
        assert get("//portals/p/t1/@erasure_codec") == "reed_solomon_6_3"
        assert get("//portals/p/t1/@enable_striped_erasure")
        assert get("//portals/p/t1/@enable_skynet_sharing")

        copy("//portals/p/t1", "//tmp/t2")

        assert get("//tmp/t2/@optimize_for") == "lookup"
        assert get("//tmp/t2/@compression_codec") == "zlib_6"
        assert get("//tmp/t2/@erasure_codec") == "reed_solomon_6_3"
        assert get("//tmp/t2/@enable_striped_erasure")
        assert get("//tmp/t2/@enable_skynet_sharing")

    @authors("shakurov")
    def test_cross_shard_copy_w_tx(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        tx = start_transaction()
        create("table", "//portals/p/t1", tx=tx, attributes={"external_cell_tag": 13})
        # Must not crash.
        move("//portals/p/t1", "//tmp/t1_copy", tx=tx)

        create("table", "//tmp/t2", tx=tx, attributes={"external_cell_tag": 13})
        # Must not crash.
        move("//tmp/t2", "//portals/p/t2_copy", tx=tx)

    @authors("cherepashka")
    @not_implemented_in_sequoia
    def test_access_time_in_shard_copy(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/t1")
        creation_time = get("//tmp/t1/@access_time")
        copy("//tmp/t1", "//portals/p/t2")
        time.sleep(1)
        assert get("//tmp/t1/@access_time") > creation_time

    @authors("avmatrosov")
    def test_annotation_portal(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 1000)
        set("//portals/@annotation", "test")

        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})
        create("map_node", "//portals/p/test")

        assert get("//portals/p/test/@annotation") == "test"

        set("//portals/@annotation", "")
        wait(lambda: get("//portals/p/test/@annotation") == "")

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
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        # test cross-cell copy
        copy("//tmp/doc", "//portals/p/doc", preserve_owner=True)
        assert get("//tmp/doc/@owner") == get("//portals/p/doc/@owner") == "u1"

    @authors("avmatrosov")
    def test_preserve_acl(self):
        create("document", "//tmp/t1")
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})

        set("//tmp/t1/@inherit_acl", False)
        acl = [make_ace("deny", "guest", "write")]
        set("//tmp/t1/@acl", acl)
        copy("//tmp/t1", "//portals/p/t2", preserve_acl=True)

        assert not get("//portals/p/t2/@inherit_acl")
        assert_items_equal(get("//portals/p/t2/@acl"), acl)

################################################################################


class TestCypressShardedTx(TestCypressPortal):
    NUM_SECONDARY_MASTER_CELLS = 4
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["transaction_coordinator", "chunk_host"]},
        "14": {"roles": ["transaction_coordinator"]},
    }


class TestCypressShardedTxCTxS(TestCypressShardedTx):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


class TestCypressNoLocalReadExecutor(TestCypress):
    def setup_method(self, method):
        super(TestCypressNoLocalReadExecutor, self).setup_method(method)
        set("//sys/@config/object_service/enable_local_read_executor", False)


class TestCypressCypressProxy(TestCypressShardedTx):
    NUM_CYPRESS_PROXIES = 2


##################################################################


class TestCypressRpcProxy(TestCypress):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestCypressMulticellRpcProxy(TestCypressMulticell, TestCypressRpcProxy):
    pass


##################################################################


class TestCypressLeaderSwitch(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    def _switch_leader(self):
        old_leader_rpc_address = get_active_primary_master_leader_address(self)
        new_leader_rpc_address = get_active_primary_master_follower_address(self)
        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_rpc_address)
        wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))

    @authors("shakurov")
    @flaky(max_runs=3)
    def test_expiration_timeout_leader_switch(self):
        create("table", "//tmp/t", attributes={"expiration_timeout": 2000})

        self._switch_leader()

        time.sleep(2.0)
        assert not exists("//tmp/t")


##################################################################

class TestCypressForbidSet(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

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

        set(
            "//tmp/dir/@acl/end",
            {"action": "allow", "subjects": ["root"], "permissions": ["write"]},
        )
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
        input_stream = BytesIO(kwargs.pop("input")) if "input" in kwargs else None
        return yson.loads(execute_command(command, kwargs, input_stream=input_stream))

    @authors("levysotsky")
    def test_create(self):
        assert "node_id" in self._execute("create", type="map_node", path="//tmp/dir")

    @authors("levysotsky")
    def test_set_and_get(self):
        assert self._execute("set", path="//tmp/a", input=b'{"b"= 3}') == {}
        assert self._execute("get", path="//tmp/a") == {"value": {"b": 3}}

    @authors("levysotsky")
    def test_list(self):
        self._execute("set", path="//tmp/map", input=b'{"a"= 1; "b"= 2; "c"= 3}')
        assert self._execute("list", path="//tmp/map") == {"value": ["a", "b", "c"]}

    @authors("levysotsky")
    def test_transaction(self):
        tx_result = self._execute("start_transaction")
        assert "transaction_id" in tx_result

    @authors("levysotsky", "shakurov")
    def test_lock_unlock(self):
        self._execute("set", path="//tmp/a", input=b'{"a"= 1}', force=True)
        result = self._execute("start_transaction")
        tx = result["transaction_id"]
        lock_result = self._execute("lock", path="//tmp/a", transaction_id=tx)
        assert "lock_id" in lock_result and "node_id" in lock_result
        assert len(get("//tmp/a/@locks")) == 1
        with pytest.raises(YtError):
            self._execute("set", path="//tmp/a", input=b'{"a"=2}', force=True)
        self._execute("unlock", path="//tmp/a", transaction_id=tx)
        assert len(get("//tmp/a/@locks")) == 0
        self._execute("set", path="//tmp/a", input=b'{"a"=3}', force=True)


##################################################################


class TestCypressNestingLevelLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    DEPTH_LIMIT = 80

    DELTA_DRIVER_CONFIG = {
        "cypress_write_yson_nesting_level_limit": DEPTH_LIMIT,
    }

    @staticmethod
    def _create_deep_object(depth):
        result = {}
        current = result
        for _ in range(depth):
            current["a"] = {}
            current = current["a"]
        return result

    def _wrap(f):
        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    set = _wrap(set)
    get = _wrap(get)
    create = _wrap(create)
    multiset_attributes = _wrap(multiset_attributes)

    @authors("levysotsky")
    def test_set(self):
        create("map_node", "//tmp/node")
        good_depth = self.DEPTH_LIMIT - 2
        good_obj = self._create_deep_object(good_depth)
        self.set("//tmp/node/@attr1", good_obj)

        bad_obj = self._create_deep_object(self.DEPTH_LIMIT + 1)
        with raises_yt_error("Depth limit exceeded"):
            self.set("//tmp/node/@attr2", bad_obj)

        extra_depth = 10
        for i in range(extra_depth):
            self.set("//tmp/node/@attr3" + "/a" * i, {})
        # Deep attributes can exist and be read.
        self.set("//tmp/node/@attr3" + "/a" * extra_depth, good_obj)
        assert self.get("//tmp/node/@attr3") == self._create_deep_object(good_depth + extra_depth)

    @authors("levysotsky")
    def test_create(self):
        good_depth = self.DEPTH_LIMIT - 2
        good_obj = self._create_deep_object(good_depth)
        self.create("map_node", "//tmp/node1", attributes={"attr1": good_obj})

        bad_obj = self._create_deep_object(self.DEPTH_LIMIT + 1)
        with raises_yt_error("Depth limit exceeded"):
            self.create("map_node", "//tmp/node2", attributes={"attr2": bad_obj})

    @authors("levysotsky")
    def test_multiset_attributes(self):
        create("map_node", "//tmp/node")
        good_depth = self.DEPTH_LIMIT - 2
        good_obj = self._create_deep_object(good_depth)
        self.multiset_attributes("//tmp/node/@", {"attr1": good_obj})

        bad_obj = self._create_deep_object(self.DEPTH_LIMIT + 1)
        with raises_yt_error("Depth limit exceeded"):
            self.set("//tmp/node/@", {"attr2": bad_obj})


##################################################################


class TestCypressNestingLevelLimitRpcProxy(TestCypressNestingLevelLimit):
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    DRIVER_BACKEND = "rpc"

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "cypress_write_yson_nesting_level_limit": TestCypressNestingLevelLimit.DEPTH_LIMIT,
        }
    }


##################################################################


class TestCypressNestingLevelLimitHttpProxy(TestCypressNestingLevelLimit):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_PROXY_CONFIG = {
        "cluster_connection": {
            "cypress_write_yson_nesting_level_limit": TestCypressNestingLevelLimit.DEPTH_LIMIT,
        }
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _execute_command(self, method, command_name, params, input_stream=None):
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Input-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }
        rsp = requests.request(
            method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
            data=input_stream,
        )
        if "X-YT-Error" in rsp.headers:
            assert "X-YT-Framing" not in rsp.headers
            raise YtResponseError(json.loads(rsp.headers.get("X-YT-Error")))
        rsp.raise_for_status()
        return yson.loads(rsp.content)

    def set(self, path, value, **kwargs):
        kwargs["path"] = path
        value = yson.dumps(value)
        self._execute_command("PUT", "set", kwargs, input_stream=BytesIO(value))

    def get(self, path, **kwargs):
        kwargs["path"] = path
        return self._execute_command("GET", "get", kwargs)["value"]

    def create(self, object_type, path, **kwargs):
        kwargs["type"] = object_type
        kwargs["path"] = path
        self._execute_command("POST", "create", kwargs)

    def multiset_attributes(self, path, subrequests, **kwargs):
        subrequests = yson.dumps(subrequests)
        kwargs["path"] = path
        self._execute_command("PUT", "multiset_attributes", kwargs, input_stream=BytesIO(subrequests))


class TestBuiltinAttributesRevision(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    @authors("kvk1920")
    def test_changing_builtin_attribute_increases_revision_yt_16764(self):
        set("//sys/accounts/tmp/@resource_limits/tablet_count", 1)
        sync_create_cells(1)
        create_user("u")
        create("table", "//tmp/t", attributes={"dynamic": True, "schema": [
            {"name": "ShardId", "type": "uint64", "sort_order": "ascending"},
            {"name": "Offset", "type": "uint64"},
        ]})
        sync_mount_table("//tmp/t")

        attributes = {
            "treat_as_queue_consumer": True,
            "queue_agent_stage": "testing",
            "owner": "u",
            "vital_queue_consumer": True,
            "acl": [make_ace("deny", "guest", "write")]
        }

        for attr, value in attributes.items():
            old_revision = get("//tmp/t/@attribute_revision")
            set(f"//tmp/t/@{attr}", value)
            assert get(f"//tmp/t/@{attr}") == value
            assert get("//tmp/t/@attribute_revision") > old_revision


##################################################################


class TestAccessControlObjects(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 1

    @authors("shakurov")
    def test_access_control_object_creation(self):
        # Namespace is required.
        with pytest.raises(YtError):
            execute_command("create", {
                "type": "access_control_object",
                "attributes": {
                    "name": "garfield"
                }
            })

        with pytest.raises(YtError):
            create_access_control_object("garfield", "bears")

        create_access_control_object_namespace("cats")
        assert exists("//sys/access_control_object_namespaces/cats")
        assert exists("//sys/access_control_object_namespaces/cats/@name")

        create_access_control_object("garfield", "cats")
        assert exists("//sys/access_control_object_namespaces/cats/garfield")
        assert get("//sys/access_control_object_namespaces/cats/garfield/@name") == "garfield"
        assert get("//sys/access_control_object_namespaces/cats/garfield/@namespace") == "cats"
        assert exists("//sys/access_control_object_namespaces/cats/garfield/principal")
        assert exists("//sys/access_control_object_namespaces/cats/garfield/principal/@acl")
        assert exists("//sys/access_control_object_namespaces/cats/garfield/principal/@owner")

        # @namespace is read-only.
        create_access_control_object_namespace("dogs")
        with pytest.raises(YtError):
            set("//sys/access_control_object_namespaces/cats/garfield/@namespace", "dogs")

        create_access_control_object("tom", "cats")
        assert sorted(ls("//sys/access_control_object_namespaces/cats")) == ["garfield", "tom"]

    @authors("shakurov", "ni-stoiko")
    def test_access_control_object_removal(self):
        create_access_control_object_namespace("cats")
        create_access_control_object_namespace("dogs")
        create_access_control_object("tom", "cats")
        create_access_control_object("garfield", "cats")
        create_access_control_object("snoopy", "dogs")
        create_access_control_object("spike", "dogs")

        with raises_yt_error("Cannot remove non-empty composite node"):
            remove("//sys/access_control_object_namespaces/cats", recursive=False)

        remove("//sys/access_control_object_namespaces/cats/tom")
        assert ls("//sys/access_control_object_namespaces/cats") == ["garfield"]

        remove("//sys/access_control_object_namespaces/cats/garfield")
        assert ls("//sys/access_control_object_namespaces/cats") == []

        remove("//sys/access_control_object_namespaces/cats")
        assert "cats" not in ls("//sys/access_control_object_namespaces")
        assert "dogs" in ls("//sys/access_control_object_namespaces")

        remove("//sys/access_control_object_namespaces/dogs", force=True)
        assert "dogs" not in ls("//sys/access_control_object_namespaces")

    @authors("shakurov")
    def test_access_control_object_permissions(self):
        create_user("dog")
        create_user("cat")
        create_user("rat")

        create_access_control_object_namespace(
            "animal_shelters",
            attributes={"acl": [
                make_ace("allow", "dog", "read"),
                make_ace("allow", "cat", "read"),
                make_ace("deny", "rat", "read")
            ]})

        create_access_control_object(
            "dogs_house",
            "animal_shelters",
            attributes={
                "acl": [
                    make_ace("allow", "dog", "write"),
                    make_ace("deny", "cat", "read"),
                ]
            })

        get("//sys/access_control_object_namespaces/animal_shelters/@acl")
        get("//sys/access_control_object_namespaces/animal_shelters/dogs_house/@acl")
        get("//sys/access_control_object_namespaces/animal_shelters/dogs_house/@effective_acl")

        assert check_permission("dog", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house")["action"] == "allow"
        assert check_permission("dog", "write", "//sys/access_control_object_namespaces/animal_shelters/dogs_house")["action"] == "allow"
        assert check_permission("cat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house")["action"] == "deny"

        assert check_permission("rat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house")["action"] == "deny"
        set("//sys/access_control_object_namespaces/animal_shelters/@acl/2", make_ace("allow", "rat", "read"))
        assert check_permission("rat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house")["action"] == "allow"

        assert not get("//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal/@inherit_acl")
        with pytest.raises(YtError):
            set("//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal/@inherit_acl", True)

        # Principal ACL inherits nothing, so this should still be denied.
        assert check_permission("rat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal")["action"] == "deny"

        set("//sys/access_control_object_namespaces/animal_shelters/dogs_house/@acl/end", make_ace("allow", "rat", "read"))

        assert check_permission("rat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal")["action"] == "deny"

        set("//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal/@acl/end", make_ace("allow", "rat", "read"))
        assert check_permission("rat", "read", "//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal")["action"] == "allow"
        assert get("//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal/@acl") == \
            get("//sys/access_control_object_namespaces/animal_shelters/dogs_house/principal/@effective_acl")

    @authors("shakurov")
    def test_access_control_object_replication(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("tom", "cats")
        create_user("tom")

        acl = [make_ace("allow", "tom", "read")]

        set("//sys/access_control_object_namespaces/cats/tom/principal/@acl", acl)
        assert get("//sys/access_control_object_namespaces/cats/tom/@principal_acl") == acl
        assert get("//sys/access_control_object_namespaces/cats/tom/@principal_acl", driver=get_driver(1)) == acl
        assert get("//sys/access_control_object_namespaces/cats/tom/principal/@acl", driver=get_driver(1)) == acl

        assert get("//sys/access_control_object_namespaces/cats/tom/@principal_owner", driver=get_driver(1)) == "root"
        assert get("//sys/access_control_object_namespaces/cats/tom/principal/@owner", driver=get_driver(1)) == "root"

        set("//sys/access_control_object_namespaces/cats/tom/principal/@owner", "tom")
        assert get("//sys/access_control_object_namespaces/cats/tom/@principal_owner") == "tom"
        assert get("//sys/access_control_object_namespaces/cats/tom/@principal_owner", driver=get_driver(1)) == "tom"
        assert get("//sys/access_control_object_namespaces/cats/tom/principal/@owner", driver=get_driver(1)) == "tom"

        remove("//sys/access_control_object_namespaces/cats/tom/principal/@owner")
        assert not exists("//sys/access_control_object_namespaces/cats/tom/@principal_owner")
        assert not exists("//sys/access_control_object_namespaces/cats/tom/@principal_owner", driver=get_driver(1))
        assert not exists("//sys/access_control_object_namespaces/cats/tom/principal/@owner", driver=get_driver(1))

        remove("//sys/access_control_object_namespaces/cats/tom")
        assert not exists("//sys/access_control_object_namespaces/cats/tom")
        assert not exists("//sys/access_control_object_namespaces/cats/tom", driver=get_driver(1))

        remove("//sys/access_control_object_namespaces/cats")
        assert not exists("//sys/access_control_object_namespaces/cats")
        assert not exists("//sys/access_control_object_namespaces/cats", driver=get_driver(1))

    @authors("shakurov")
    def test_access_control_object_remove(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("tom", "cats")
        create_access_control_object("garfield", "cats")

        # Should not crash.
        remove("//sys/access_control_object_namespaces/cats/tom/*")

        remove("//sys/access_control_object_namespaces/cats/*")
        assert not exists("//sys/access_control_object_namespaces/cats/tom")
        assert not exists("//sys/access_control_object_namespaces/cats/garfield")

    @authors("shakurov")
    def test_access_control_object_replace_not_supported(self):
        create_access_control_object_namespace("cats")

        with raises_yt_error("already exists"):
            create_access_control_object_namespace("cats")

        with raises_yt_error("already exists"):
            create_access_control_object_namespace("cats", force=True)

        create_access_control_object("tom", "cats")

        with raises_yt_error("already exists"):
            create_access_control_object("tom", "cats")

        with raises_yt_error("already exists"):
            create_access_control_object("tom", "cats")


################################################################################


class TestSequoia(TestCypressMulticell):
    NUM_NODES = 5
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 3

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["sequoia_node_host"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["chunk_host"]},
    }


################################################################################
