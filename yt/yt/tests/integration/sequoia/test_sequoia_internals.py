from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, ls, get, remove, build_master_snapshots, raises_yt_error,
    exists, set, copy, move, gc_collect, write_table, read_table, create_user,
    start_transaction, abort_transaction, commit_transaction, link, wait, lock,
    execute_batch, make_batch_request, get_batch_output, print_debug,
)

from yt_sequoia_helpers import (
    resolve_sequoia_id, resolve_sequoia_path, select_rows_from_ground,
    select_paths_from_ground,
    lookup_cypress_transaction, select_cypress_transaction_replicas,
    select_cypress_transaction_descendants, clear_table_in_ground,
    select_cypress_transaction_prerequisites, lookup_rows_in_ground,
    mangle_sequoia_path, insert_rows_to_ground,
)

from yt.sequoia_tools import DESCRIPTORS

from yt_helpers import profiler_factory

import yt.yson as yson

import pytest
from flaky import flaky

import builtins
from collections import namedtuple
from copy import deepcopy
from datetime import datetime, timedelta
import itertools
import functools
from random import randint
from time import sleep

# TODO: drop after opensource tests migration to python >= 3.12
try:
    from typing import override
except ImportError:
    def override(function):
        return function

##################################################################


@pytest.mark.enabled_multidaemon
class TestSequoiaEnvSetup(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_MASTERS = 1
    NUM_CLOCKS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_REMOTE_CLUSTERS = 2
    USE_SEQUOIA_REMOTE_0 = False

    @authors("h0pless")
    def test1(self):
        sleep(10)  # Just don't crash...
        assert True

    @authors("h0pless")
    def test2(self):
        sleep(10)  # Just don't crash... (again)
        assert True


##################################################################


def with_cypress_dir(test_case):
    @functools.wraps(test_case)
    def wrapped(*args, **kwargs):
        create("map_node", "//cypress")
        try:
            test_case(*args, **kwargs)
        finally:
            remove("//cypress", recursive=True, force=True)

    return wrapped


@pytest.mark.enabled_multidaemon
class TestSequoiaInternals(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = True
    NUM_CYPRESS_PROXIES = 1

    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["sequoia_node_host"]},
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            "enable_ground_update_queues": True
        },
    }

    def lookup_path_to_node_id(self, path):
        return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

    @authors("kvk1920")
    def test_map_node_set_doesnt_cause_detach(self):
        set("//tmp/m1/m2/m3", {"abcde": 4}, recursive=True, force=True)
        assert get("//tmp/m1") == {"m2": {"m3": {"abcde": 4}}}
        m2_id = get("//tmp/m1/m2/@id")
        set("//tmp/m1/m2", {}, force=True)
        assert get("//tmp/m1") == {"m2": {}}
        assert get("//tmp/m1/m2/@id") == m2_id
        # Should not crash.
        remove("//tmp/m1/m2")

    @authors("h0pless")
    def test_create_table(self):
        create("table", "//tmp/some_dir/table", recursive=True)
        assert select_paths_from_ground() == [
            "//tmp/",
            "//tmp/some_dir/",
            "//tmp/some_dir/table/",
        ]

        assert get("//tmp") == {"some_dir": {"table": yson.YsonEntity()}}
        write_table("//tmp/some_dir/table", [{"x": "hello"}])

        # We should not read anything from table with get.
        assert get("//tmp") == {"some_dir": {"table": yson.YsonEntity()}}
        assert read_table("//tmp/some_dir/table") == [{"x": "hello"}]

    @authors("h0pless")
    def test_get(self):
        create("map_node", "//tmp/test_node")
        assert get("//tmp") == {"test_node": {}}
        assert get("//tmp") == get("#{}".format(get("//tmp/@id")))
        assert get("//tmp/test_node") == {}
        create("int64_node", "//tmp/test_node/test_int")
        set("//tmp/test_node/test_int", 1337)
        assert get("//tmp/test_node") == {"test_int": 1337}
        assert get("//tmp/test_node/test_int") == 1337

    @authors("h0pless")
    def test_get_recursive(self):
        # These paths should be seen in get result.
        create("string_node", "//tmp/more/nodes/to", recursive=True)
        create("int64_node", "//tmp/more/stuff/to/break", recursive=True)
        create("map_node", "//tmp/more/memes", recursive=True)
        set("//tmp/more/nodes/to", "test")
        set("//tmp/more/stuff/to/break", 1337)

        # These ones should be not.
        create("map_node", "//tmp/less/gameplay/less", recursive=True)
        create("map_node", "//tmp/w/please", recursive=True)

        # All nodes should be printed.
        assert get("//tmp/more") == {"memes": {}, "nodes": {"to": "test"}, "stuff": {"to": {"break": 1337}}}
        # Node "memes" should be printed as an empty node, others should be opaque.
        assert get("//tmp/more", max_size=3) == {"memes": {}, "nodes": yson.YsonEntity(), "stuff": yson.YsonEntity()}

        create("map_node", "//tmp/more/memes/hi")
        # All 3 nodes should be opaque.
        assert get("//tmp/more", max_size=3) == {"memes": yson.YsonEntity(), "nodes": yson.YsonEntity(), "stuff": yson.YsonEntity()}

    @authors("kvk1920", "cherepashka")
    def test_create_and_remove(self):
        create("map_node", "//tmp/some_node")
        remove("//tmp/some_node")
        with raises_yt_error("Node //tmp has no child with key \"some_node\""):
            get("//tmp/some_node")
        assert ls("//tmp") == []

    @authors("cherepashka")
    def test_recursive_remove(self):
        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m1/m2")
        create("map_node", "//tmp/m1/m2/m3")
        create("int64_node", "//tmp/m1/m2/m3/i")

        remove("//tmp/m1")
        assert ls("//tmp") == []

    @authors("cherepashka")
    def test_user(self):
        create_user("u")
        create("map_node", "//tmp/m1", authenticated_user="u")
        assert get("//tmp/m1/@owner") == "u"
        copy("//tmp/m1", "//tmp/m2", authenticated_user="u")
        assert get("//tmp/m2/@owner") == "u"
        move("//tmp/m2", "//tmp/m3", authenticated_user="u")
        assert get("//tmp/m3/@owner") == "u"

        assert ls("//tmp/m1", authenticated_user="u") == []
        set("//tmp/m", {"s": "u", "b": "tree"}, authenticated_user="u")
        assert get("//tmp/m", authenticated_user="u") == {"s": "u", "b": "tree"}

        remove("//tmp/m1", authenticated_user="u")

    @authors("danilalexeev")
    def test_list(self):
        assert ls("//tmp") == []

        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m2")
        assert ls("//tmp") == ["m1", "m2"]
        assert ls("//tmp") == ls("#{}".format(get("//tmp/@id")))

        create("map_node", "//tmp/m2/m3")
        create("int64_node", "//tmp/m1/i")
        assert ls("//tmp/m2") == ["m3"]
        assert ls("//tmp") == ["m1", "m2"]
        assert ls("//tmp/m1") == ["i"]

    @authors("h0pless")
    @pytest.mark.parametrize("result_type", ["map_node", "int64_node"])
    def test_create_force(self, result_type):
        create("map_node", "//tmp/node/and/other/nodes/to/remove", recursive=True)
        create(result_type, "//tmp/node", force=True)

    @authors("h0pless")
    @pytest.mark.parametrize("copy_mode", ["copy", "move"])
    def test_copy_simple(self, copy_mode):
        create("string_node", "//tmp/strings/s1", recursive=True)
        create("string_node", "//tmp/strings/s2")

        COMMON_ROWS = [
            "//tmp/",
            "//tmp/other/",
            "//tmp/other/s1/",
            "//tmp/other/s2/",
        ]

        if copy_mode == "copy":
            copy("//tmp/strings", "//tmp/other")
            assert select_paths_from_ground() == COMMON_ROWS + [
                "//tmp/strings/",
                "//tmp/strings/s1/",
                "//tmp/strings/s2/",
            ]

            # Let's do it twice for good measure.
            copy("//tmp/strings", "//tmp/other_other")
            assert select_paths_from_ground() == COMMON_ROWS + [
                "//tmp/other_other/",
                "//tmp/other_other/s1/",
                "//tmp/other_other/s2/",
                "//tmp/strings/",
                "//tmp/strings/s1/",
                "//tmp/strings/s2/",
            ]
        else:
            move("//tmp/strings", "//tmp/other")
            assert select_paths_from_ground() == COMMON_ROWS

    @authors("h0pless")
    @pytest.mark.parametrize("copy_mode", ["copy", "move"])
    def test_copy_recursive(self, copy_mode):
        create("map_node", "//tmp/src/a/b", recursive=True)
        create("map_node", "//tmp/src/a/c")
        create("map_node", "//tmp/src/d")

        COMMON_ROWS = [
            "//tmp/",
            "//tmp/d/",
            "//tmp/d/s/",
            "//tmp/d/s/t/",
            "//tmp/d/s/t/a/",
            "//tmp/d/s/t/a/b/",
            "//tmp/d/s/t/a/c/",
            "//tmp/d/s/t/d/",
        ]

        if copy_mode == "copy":
            copy("//tmp/src", "//tmp/d/s/t", recursive=True)
            assert select_paths_from_ground() == COMMON_ROWS + [
                "//tmp/src/",
                "//tmp/src/a/",
                "//tmp/src/a/b/",
                "//tmp/src/a/c/",
                "//tmp/src/d/",
            ]
        else:
            move("//tmp/src", "//tmp/d/s/t", recursive=True)
            assert select_paths_from_ground() == COMMON_ROWS

    @authors("h0pless")
    @pytest.mark.parametrize("copy_mode", ["copy", "move"])
    @pytest.mark.parametrize("is_excessive", [True, False])
    def test_copy_force(self, copy_mode, is_excessive):
        create("map_node", "//tmp/src/a/b", recursive=True)
        create("map_node", "//tmp/src/a/c")
        create("map_node", "//tmp/src/d")

        COMMON_ROWS = [
            "//tmp/",
            "//tmp/dst/",
            "//tmp/dst/a/",
            "//tmp/dst/a/b/",
            "//tmp/dst/a/c/",
            "//tmp/dst/d/",
        ]

        if not is_excessive:
            create("map_node", "//tmp/dst/some/unimportant/stuff/to/overwrite", recursive=True)

        if copy_mode == "copy":
            copy("//tmp/src", "//tmp/dst", force=True)
            assert select_paths_from_ground() == COMMON_ROWS + [
                "//tmp/src/",
                "//tmp/src/a/",
                "//tmp/src/a/b/",
                "//tmp/src/a/c/",
                "//tmp/src/d/",
            ]
        else:
            move("//tmp/src", "//tmp/dst", force=True)
            assert select_paths_from_ground() == COMMON_ROWS

    @authors("h0pless")
    @pytest.mark.parametrize("copy_mode", ["copy", "move"])
    def test_copy_force_overlapping(self, copy_mode):
        create("map_node", "//tmp/dst/src/a/b", recursive=True)
        create("map_node", "//tmp/dst/src/a/c")
        create("map_node", "//tmp/dst/src/d")

        if copy_mode == "copy":
            copy("//tmp/dst/src", "//tmp/dst", force=True)
        else:
            move("//tmp/dst/src", "//tmp/dst", force=True)

        assert select_paths_from_ground() == [
            "//tmp/",
            "//tmp/dst/",
            "//tmp/dst/a/",
            "//tmp/dst/a/b/",
            "//tmp/dst/a/c/",
            "//tmp/dst/d/",
        ]

    @authors("kvk1920")
    @pytest.mark.parametrize("copy_mode", ["copy", "move"])
    def test_copy_force_overlapping_2(self, copy_mode):
        create("map_node", "//tmp/src/dst/a/b", recursive=True)
        create("map_node", "//tmp/src/dst/a/c")
        create("map_node", "//tmp/src/dst/d")

        action = copy if copy_mode == "copy" else move

        with raises_yt_error("Cannot copy or move a node to its descendant"):
            action("//tmp/src", "//tmp/src/dst", force=True)
        with raises_yt_error("Cannot copy or move a node to itself"):
            action("//tmp/src", "//tmp/src/nonexistent", force=True)

    @authors("h0pless")
    def test_copy_ignore_existing(self):
        create("map_node", "//tmp/src/a/b", recursive=True)
        create("map_node", "//tmp/src/a/c")
        create("map_node", "//tmp/src/d")
        create("map_node", "//tmp/dst")

        original_select_result = select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}]")

        copy("//tmp/src", "//tmp/dst", ignore_existing=True)
        assert original_select_result == select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}]")

    @authors("danilalexeev")
    def test_create_recursive_fail(self):
        create("map_node", "//tmp/some_node")
        with raises_yt_error("Node //tmp has no child with key \"a\""):
            create("map_node", "//tmp/a/b")

    @authors("danilalexeev")
    def test_create_recursive_success(self):
        create("map_node", "//tmp/a/b", recursive=True)
        assert ls("//tmp/a") == ["b"]
        child_id = create("map_node", "//tmp/a/b/c/d/e/f/g/h/i", recursive=True)
        assert ls("//tmp/a/b/c/d/e/f/g/h") == ["i"]
        assert get("//tmp/a/b/c/d/e/f/g/h/@children") == {"i": child_id}
        root_id = get("//tmp/a/b/c/@id")
        assert get("//tmp/a/b/@children") == {"c": root_id}

    @authors("cherepashka", "danilalexeev")
    def test_create_and_set_scalars(self):
        create("int64_node", "//tmp/i")
        set("//tmp/i", 0xbebe)
        assert get("//tmp/i") == 0xbebe
        create("uint64_node", "//tmp/ui")
        set("//tmp/ui", 0xbebebe)
        assert get("//tmp/ui") == 0xbebebe
        set("//tmp/s", "str")
        assert get("//tmp/s") == "str"
        set("//tmp/d", 0.5)
        assert get("//tmp/d") == 0.5
        set("//tmp/b", False)
        assert not get("//tmp/b")
        with raises_yt_error("List nodes cannot be created inside Sequoia"):
            set("//tmp/l", [])

    @authors("danilalexeev")
    def test_set_map(self):
        subtree = {"hello": "world", "m": {"n": 0, "s": "string", "m": {}}, "d": 0.5}
        set("//tmp/m", subtree)
        assert get("//tmp/m/hello") == "world"
        assert sorted(ls("//tmp/m/m")) == ["m", "n", "s"]
        set("//tmp/a/b/c", subtree, recursive=True)
        assert sorted(ls("//tmp/a/b/c")) == ["d", "hello", "m"]
        assert get("//tmp/a/b/c/m/s") == "string"

    @authors("danilalexeev")
    def test_set_map_force(self):
        create("map_node", "//tmp/m/m", recursive=True)
        node_id = get("//tmp/m/@id")
        with raises_yt_error("\"set\" command without \"force\" flag is forbidden; use \"create\" instead"):
            set("//tmp/m", {"a": 0})
        set("//tmp/m", {"a": 0}, force=True)
        assert ls("//tmp/m") == ["a"]
        assert exists(f"#{node_id}")
        assert get("//tmp/m/@id") == node_id

        set(f"#{node_id}/m", {"b": 0}, force=True)
        assert get(f"#{node_id}/m") == {"b": 0}

    @authors("danilalexeev")
    def test_nodes_different_cell_tags(self):
        cell_tags = builtins.set()
        key = 0

        def create_and_check():
            nonlocal key
            create("map_node", f"//tmp/{key}")
            cell_tag = get(f"//tmp/{key}/@native_cell_tag")
            cell_tags.add(cell_tag)
            key += 1
            return len(cell_tags) > 1

        while not create_and_check():
            pass

    @authors("danilalexeev")
    def test_escaped_symbols(self):
        with raises_yt_error("Unexpected token \"{\" of type Left-brace"):
            create("map_node", "//tmp/special@&*[{symbols")
        path = r"//tmp/special\\\/\@\&\*\[\{symbols"
        create("map_node", path + "/m", recursive=True)
        assert r"special\/@&*[{symbols" in ls("//tmp")
        assert "m" in ls(path)
        child_id = create("map_node", r"//tmp/m\@1")
        assert get(r"//tmp/m\@1/@id") == child_id

    @authors("danilalexeev")
    @with_cypress_dir
    def test_link_through_sequoia(self):
        id1 = create("table", "//cypress/t1")
        link("//cypress/t1", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        link("//tmp/l2", "//cypress/l3")
        wait(lambda: len(self.lookup_path_to_node_id('//cypress/l3')) == 1)
        link("//cypress/l3", "//cypress/l4")
        wait(lambda: len(self.lookup_path_to_node_id('//cypress/l4')) == 1)
        assert get("//tmp/l1/@id") == id1
        assert get("//tmp/l2/@id") == id1
        assert get("//cypress/l3/@id") == id1
        assert get("//cypress/l4/@id") == id1

    @authors("danilalexeev")
    @with_cypress_dir
    def test_cyclic_link_through_sequoia(self):
        link("//cypress/l2", "//tmp/l1", force=True)
        link("//tmp/l3", "//cypress/l2", force=True, recursive=True)
        wait(lambda: len(self.lookup_path_to_node_id('//cypress/l2')) == 1)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/l1", "//tmp/l3", force=True)
        remove("//cypress", force=True, recursive=True)

    @authors("danilalexeev")
    @with_cypress_dir
    def test_broken_links(self):
        set("//tmp/t1", 1)
        set("//cypress/t2", 2, recursive=True)
        link("//cypress/t2", "//tmp/l1")
        link("//tmp/t1", "//cypress/l2")
        assert not get("//tmp/l1&/@broken")
        assert not get("//cypress/l2&/@broken")
        remove("//tmp/t1")
        remove("//cypress/t2")
        assert get("//tmp/l1&/@broken")
        assert get("//cypress/l2&/@broken")
        remove("//cypress", force=True, recursive=True)

    @authors("kvk1920")
    def test_create_map_node(self):
        m_id = create("map_node", "//tmp/m")
        set(f"#{m_id}/@foo", "bar")

        def check_everything():
            assert resolve_sequoia_path("//tmp") == get("//tmp&/@scion_id")
            assert resolve_sequoia_id(get("//tmp&/@scion_id")) == "//tmp"
            assert resolve_sequoia_path("//tmp/m") == m_id
            assert get(f"#{m_id}/@path") == "//tmp/m"
            assert get(f"#{m_id}/@key") == "m"
            assert get("//tmp/m/@path") == "//tmp/m"
            assert get("//tmp/m/@key") == "m"

            # TODO(kvk1920): Use attribute filter when it will be implemented in Sequoia.
            assert get(f"#{m_id}/@type") == "map_node"
            assert get(f"#{m_id}/@sequoia")

            assert get(f"#{m_id}/@foo") == "bar"

        check_everything()

        build_master_snapshots()

        # TODO(babenko): uncomment once Sequoia retries are implemented
        # TODO(kvk1920): Move it to TestMasterSnapshots.
        # with Restarter(self.Env, MASTERS_SERVICE):
        #    pass

        # check_everything()

    @authors("kvk1920")
    def test_sequoia_map_node_explicit_creation_is_forbidden(self):
        with raises_yt_error("is internal type and should not be used directly"):
            create("sequoia_map_node", "//tmp/m")

    @authors("danilalexeev")
    def test_mixed_read_write_batch(self):
        create("map_node", "//sys/cypress_proxies", ignore_existing=True)
        set("//sys/cypress_proxies/@config", {
            "object_service": {
                "alert_on_mixed_read_write_batch": True,
            }
        })
        sleep(0.5)

        results = execute_batch(
            [
                make_batch_request("set", path="//tmp/b", input="b"),
                make_batch_request("get", return_only_value=True, path="//tmp/a"),
            ]
        )
        # Do not crash.

        assert len(results) == 2
        assert not get_batch_output(results[0])
        with raises_yt_error():
            get_batch_output(results[1])


@pytest.mark.enabled_multidaemon
class TestSequoiaResolve(TestSequoiaInternals):
    ENABLE_MULTIDAEMON = True
    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestSequoiaCypressTransactions(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False
    NUM_TEST_PARTITIONS = 6
    CLASS_TEST_LIMIT = 600

    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["sequoia_node_host", "cypress_node_host", "chunk_host"]},
        "12": {"roles": ["transaction_coordinator", "cypress_node_host"]},
        "13": {"roles": ["chunk_host", "cypress_node_host"]}
    }

    # COMPAT(kvk1920): Remove when `use_cypress_transaction_service` become `true` by default.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            },
        },
    }

    def _check_transaction_not_exists(self, tx):
        assert not exists(f"#{tx}")
        assert not lookup_cypress_transaction(tx)
        assert not select_cypress_transaction_replicas(tx)

    def _check_transaction(
            self,
            tx,
            *,
            ancestors,
            descendants,
            replicas,
            title=None,
            prerequisite_transactions=None):
        assert type(descendants) is builtins.set
        assert type(replicas) is builtins.set
        if prerequisite_transactions is None:
            prerequisite_transactions = []

        def remove_duplicates(items):
            return sorted(builtins.set(items))

        prerequisite_transactions = remove_duplicates(prerequisite_transactions)

        # Coordinator cell tag should not be in "replicas".
        assert "12" not in replicas

        # Existence.
        assert exists(f"#{tx}")
        record = lookup_cypress_transaction(tx)
        assert record

        # Ancestors.
        assert record["ancestor_ids"] == ancestors
        for t, p in zip(ancestors + [tx], ["0-0-0-0"] + ancestors):
            assert get(f"#{t}/@parent_id") == p

        # Descendants.
        assert {*select_cypress_transaction_descendants(tx)} == descendants
        descendants_from_master = [tx]
        i = 0
        while i < len(descendants_from_master):
            cur = descendants_from_master[i]
            i += 1
            children = get(f"#{cur}/@nested_transaction_ids")
            descendants_from_master += children

        # Remove self.
        descendants_from_master.pop(0)
        assert builtins.set(descendants_from_master) == descendants

        # Replicas.
        assert builtins.set(get(f"#{tx}/@replicated_to_cell_tags")) == replicas
        assert builtins.set(select_cypress_transaction_replicas(tx)) == replicas

        # Title.
        assert record["attributes"].get("title", None) == title
        if title is None:
            assert not exists(f"#{tx}/@title")
        else:
            assert get(f"#{tx}/@title", None) == title

        # Prerequisites.
        assert remove_duplicates(get(f"#{tx}/@prerequisite_transaction_ids")) == prerequisite_transactions
        assert remove_duplicates(record["prerequisite_transaction_ids"]) == prerequisite_transactions
        prerequisites_from_ground = select_cypress_transaction_prerequisites(tx)
        assert len(prerequisites_from_ground) == len(builtins.set(prerequisites_from_ground))
        assert prerequisites_from_ground == prerequisite_transactions

    @authors("kvk1920")
    def test_start_transaction(self):
        tx = start_transaction(attributes={"title": "my title"})
        self._check_transaction(
            tx,
            ancestors=[],
            descendants=builtins.set(),
            replicas=builtins.set(),
            title="my title")

    @authors("kvk1920")
    def test_commit_empty_transaction(self):
        tx = start_transaction()
        commit_transaction(tx)
        self._check_transaction_not_exists(tx)

    @authors("kvk1920")
    def test_abort_transaction(self):
        tx = start_transaction(attributes={"title": "my title"})

        abort_transaction(tx)
        self._check_transaction_not_exists(tx)

        # Should not be error.
        abort_transaction(tx)

    @authors("kvk1920")
    def test_nested_transactions(self):
        # 0 - 1 - 2 - 3
        #     |
        # 6 - 4 - 5
        txs = [start_transaction(timeout=180000)]
        assert get(f"#{txs[0]}/@type") == "transaction"
        for p in (0, 1, 2, 1, 4, 4):
            txs.append(start_transaction(tx=txs[p], timeout=180000))
            assert get(f"#{txs[-1]}/@type") == "nested_transaction"
            assert get(f"#{txs[-1]}/@parent_id") == txs[p]

        def check_transaction(tx, ancestors, descendants):
            self._check_transaction(
                txs[tx],
                ancestors=[txs[a] for a in ancestors],
                descendants={txs[d] for d in descendants},
                replicas=builtins.set())

        for tx, ancestors, descendants in [
            (0, [], [1, 2, 3, 4, 5, 6]),
            (1, [0], [2, 3, 4, 5, 6]),
            (2, [0, 1], [3]),
            (3, [0, 1, 2], []),
            (4, [0, 1], [5, 6]),
            (5, [0, 1, 4], []),
            (6, [0, 1, 4], []),
        ]:
            check_transaction(tx, ancestors, descendants)

        abort_transaction(txs[4])
        for t in (4, 5, 6):
            self._check_transaction_not_exists(txs[t])

        for tx, ancestors, descendants in [
            (0, [], [1, 2, 3]),
            (1, [0], [2, 3]),
            (2, [0, 1], [3]),
            (3, [0, 1, 2], [])
        ]:
            check_transaction(tx, ancestors, descendants)

    @authors("kvk1920")
    def test_transaction_replication_on_start(self):
        # 0 - 1 - 2
        #     |
        # 4 - 3 - 5
        txs = []

        def check_transaction(tx, ancestors, descendants, replicas, title=None):
            self._check_transaction(
                txs[tx],
                ancestors=[txs[a] for a in ancestors],
                descendants={txs[d] for d in descendants},
                replicas=builtins.set(replicas),
                title=title)

        txs.append(start_transaction(replicate_to_master_cell_tags=[11], attributes={"title": "xxxxx"}, timeout=180000))
        check_transaction(0, [], [], [11], title="xxxxx")

        # Nested transaction shouldn't be replicated because parent is replicated.
        txs.append(start_transaction(tx=txs[0], timeout=180000))
        check_transaction(0, [], [1], [11], title="xxxxx")
        check_transaction(1, [0], [], [])

        # Parent transaction should be replicated.
        # Root transaction shouldn't be replicated twice.
        txs.append(start_transaction(tx=txs[1], replicate_to_master_cell_tags=[11], timeout=180000))
        check_transaction(0, [], [1, 2], [11], title="xxxxx")
        check_transaction(1, [0], [2], [11])
        check_transaction(2, [0, 1], [], [11])

        txs.append(start_transaction(tx=txs[1], timeout=180000))
        check_transaction(0, [], [1, 2, 3], [11], title="xxxxx")
        check_transaction(1, [0], [2, 3], [11])
        check_transaction(2, [0, 1], [], [11])
        check_transaction(3, [0, 1], [], [])

        txs.append(start_transaction(tx=txs[3], timeout=180000))
        check_transaction(0, [], [1, 2, 3, 4], [11], title="xxxxx")
        check_transaction(1, [0], [2, 3, 4], [11])
        check_transaction(2, [0, 1], [], [11])
        check_transaction(3, [0, 1], [4], [])
        check_transaction(4, [0, 1, 3], [], [])

        # Transaction can be replicated to more than one cell.
        # All ancestor transactions (including root one) should be replicated.
        txs.append(start_transaction(tx=txs[3], replicate_to_master_cell_tags=[10, 13], timeout=180000))
        check_transaction(0, [], [1, 2, 3, 4, 5], [10, 11, 13], title="xxxxx")
        check_transaction(1, [0], [2, 3, 4, 5], [10, 11, 13])
        check_transaction(2, [0, 1], [], [11])
        check_transaction(3, [0, 1], [4, 5], [10, 13])
        check_transaction(4, [0, 1, 3], [], [])
        check_transaction(5, [0, 1, 3], [], [10, 13])

        # Should not crash.
        abort_transaction(txs[3])
        check_transaction(0, [], [1, 2], [10, 11, 13], title="xxxxx")
        check_transaction(1, [0], [2], [10, 11, 13])
        check_transaction(2, [0, 1], [], [11])
        for tx in txs[3:]:
            self._check_transaction_not_exists(tx)

    def _create_per_cell_portals(self):
        create("map_node", "//tmp/p10")
        for cell_tag in (11, 12, 13):
            create(
                "portal_entrance",
                f"//tmp/p{cell_tag}",
                attributes={"exit_cell_tag": cell_tag})
            # Warm up resolve cache.
            assert exists(f"//tmp/p{cell_tag}")

    @authors("kvk1920")
    def test_commit_without_lazy_replication(self):
        self._create_per_cell_portals()

        # Note that replication to primary cell is necessary since portal
        # entrance resolve is going under tx.

        tx1 = start_transaction(timeout=180000)
        tx2 = start_transaction(tx=tx1, replicate_to_master_cell_tags=[10], timeout=180000)
        tx3 = start_transaction(tx=tx1, replicate_to_master_cell_tags=[11, 13], timeout=180000)
        tx4 = start_transaction(tx=tx1, timeout=180000)

        for cell_tag in (10, 11, 12, 13):
            create("map_node", f"//tmp/p{cell_tag}/tx1", tx=tx1)

        for cell_tag in (10, 12):
            create("map_node", f"//tmp/p{cell_tag}/tx2", tx=tx2)

        for cell_tag in (11, 12, 13):
            create("map_node", f"//tmp/p{cell_tag}/tx3", tx=tx3)

        create("map_node", "//tmp/p12/tx4", tx=tx4)
        assert not exists("//tmp/p12/tx4", tx=tx1)
        assert exists("//tmp/p12/tx4", tx=tx4)

        abort_transaction(tx4)
        self._check_transaction_not_exists(tx4)
        self._check_transaction(tx1, ancestors=[], descendants={tx2, tx3}, replicas={10, 11, 13})
        assert not exists("//tmp/p12/tx4", tx=tx1)

        # Cross-cell copy should not fail.
        self._check_transaction(tx3, ancestors=[tx1], descendants=builtins.set(), replicas={11, 13})
        set("//tmp/p11/ccc", {"a": {}, "b": {}}, force=True)
        copy("//tmp/p11/ccc", "//tmp/p13/ccc", tx=tx3)

        commit_transaction(tx3)
        self._check_transaction_not_exists(tx3)
        self._check_transaction(tx1, ancestors=[], descendants={tx2}, replicas={10, 11, 13})
        assert exists("//tmp/p11/tx3", tx=tx1)
        assert exists("//tmp/p12/tx3", tx=tx1)
        assert exists("//tmp/p13/tx3", tx=tx1)
        assert get("//tmp/p11/ccc", tx=tx1) == get("//tmp/p13/ccc", tx=tx1)

        commit_transaction(tx1)
        self._check_transaction_not_exists(tx2)
        self._check_transaction_not_exists(tx1)

        for tx, cell_tags, committed in [
            (1, [10, 11, 12, 13], True),
            (2, [10, 12], False),
            (3, [11, 12, 13], True),
            (4, [12], False)
        ]:
            for cell_tag in cell_tags:
                assert exists(f"//tmp/p{cell_tag}/tx{tx}") == committed

        assert get("//tmp/p11/ccc") == get("//tmp/p13/ccc")

    @authors("kvk1920")
    def test_lazy_transaction_replication(self):
        self._create_per_cell_portals()

        root = start_transaction(timeout=180000)
        t10_create = start_transaction(tx=root, timeout=180000)
        t11_13_copy = start_transaction(tx=root, timeout=180000)
        t12_tx_coord = start_transaction(tx=root, timeout=180000)
        t13_replicated_on_start = start_transaction(tx=root, replicate_to_master_cell_tags=[13], timeout=180000)
        all_txs = [t10_create, t11_13_copy, t12_tx_coord, t13_replicated_on_start]

        def check_transaction(tx, ancestors, descendants, replicas):
            self._check_transaction(tx,
                                    ancestors=ancestors,
                                    descendants=builtins.set(descendants),
                                    replicas=builtins.set(replicas))

        check_transaction(root, [], all_txs, [13])
        check_transaction(t10_create, [root], [], [])
        check_transaction(t11_13_copy, [root], [], [])
        check_transaction(t12_tx_coord, [root], [], [])
        check_transaction(t13_replicated_on_start, [root], [], [13])

        create("map_node", "//tmp/p12/d", tx=t12_tx_coord)
        # This cell is tx coordinator so all transactions should be already
        # present here.
        # NB: Every transaction should be present on primary cell since portal
        # entrance resolve is being done under this transaction.
        check_transaction(root, [], all_txs, [10, 13])
        check_transaction(t12_tx_coord, [root], [], [10])

        create("map_node", "//tmp/p10/d", tx=t10_create)
        check_transaction(root, [], all_txs, [10, 13])
        check_transaction(t10_create, [root], [], [10])

        set("//tmp/p11/d", {"e": {}}, force=True)
        copy("//tmp/p11/d", "//tmp/p13/d", tx=t11_13_copy)
        check_transaction(root, [], all_txs, [10, 11, 13])
        check_transaction(t11_13_copy, [root], [], [10, 11, 13])

        with raises_yt_error("Cannot take lock for child \"d\" of node //tmp/p13 since this child "
                             f"is locked by concurrent transaction {t11_13_copy}"):
            create("map_node", "//tmp/p13/d", tx=t13_replicated_on_start)

        create("map_node", "//tmp/p13/d2", tx=t13_replicated_on_start)
        check_transaction(root, [], all_txs, [10, 11, 13])
        check_transaction(t13_replicated_on_start, [root], [], [13])

        create("map_node", "//tmp/p11/d2", tx=t13_replicated_on_start)
        check_transaction(root, [], all_txs, [10, 11, 13])
        check_transaction(t13_replicated_on_start, [root], [], [11, 13])

        # Commit child transaction and check if the result is visible under root
        # tx.
        commit_transaction(t11_13_copy)
        self._check_transaction_not_exists(t11_13_copy)
        check_transaction(root, [], builtins.set(all_txs) - {t11_13_copy}, [10, 11, 13])
        assert get("//tmp/p13/d", tx=root) == {"e": {}}

        # Abort root tx and check if the result doesn't exist.
        abort_transaction(root)
        for t in all_txs:
            self._check_transaction_not_exists(t)
        assert not exists("//tmp/p13/d")
        assert not exists("//tmp/p11/d2")

    @authors("kvk1920")
    def test_prerequisite_transactions(self):
        # t1 - t2 - t3
        #        \
        #         t4
        #
        # t5 - t6
        # t5 depends on t3
        # t6 depends on t2 and t4
        #
        # t7 - t8
        # t8 depends on t4
        t1 = start_transaction(timeout=180000)
        t2 = start_transaction(tx=t1, timeout=180000)
        t3 = start_transaction(tx=t2, timeout=180000)
        t4 = start_transaction(tx=t2, timeout=180000)

        t5 = start_transaction(prerequisite_transaction_ids=[t3], timeout=180000)
        t6 = start_transaction(
            tx=t5,
            prerequisite_transaction_ids=[t2, t4],
            timeout=180000,
            replicate_to_master_cell_tags=[10, 11])

        t7 = start_transaction(timeout=180000)
        t8 = start_transaction(tx=t7, prerequisite_transaction_ids=[t4], timeout=180000)

        self._check_transaction(t1, ancestors=[], descendants={t2, t3, t4}, replicas=builtins.set(), prerequisite_transactions=[])
        self._check_transaction(t5, ancestors=[], descendants={t6}, replicas={10, 11}, prerequisite_transactions=[t3])
        self._check_transaction(t6, ancestors=[t5], descendants=builtins.set(), replicas={10, 11}, prerequisite_transactions=[t2, t4])
        self._check_transaction(t7, ancestors=[], descendants={t8}, replicas=builtins.set())
        self._check_transaction(t8, ancestors=[t7], descendants=builtins.set(), replicas=builtins.set(), prerequisite_transactions=[t4])

        abort_transaction(t1)

        self._check_transaction_not_exists(t1)
        self._check_transaction_not_exists(t2)
        self._check_transaction_not_exists(t3)
        self._check_transaction_not_exists(t4)
        self._check_transaction_not_exists(t5)
        self._check_transaction_not_exists(t6)
        self._check_transaction(t7, ancestors=[], descendants=builtins.set(), replicas=builtins.set())
        self._check_transaction_not_exists(t8)

        # Check if Sequoia tables were cleaned up properly.

        for table in DESCRIPTORS.get_group("transaction_tables"):
            records = select_rows_from_ground(f"* from [{table.get_default_path()}]")
            if table.name == "transactions":
                assert len(records) == 1
                del records[0]["transaction_id_hash"]
                assert records == [
                    {"transaction_id": t7, "ancestor_ids": [], "attributes": {}, "prerequisite_transaction_ids": []},
                ]
            else:
                assert records == []

    @authors("kvk1920")
    def test_timeout(self):
        before = datetime.now()

        t1 = start_transaction(timeout=20000)
        t2 = start_transaction(prerequisite_transaction_ids=[t1], timeout=180000)

        def sleep_until(t):
            delta = (t - datetime.now()).total_seconds()
            if delta > 0:
                sleep(delta)

        sleep_until(before + timedelta(seconds=6))

        # Transactions should still exist.
        self._check_transaction(t1, ancestors=[], descendants=builtins.set(), replicas=builtins.set())
        self._check_transaction(t2, ancestors=[], descendants=builtins.set(), replicas=builtins.set(), prerequisite_transactions=[t1])

        sleep_until(before + timedelta(seconds=25))
        self._check_transaction_not_exists(t1)
        self._check_transaction_not_exists(t2)

    @authors("kvk1920")
    def test_rollback(self):
        # t1 is prerequisite for t2
        #                       /  \
        #                      t3  t4
        t1 = start_transaction(timeout=100000)
        t2 = start_transaction(prerequisite_transaction_ids=[t1], timeout=100000)
        t3 = start_transaction(tx=t2, timeout=100000)
        t4 = start_transaction(tx=t2, timeout=100000)
        t5 = start_transaction(tx=t1, timeout=100000, replicate_to_master_cell_tags=[13])

        m3 = create("map_node", "//tmp/m3", tx=t3)
        create("map_node", "//tmp/m1", tx=t1)

        for table in DESCRIPTORS.get_group("transaction_tables"):
            clear_table_in_ground(table)

        with raises_yt_error("No such transaction"):
            # "transactions" table is empty so there is no active transactions
            # from Sequoia point of view.
            commit_transaction(t4)

        # Looks like abort_transaction() never returns an error. Strange but OK
        # for now...
        # TODO(kvk1920): in this case client should retry request after
        # mirroring will be disabled. So client should actually get an error.
        abort_transaction(t4)
        assert exists(f"#{t4}")

        # Disable transaction enabling and try to commit/abort some transactions.
        set("//sys/@config/sequoia_manager/enable_cypress_transactions_in_sequoia", False)

        assert exists(f"#{t1}")
        assert exists(f"#{t2}")
        assert exists(f"#{t3}")
        assert exists(f"#{t4}")
        assert exists(f"#{t5}")

        assert exists("//tmp/m3", tx=t3)
        assert exists("//tmp/m1", tx=t1)

        # Ensure that tx replication still works.
        create("portal_entrance", "//tmp/p11", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p11/m", tx=t5)

        # NB: T5 is already replicated to cell 13.
        create("portal_entrance", "//tmp/p13", attributes={"exit_cell_tag": 13})
        create("map_node", "//tmp/p13/m", tx=t5)

        commit_transaction(t5)
        commit_transaction(t1)
        # All other transactions should be aborted due to prerequisite finish.

        gc_collect()

        assert not exists(f"#{t1}")
        assert not exists(f"#{t2}")
        assert not exists(f"#{t3}")
        assert not exists(f"#{t4}")
        assert not exists(f"#{t5}")

        assert not exists("//tmp/m3")
        assert not exists(f"#{m3}")

        assert exists("//tmp/m1")
        assert exists("//tmp/p11/m")
        assert exists("//tmp/p13/m")


##################################################################


@pytest.mark.enabled_multidaemon
class SequoiaNodeVersioningBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False

    # COMPAT(kvk1920): remove when `use_cypress_transaction_service` become
    # `true` by default.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            },
        },
    }

    # Creates node and returns its ID. For rootstock returns corresponding
    # scion's ID.
    def create_node(self, node_type, path, tx=None):
        assert False, "not implemented"

    def remove_node(self, path, tx):
        assert False, "not implemented"

    def fill_or_check_resolve_table(self, table_descriptor, rows, tx_mapping=None):
        assert False, "not implemented"

    def lock_node(self, tx, node_id, lock_mode):
        assert False, "not implemented"

    # Record helpers.

    def node_id_to_path(self, node, path, tx=None, fork="regular"):
        assert node is not None
        assert node != "0-0-0-0"
        assert path.startswith("//tmp/")
        return {
            "node_id": node,
            "transaction_id": tx,
            "path": path,
            "target_path": "",
            "fork_kind": fork,
        }

    def path_to_node_id(self, path, node, tx=None):
        assert path.startswith("//tmp/")
        return {
            "path": mangle_sequoia_path(path),
            "transaction_id": tx,
            "node_id": node,
        }

    def child_node(self, parent, child_key, child, tx=None):
        assert parent is not None
        return {
            "parent_id": parent,
            "transaction_id": tx,
            "child_key": child_key,
            "child_id": child,
        }

    def node_snapshot(self, tx, node):
        assert node is not None
        assert tx is not None
        return {"transaction_id": tx, "node_id": node, "dummy": 0}

    def node_fork(self, tx, node, path, progenitor_tx):
        assert node is not None
        assert tx is not None
        assert path.startswith("//tmp/")
        return {
            "transaction_id": tx,
            "node_id": node,
            "path": path,
            "target_path": "",
            "progenitor_transaction_id": progenitor_tx,
        }

    def path_fork(self, tx, path, node, progenitor_tx):
        assert tx is not None
        assert path.startswith("//tmp/")
        return {
            "transaction_id": tx,
            "path": mangle_sequoia_path(path),
            "node_id": node,
            "progenitor_transaction_id": progenitor_tx,
        }

    def child_fork(self, tx, parent, key, child, progenitor_tx):
        assert tx is not None
        return {
            "transaction_id": tx,
            "parent_id": parent,
            "child_key": key,
            "child_id": child,
            "progenitor_transaction_id": progenitor_tx,
        }

    def fetch_resolve_table(self, table_descriptor):
        query = f"select * from [{table_descriptor.get_default_path()}]"
        if table_descriptor is DESCRIPTORS.node_id_to_path:
            # NB: We have to filter out symlinks which are mirrored to Sequoia.
            query += " where not is_prefix('//sys', path)"
        elif table_descriptor is DESCRIPTORS.child_node:
            # TODO(kvk1920): drop when YT-23209 will be done.
            query += " where (not is_null(transaction_id) and transaction_id != \"0-0-0-0\")" \
                     " or (not is_null(child_id) and child_id != \"0-0-0-0\")"
        return select_rows_from_ground(query)

    def check_resolve_table(self, table_descriptor, rows, tx_mapping=None):
        if tx_mapping is None:
            tx_mapping = {}

        self.assert_equal(
            tx_mapping,
            self.fetch_resolve_table(table_descriptor),
            rows)

    def assert_equal(self, tx_mapping, actual, expected):
        if not actual and not expected:
            return

        if actual:
            # `actual` has complete key list because it's the result of "select"
            # from table.
            keys = list(actual[0].keys())
        else:
            keys = list(expected[0].keys())
            for r in expected:
                for k in r.keys():
                    if k not in keys:
                        keys.append(k)

        for k in keys.copy():
            if "hash" in k:
                keys.remove(k)
            elif "transaction" in k:
                keys.append(k[:-2] + "name")

        if "path" in keys:
            keys.remove("path")
            keys = ["path"] + keys

        # Dict is not comparable. Simple tuple cause weird error message in
        # assert. Named tuple is much more readable.
        record_type = namedtuple("record", keys, defaults=[None] * len(keys))

        tx_mapping = tx_mapping | {"0-0-0-0": "trunk"}

        def normalize_record(record):
            def sanitize_none(value):
                if value is None or type(value) is yson.YsonEntity:
                    return "0-0-0-0"
                return value

            # Filter out "hash" columns and replace tx IDs with their names.
            result = {k: sanitize_none(v) for k, v in record.items() if "hash" not in k}
            for k, v in result.copy().items():
                if "transaction" in k and v is not None:
                    result[k[:-2] + "name"] = tx_mapping.get(v, "unknown")
            return result

        def normalize(records):
            return sorted(record_type(**normalize_record(r)) for r in records)

        actual = normalize(actual)
        expected = normalize(expected)
        if actual != expected:
            # Pytest reports too little information here.
            print_debug("actual:")
            for record in actual:
                print_debug(f"    {record}")
            print_debug("exepcted:")
            for record in expected:
                print_debug(f"    {record}")
        assert actual == expected

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_snapshot(self, finish_tx):
        # (tx5 and tx4 are out of tx tree: they just use some tx as prerequisites)
        # tx0 - tx1 - tx2
        #  |  `     ` tx3
        #  |    tx4 _/
        # tx5
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)
        tx2 = start_transaction(tx=tx1, timeout=100000)
        tx3 = start_transaction(tx=tx1, timeout=100000)
        tx4 = start_transaction(prerequisite_transaction_ids=[tx3, tx0], timeout=100000)
        tx5 = start_transaction(prerequisite_transaction_ids=[tx0], timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1", tx2: "tx2", tx3: "tx3", tx4: "tx4", tx5: "tx5"}

        scion_id = self.create_node("rootstock", "//tmp/scion")
        table_id = self.create_node("table", "//tmp/scion/t")
        for tx in [tx0, tx1, tx2, tx3, tx4, tx5]:
            self.lock_node(table_id, tx, "snapshot")

        self.fill_or_check_resolve_table(DESCRIPTORS.path_to_node_id, rows=[
            self.path_to_node_id("//tmp/scion", scion_id),
            self.path_to_node_id("//tmp/scion/t", table_id),
        ])
        self.fill_or_check_resolve_table(DESCRIPTORS.node_id_to_path, rows=[
            self.node_id_to_path(table_id, "//tmp/scion/t", tx=tx, fork="snapshot")
            for tx in [tx0, tx1, tx2, tx3, tx4, tx5]
        ] + [
            self.node_id_to_path(scion_id, "//tmp/scion"),
            self.node_id_to_path(table_id, "//tmp/scion/t"),
        ])
        self.fill_or_check_resolve_table(DESCRIPTORS.node_snapshots, rows=[
            self.node_snapshot(tx, table_id)
            for tx in [tx0, tx1, tx2, tx3, tx4, tx5]
        ])
        self.fill_or_check_resolve_table(DESCRIPTORS.child_node, rows=[
            self.child_node(scion_id, "t", table_id),
        ])

        finish_tx(tx1)

        # All snapshot branches are created under tx1, tx2 and tx3 must be
        # removed. Snapshot branches under tx4 should be removed too because
        # prerequisite of tx4 is finished.

        self.check_resolve_table(
            DESCRIPTORS.node_snapshots,
            [self.node_snapshot(t, table_id) for t in (tx0, tx5)],
            tx_mapping)

        self.check_resolve_table(
            DESCRIPTORS.node_id_to_path,
            [
                self.node_id_to_path(table_id, "//tmp/scion/t", t, "snapshot")
                for t in (tx0, tx5)
            ] + [
                self.node_id_to_path(scion_id, "//tmp/scion"),
                self.node_id_to_path(table_id, "//tmp/scion/t"),
            ],
            tx_mapping)

    @pytest.mark.parametrize("originator", ["trunk", "transaction"])
    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_subtree_replacement(self, originator, finish_tx):
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1"}

        # Originator (either trunk or tx0).
        tx_or = None if originator == "trunk" else tx0

        # originator:
        #   tmp-s-a-b-c-d-e
        #          `f
        # tx1: remove //tmp/s/a/b and //tmp/s/a/f, then create [//tmp/s/a/b,
        #                                                       //tmp/s/a/b/c,
        #                                                       //tmp/s/a/b/g]
        #   tmp-s-a-b-c
        #            `g

        scion_id = self.create_node("rootstock", "//tmp/s")
        a_id = self.create_node("map_node", "//tmp/s/a", tx_or)
        b_id = self.create_node("map_node", "//tmp/s/a/b", tx_or)
        c_id = self.create_node("map_node", "//tmp/s/a/b/c", tx_or)
        d_id = self.create_node("map_node", "//tmp/s/a/b/c/d", tx_or)
        e_id = self.create_node("table", "//tmp/s/a/b/c/d/e", tx_or)
        f_id = self.create_node("table", "//tmp/s/a/f", tx_or)

        self.remove_node("//tmp/s/a/b", tx1)
        self.remove_node(f"#{f_id}", tx1)

        b_new_id = self.create_node("map_node", "//tmp/s/a/b", tx1)
        c_new_id = self.create_node("table", "//tmp/s/a/b/c", tx1)
        g_id = self.create_node("table", "//tmp/s/a/b/g", tx1)

        old_node_id_to_path = [
            self.node_id_to_path(scion_id, "//tmp/s"),

            self.node_id_to_path(a_id, "//tmp/s/a", tx=tx_or),
            self.node_id_to_path(f_id, "//tmp/s/a/f", tx=tx_or),
            self.node_id_to_path(b_id, "//tmp/s/a/b", tx=tx_or),
            self.node_id_to_path(c_id, "//tmp/s/a/b/c", tx=tx_or),
            self.node_id_to_path(d_id, "//tmp/s/a/b/c/d", tx=tx_or),
            self.node_id_to_path(e_id, "//tmp/s/a/b/c/d/e", tx=tx_or),

            self.node_id_to_path(f_id, "//tmp/s/a/f", tx=tx1, fork="tombstone"),
            self.node_id_to_path(b_id, "//tmp/s/a/b", tx=tx1, fork="tombstone"),
            self.node_id_to_path(c_id, "//tmp/s/a/b/c", tx=tx1, fork="tombstone"),
            self.node_id_to_path(d_id, "//tmp/s/a/b/c/d", tx=tx1, fork="tombstone"),
            self.node_id_to_path(e_id, "//tmp/s/a/b/c/d/e", tx=tx1, fork="tombstone"),

            self.node_id_to_path(b_new_id, "//tmp/s/a/b", tx=tx1),
            self.node_id_to_path(c_new_id, "//tmp/s/a/b/c", tx=tx1),
            self.node_id_to_path(g_id, "//tmp/s/a/b/g", tx=tx1),
        ]
        old_path_to_node_id = [
            self.path_to_node_id("//tmp/s", scion_id),

            self.path_to_node_id("//tmp/s/a", a_id, tx=tx_or),
            self.path_to_node_id("//tmp/s/a/f", f_id, tx=tx_or),
            self.path_to_node_id("//tmp/s/a/b", b_id, tx=tx_or),
            self.path_to_node_id("//tmp/s/a/b/c", c_id, tx=tx_or),
            self.path_to_node_id("//tmp/s/a/b/c/d", d_id, tx=tx_or),
            self.path_to_node_id("//tmp/s/a/b/c/d/e", e_id, tx=tx_or),

            self.path_to_node_id("//tmp/s/a/b", b_new_id, tx=tx1),
            self.path_to_node_id("//tmp/s/a/b/c", c_new_id, tx=tx1),
            self.path_to_node_id("//tmp/s/a/b/g", g_id, tx=tx1),

            self.path_to_node_id("//tmp/s/a/f", None, tx=tx1),
            self.path_to_node_id("//tmp/s/a/b/c/d", None, tx=tx1),

            # NB: Tomstones for nested removed nodes can be useful for resolve
            # optimization (i.e. don't lookup nodes near the scion).
            self.path_to_node_id("//tmp/s/a/b/c/d/e", None, tx=tx1),
        ]
        old_child_node = [
            self.child_node(scion_id, "a", a_id, tx=tx_or),
            self.child_node(a_id, "f", f_id, tx=tx_or),
            self.child_node(a_id, "b", b_id, tx=tx_or),
            self.child_node(b_id, "c", c_id, tx=tx_or),
            self.child_node(c_id, "d", d_id, tx=tx_or),
            self.child_node(d_id, "e", e_id, tx=tx_or),

            self.child_node(a_id, "f", None, tx=tx1),
            self.child_node(a_id, "b", b_new_id, tx=tx1),
            self.child_node(b_new_id, "c", c_new_id, tx=tx1),
            self.child_node(b_new_id, "g", g_id, tx=tx1),

            # NB: Tombstones for the children of removed node are useless. There
            # is even no node with ID |b_id|. But we still keep them to simplify
            # resolve table modification: it's simplier to not distinguish
            # nested node and subtree root removal.
            self.child_node(b_id, "c", None, tx=tx1),
            self.child_node(c_id, "d", None, tx=tx1),
            self.child_node(d_id, "e", None, tx=tx1),
        ]
        old_node_forks = [
            self.node_fork(tx1, f_id, "//tmp/s/a/f", tx_or),
            self.node_fork(tx1, b_id, "//tmp/s/a/b", tx_or),
            self.node_fork(tx1, c_id, "//tmp/s/a/b/c", tx_or),
            self.node_fork(tx1, d_id, "//tmp/s/a/b/c/d", tx_or),
            self.node_fork(tx1, e_id, "//tmp/s/a/b/c/d/e", tx_or),
            self.node_fork(tx1, b_new_id, "//tmp/s/a/b", tx1),
            self.node_fork(tx1, c_new_id, "//tmp/s/a/b/c", tx1),
            self.node_fork(tx1, g_id, "//tmp/s/a/b/g", tx1),
        ]
        if originator == "transaction":
            old_node_forks += [
                self.node_fork(tx0, a_id, "//tmp/s/a", tx0),
                self.node_fork(tx0, f_id, "//tmp/s/a/f", tx0),
                self.node_fork(tx0, b_id, "//tmp/s/a/b", tx0),
                self.node_fork(tx0, c_id, "//tmp/s/a/b/c", tx0),
                self.node_fork(tx0, d_id, "//tmp/s/a/b/c/d", tx0),
                self.node_fork(tx0, e_id, "//tmp/s/a/b/c/d/e", tx0),
            ]
        old_path_forks = [
            self.path_fork(tx1, "//tmp/s/a/f", None, tx_or),
            self.path_fork(tx1, "//tmp/s/a/b", b_new_id, tx_or),
            self.path_fork(tx1, "//tmp/s/a/b/c", c_new_id, tx_or),
            self.path_fork(tx1, "//tmp/s/a/b/c/d", None, tx_or),
            self.path_fork(tx1, "//tmp/s/a/b/c/d/e", None, tx_or),
            self.path_fork(tx1, "//tmp/s/a/b/g", g_id, tx1),
        ]
        if originator == "transaction":
            old_path_forks += [
                self.path_fork(tx0, "//tmp/s/a", a_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/f", f_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b", b_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b/c", c_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b/c/d", d_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b/c/d/e", e_id, tx0),
            ]
        old_child_forks = [
            self.child_fork(tx1, a_id, "b", b_new_id, tx_or),
            self.child_fork(tx1, b_new_id, "c", c_new_id, tx1),
            self.child_fork(tx1, b_new_id, "g", g_id, tx1),

            self.child_fork(tx1, a_id, "f", None, tx_or),
            self.child_fork(tx1, b_id, "c", None, tx_or),
            self.child_fork(tx1, c_id, "d", None, tx_or),
            self.child_fork(tx1, d_id, "e", None, tx_or),
        ]
        if originator == "transaction":
            old_child_forks += [
                self.child_fork(tx0, scion_id, "a", a_id, tx0),
                self.child_fork(tx0, a_id, "f", f_id, tx0),
                self.child_fork(tx0, a_id, "b", b_id, tx0),
                self.child_fork(tx0, b_id, "c", c_id, tx0),
                self.child_fork(tx0, c_id, "d", d_id, tx0),
                self.child_fork(tx0, d_id, "e", e_id, tx0),
            ]

        self.fill_or_check_resolve_table(DESCRIPTORS.node_id_to_path, old_node_id_to_path, tx_mapping)
        self.fill_or_check_resolve_table(DESCRIPTORS.path_to_node_id, old_path_to_node_id, tx_mapping)
        self.fill_or_check_resolve_table(DESCRIPTORS.child_node, old_child_node, tx_mapping)
        self.fill_or_check_resolve_table(DESCRIPTORS.node_forks, old_node_forks, tx_mapping)
        self.fill_or_check_resolve_table(DESCRIPTORS.path_forks, old_path_forks, tx_mapping)
        self.fill_or_check_resolve_table(DESCRIPTORS.child_forks, old_child_forks, tx_mapping)

        finish_tx(tx1)

        if finish_tx == abort_transaction:
            # Drop all tx1 changes.
            def build_expected(old):
                return [record for record in old if record.get("transaction_id", None) != tx1]

            expected_node_id_to_path = build_expected(old_node_id_to_path)
            expected_path_to_node_id = build_expected(old_path_to_node_id)
            expected_child_node = build_expected(old_child_node)
            expected_path_forks = build_expected(old_path_forks)
            expected_node_forks = build_expected(old_node_forks)
            expected_child_forks = build_expected(old_child_forks)
        elif originator == "trunk":
            # Just propagate delta to parent tx.
            def build_expected(old):
                def build_expected_record(old_record):
                    new_record = old_record.copy()
                    for k, v in old_record.items():
                        if "transaction_id" in k and v == tx1:
                            new_record[k] = tx0
                    return new_record

                return list(map(build_expected_record, old))

            expected_node_id_to_path = build_expected(old_node_id_to_path)
            expected_path_to_node_id = build_expected(old_path_to_node_id)
            expected_child_node = build_expected(old_child_node)
            expected_node_forks = build_expected(old_node_forks)
            expected_path_forks = build_expected(old_path_forks)
            expected_child_forks = build_expected(old_child_forks)
        else:
            # This case has non-trivial branch merging.
            expected_node_id_to_path = [
                self.node_id_to_path(scion_id, "//tmp/s"),
                self.node_id_to_path(a_id, "//tmp/s/a", tx=tx0),
                self.node_id_to_path(b_new_id, "//tmp/s/a/b", tx=tx0),
                self.node_id_to_path(c_new_id, "//tmp/s/a/b/c", tx=tx0),
                self.node_id_to_path(g_id, "//tmp/s/a/b/g", tx=tx0),
            ]
            expected_path_to_node_id = [
                self.path_to_node_id("//tmp/s", scion_id),
                self.path_to_node_id("//tmp/s/a", a_id, tx=tx0),
                self.path_to_node_id("//tmp/s/a/b", b_new_id, tx=tx0),
                self.path_to_node_id("//tmp/s/a/b/c", c_new_id, tx=tx0),
                self.path_to_node_id("//tmp/s/a/b/g", g_id, tx=tx0),
            ]
            expected_child_node = [
                self.child_node(scion_id, "a", a_id, tx=tx0),
                self.child_node(a_id, "b", b_new_id, tx=tx0),
                self.child_node(b_new_id, "c", c_new_id, tx=tx0),
                self.child_node(b_new_id, "g", g_id, tx=tx0),
            ]
            expected_node_forks = [
                self.node_fork(tx0, a_id, "//tmp/s/a", tx0),
                self.node_fork(tx0, b_new_id, "//tmp/s/a/b", tx0),
                self.node_fork(tx0, c_new_id, "//tmp/s/a/b/c", tx0),
                self.node_fork(tx0, g_id, "//tmp/s/a/b/g", tx0),
            ]
            expected_path_forks = [
                self.path_fork(tx0, "//tmp/s/a", a_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b", b_new_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b/c", c_new_id, tx0),
                self.path_fork(tx0, "//tmp/s/a/b/g", g_id, tx0),
            ]
            expected_child_forks = [
                self.child_fork(tx0, scion_id, "a", a_id, tx0),
                self.child_fork(tx0, a_id, "b", b_new_id, tx0),
                self.child_fork(tx0, b_new_id, "c", c_new_id, tx0),
                self.child_fork(tx0, b_new_id, "g", g_id, tx0)
            ]

        self.check_resolve_table(DESCRIPTORS.node_id_to_path, expected_node_id_to_path, tx_mapping)
        self.check_resolve_table(DESCRIPTORS.path_to_node_id, expected_path_to_node_id, tx_mapping)
        self.check_resolve_table(DESCRIPTORS.child_node, expected_child_node, tx_mapping)
        self.check_resolve_table(DESCRIPTORS.node_forks, expected_node_forks, tx_mapping)
        self.check_resolve_table(DESCRIPTORS.path_forks, expected_path_forks, tx_mapping)
        self.check_resolve_table(DESCRIPTORS.child_forks, expected_child_forks, tx_mapping)

    def test_merge_committed_changes(self):
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)
        tx2 = start_transaction(tx=tx1, timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1", tx2: "tx2"}

        scion_id = self.create_node("rootstock", "//tmp/s")

        # trunk: //tmp/s/replace/nested
        # tx0: remove //tmp/s/replace/nested
        # tx1: remove //tmp/s/replace
        # tx2: create [//tmp/s/replace, //tmp/s/replace/nested]
        to_replace_id = self.create_node("map_node", "//tmp/s/replace")
        to_replace_nested_id = self.create_node("table", "//tmp/s/replace/nested")
        self.remove_node("//tmp/s/replace/nested", tx0)
        self.remove_node("//tmp/s/replace", tx1)
        replaced_id = self.create_node("map_node", "//tmp/s/replace", tx2)
        replaced_nested_id = self.create_node("table", "//tmp/s/replace/nested", tx2)

        # trunk: //tmp/s/remove/recursive
        # tx0: remove //tmp/s/remove
        # tx1: create //tmp/s/remove + create //tmp/s/remove/nested
        # tx2: remove //tmp/s/remove
        to_remove_id = self.create_node("map_node", "//tmp/s/remove")
        to_remove_recursive_id = self.create_node("table", "//tmp/s/remove/recursive")
        self.remove_node("//tmp/s/remove", tx0)
        recreated_id = self.create_node("map_node", "//tmp/s/remove", tx1)
        recreated_nested_id = self.create_node("table", "//tmp/s/remove/nested", tx1)
        self.remove_node("//tmp/s/remove", tx2)

        # trunk: None
        # tx0: create //tmp/s/create
        # tx1: remove //tmp/s/create
        # tx2: create //tmp/s/create
        created_1_id = self.create_node("table", "//tmp/s/create", tx0)
        self.remove_node("//tmp/s/create", tx1)
        created_2_id = self.create_node("table", "//tmp/s/create", tx2)

        # trunk: //tmp/s/remove_recreate
        # tx1: replace //tmp/s/remove_recreate
        # tx2: remove //tmp/s/remove_recreate
        remove_recreate_1_id = self.create_node("table", "//tmp/s/remove_recreate")
        remove_recreate_2_id = self.create_node("table", "//tmp/s/remove_recreate", tx1)
        self.remove_node("//tmp/s/remove_recreate", tx2)

        # node_id, path, tx_id, fork_kind="regular"
        self.fill_or_check_resolve_table(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(scion_id, "//tmp/s"),

            self.node_id_to_path(to_replace_id, "//tmp/s/replace"),
            self.node_id_to_path(to_replace_id, "//tmp/s/replace", tx1, "tombstone"),

            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested"),
            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested", tx0, "tombstone"),

            self.node_id_to_path(replaced_id, "//tmp/s/replace", tx2),

            self.node_id_to_path(replaced_nested_id, "//tmp/s/replace/nested", tx2),

            self.node_id_to_path(to_remove_id, "//tmp/s/remove"),
            self.node_id_to_path(to_remove_id, "//tmp/s/remove", tx0, "tombstone"),

            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive"),
            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive", tx0, "tombstone"),

            self.node_id_to_path(recreated_id, "//tmp/s/remove", tx1),
            self.node_id_to_path(recreated_id, "//tmp/s/remove", tx2, "tombstone"),

            self.node_id_to_path(recreated_nested_id, "//tmp/s/remove/nested", tx1),
            self.node_id_to_path(recreated_nested_id, "//tmp/s/remove/nested", tx2, "tombstone"),

            self.node_id_to_path(created_1_id, "//tmp/s/create", tx0),
            self.node_id_to_path(created_1_id, "//tmp/s/create", tx1, "tombstone"),

            self.node_id_to_path(created_2_id, "//tmp/s/create", tx2),

            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate"),
            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate", tx1, "tombstone"),

            self.node_id_to_path(remove_recreate_2_id, "//tmp/s/remove_recreate", tx1),
            self.node_id_to_path(remove_recreate_2_id, "//tmp/s/remove_recreate", tx2, "tombstone"),
        ], tx_mapping)

        # path, node_id, tx_id=None
        self.fill_or_check_resolve_table(DESCRIPTORS.path_to_node_id, [
            self.path_to_node_id("//tmp/s", scion_id),

            self.path_to_node_id("//tmp/s/replace", to_replace_id),
            self.path_to_node_id("//tmp/s/replace", None, tx1),
            self.path_to_node_id("//tmp/s/replace", replaced_id, tx2),
            self.path_to_node_id("//tmp/s/replace/nested", to_replace_nested_id),
            self.path_to_node_id("//tmp/s/replace/nested", None, tx0),
            self.path_to_node_id("//tmp/s/replace/nested", replaced_nested_id, tx2),

            self.path_to_node_id("//tmp/s/remove", to_remove_id),
            self.path_to_node_id("//tmp/s/remove", None, tx0),
            self.path_to_node_id("//tmp/s/remove", recreated_id, tx1),
            self.path_to_node_id("//tmp/s/remove", None, tx2),
            self.path_to_node_id("//tmp/s/remove/recursive", to_remove_recursive_id),
            self.path_to_node_id("//tmp/s/remove/recursive", None, tx0),
            self.path_to_node_id("//tmp/s/remove/nested", recreated_nested_id, tx1),
            self.path_to_node_id("//tmp/s/remove/nested", None, tx2),

            self.path_to_node_id("//tmp/s/create", created_1_id, tx0),
            self.path_to_node_id("//tmp/s/create", None, tx1),
            self.path_to_node_id("//tmp/s/create", created_2_id, tx2),

            self.path_to_node_id("//tmp/s/remove_recreate", remove_recreate_1_id),
            self.path_to_node_id("//tmp/s/remove_recreate", remove_recreate_2_id, tx1),
            self.path_to_node_id("//tmp/s/remove_recreate", None, tx2),
        ], tx_mapping)

        # parent_id, path, node_id, tx_id=None
        self.fill_or_check_resolve_table(DESCRIPTORS.child_node, [
            self.child_node(scion_id, "replace", to_replace_id),
            self.child_node(scion_id, "replace", None, tx1),
            self.child_node(scion_id, "replace", replaced_id, tx2),

            self.child_node(to_replace_id, "nested", to_replace_nested_id),
            self.child_node(to_replace_id, "nested", None, tx0),

            self.child_node(replaced_id, "nested", replaced_nested_id, tx2),

            self.child_node(scion_id, "remove", to_remove_id),
            self.child_node(scion_id, "remove", None, tx0),
            self.child_node(scion_id, "remove", recreated_id, tx1),
            self.child_node(scion_id, "remove", None, tx2),

            self.child_node(to_remove_id, "recursive", to_remove_recursive_id),
            self.child_node(to_remove_id, "recursive", None, tx0),

            self.child_node(recreated_id, "nested", recreated_nested_id, tx1),
            self.child_node(recreated_id, "nested", None, tx2),

            self.child_node(scion_id, "create", created_1_id, tx0),
            self.child_node(scion_id, "create", None, tx1),
            self.child_node(scion_id, "create", created_2_id, tx2),

            self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
            self.child_node(scion_id, "remove_recreate", remove_recreate_2_id, tx1),
            self.child_node(scion_id, "remove_recreate", None, tx2),
        ], tx_mapping)

        # tx_id, node_id, path, progenitor_tx_id
        self.fill_or_check_resolve_table(DESCRIPTORS.node_forks, [
            self.node_fork(tx1, to_replace_id, "//tmp/s/replace", None),

            self.node_fork(tx0, to_replace_nested_id, "//tmp/s/replace/nested", None),

            self.node_fork(tx2, replaced_id, "//tmp/s/replace", tx2),

            self.node_fork(tx2, replaced_nested_id, "//tmp/s/replace/nested", tx2),

            self.node_fork(tx0, to_remove_id, "//tmp/s/remove", None),
            self.node_fork(tx0, to_remove_recursive_id, "//tmp/s/remove/recursive", None),

            self.node_fork(tx1, recreated_id, "//tmp/s/remove", tx1),
            self.node_fork(tx2, recreated_id, "//tmp/s/remove", tx1),

            self.node_fork(tx1, recreated_nested_id, "//tmp/s/remove/nested", tx1),
            self.node_fork(tx2, recreated_nested_id, "//tmp/s/remove/nested", tx1),

            self.node_fork(tx0, created_1_id, "//tmp/s/create", tx0),
            self.node_fork(tx1, created_1_id, "//tmp/s/create", tx0),

            self.node_fork(tx2, created_2_id, "//tmp/s/create", tx2),

            self.node_fork(tx1, remove_recreate_1_id, "//tmp/s/remove_recreate", None),

            self.node_fork(tx1, remove_recreate_2_id, "//tmp/s/remove_recreate", tx1),
            self.node_fork(tx2, remove_recreate_2_id, "//tmp/s/remove_recreate", tx1),
        ], tx_mapping)

        # tx_id, path, node_id, parent_id, progenitor_tx_id
        self.fill_or_check_resolve_table(DESCRIPTORS.path_forks, [
            self.path_fork(tx1, "//tmp/s/replace", None, None),
            self.path_fork(tx2, "//tmp/s/replace", replaced_id, None),

            self.path_fork(tx0, "//tmp/s/replace/nested", None, None),
            self.path_fork(tx2, "//tmp/s/replace/nested", replaced_nested_id, None),

            self.path_fork(tx0, "//tmp/s/remove", None, None),
            self.path_fork(tx1, "//tmp/s/remove", recreated_id, None),
            self.path_fork(tx2, "//tmp/s/remove", None, None),

            self.path_fork(tx0, "//tmp/s/remove/recursive", None, None),

            self.path_fork(tx1, "//tmp/s/remove/nested", recreated_nested_id, tx1),
            self.path_fork(tx2, "//tmp/s/remove/nested", None, tx1),

            self.path_fork(tx0, "//tmp/s/create", created_1_id, tx0),
            self.path_fork(tx1, "//tmp/s/create", None, tx0),
            self.path_fork(tx2, "//tmp/s/create", created_2_id, tx0),

            self.path_fork(tx1, "//tmp/s/remove_recreate", remove_recreate_2_id, None),
            self.path_fork(tx2, "//tmp/s/remove_recreate", None, None),
        ], tx_mapping)

        # tx_id, parent, key, child, progenitor_tx
        self.fill_or_check_resolve_table(DESCRIPTORS.child_forks, [
            self.child_fork(tx1, scion_id, "replace", None, None),
            self.child_fork(tx2, scion_id, "replace", replaced_id, None),

            self.child_fork(tx0, to_replace_id, "nested", None, None),

            self.child_fork(tx2, replaced_id, "nested", replaced_nested_id, tx2),

            self.child_fork(tx0, scion_id, "remove", None, None),
            self.child_fork(tx1, scion_id, "remove", recreated_id, None),
            self.child_fork(tx2, scion_id, "remove", None, None),

            self.child_fork(tx0, to_remove_id, "recursive", None, None),

            self.child_fork(tx1, recreated_id, "nested", recreated_nested_id, tx1),
            self.child_fork(tx2, recreated_id, "nested", None, tx1),

            self.child_fork(tx0, scion_id, "create", created_1_id, tx0),
            self.child_fork(tx1, scion_id, "create", None, tx0),
            self.child_fork(tx2, scion_id, "create", created_2_id, tx0),

            self.child_fork(tx1, scion_id, "remove_recreate", remove_recreate_2_id, None),
            self.child_fork(tx2, scion_id, "remove_recreate", None, None),
        ])

        commit_transaction(tx2)

        self.check_resolve_table(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(scion_id, "//tmp/s"),

            self.node_id_to_path(to_replace_id, "//tmp/s/replace"),
            self.node_id_to_path(to_replace_id, "//tmp/s/replace", tx1, "tombstone"),

            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested"),
            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested", tx0, "tombstone"),

            self.node_id_to_path(replaced_id, "//tmp/s/replace", tx1),

            self.node_id_to_path(replaced_nested_id, "//tmp/s/replace/nested", tx1),

            self.node_id_to_path(to_remove_id, "//tmp/s/remove"),
            self.node_id_to_path(to_remove_id, "//tmp/s/remove", tx0, "tombstone"),

            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive"),
            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive", tx0, "tombstone"),

            self.node_id_to_path(created_1_id, "//tmp/s/create", tx0),
            self.node_id_to_path(created_1_id, "//tmp/s/create", tx1, "tombstone"),

            self.node_id_to_path(created_2_id, "//tmp/s/create", tx1),

            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate"),
            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate", tx1, "tombstone"),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, [
            self.path_to_node_id("//tmp/s", scion_id),

            self.path_to_node_id("//tmp/s/replace", to_replace_id),
            self.path_to_node_id("//tmp/s/replace", replaced_id, tx1),
            self.path_to_node_id("//tmp/s/replace/nested", to_replace_nested_id),
            self.path_to_node_id("//tmp/s/replace/nested", None, tx0),
            self.path_to_node_id("//tmp/s/replace/nested", replaced_nested_id, tx1),

            self.path_to_node_id("//tmp/s/remove", to_remove_id),
            self.path_to_node_id("//tmp/s/remove", None, tx0),
            self.path_to_node_id("//tmp/s/remove", None, tx1),
            self.path_to_node_id("//tmp/s/remove/recursive", to_remove_recursive_id),
            self.path_to_node_id("//tmp/s/remove/recursive", None, tx0),

            self.path_to_node_id("//tmp/s/create", created_1_id, tx0),
            self.path_to_node_id("//tmp/s/create", created_2_id, tx1),

            self.path_to_node_id("//tmp/s/remove_recreate", remove_recreate_1_id),
            self.path_to_node_id("//tmp/s/remove_recreate", None, tx1),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.child_node, [
            self.child_node(scion_id, "replace", to_replace_id),
            self.child_node(scion_id, "replace", replaced_id, tx1),

            self.child_node(to_replace_id, "nested", to_replace_nested_id),
            self.child_node(to_replace_id, "nested", None, tx0),

            self.child_node(replaced_id, "nested", replaced_nested_id, tx1),

            self.child_node(scion_id, "remove", to_remove_id),
            self.child_node(scion_id, "remove", None, tx0),
            self.child_node(scion_id, "remove", None, tx1),

            self.child_node(to_remove_id, "recursive", to_remove_recursive_id),
            self.child_node(to_remove_id, "recursive", None, tx0),

            self.child_node(scion_id, "create", created_1_id, tx0),
            self.child_node(scion_id, "create", created_2_id, tx1),

            self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
            self.child_node(scion_id, "remove_recreate", None, tx1),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.node_forks, [
            self.node_fork(tx1, to_replace_id, "//tmp/s/replace", None),

            self.node_fork(tx0, to_replace_nested_id, "//tmp/s/replace/nested", None),

            self.node_fork(tx1, replaced_id, "//tmp/s/replace", tx1),

            self.node_fork(tx1, replaced_nested_id, "//tmp/s/replace/nested", tx1),

            self.node_fork(tx0, to_remove_id, "//tmp/s/remove", None),

            self.node_fork(tx0, to_remove_recursive_id, "//tmp/s/remove/recursive", None),

            self.node_fork(tx0, created_1_id, "//tmp/s/create", tx0),
            self.node_fork(tx1, created_1_id, "//tmp/s/create", tx0),

            self.node_fork(tx1, created_2_id, "//tmp/s/create", tx1),

            self.node_fork(tx1, remove_recreate_1_id, "//tmp/s/remove_recreate", None),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.path_forks, [
            self.path_fork(tx1, "//tmp/s/replace", replaced_id, None),

            self.path_fork(tx0, "//tmp/s/replace/nested", None, None),
            self.path_fork(tx1, "//tmp/s/replace/nested", replaced_nested_id, None),

            self.path_fork(tx0, "//tmp/s/remove", None, None),
            self.path_fork(tx1, "//tmp/s/remove", None, None),

            self.path_fork(tx0, "//tmp/s/remove/recursive", None, None),

            self.path_fork(tx0, "//tmp/s/create", created_1_id, tx0),
            self.path_fork(tx1, "//tmp/s/create", created_2_id, tx0),

            self.path_fork(tx1, "//tmp/s/remove_recreate", None, None),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.child_forks, [
            self.child_fork(tx1, scion_id, "replace", replaced_id, None),

            self.child_fork(tx0, to_replace_id, "nested", None, None),

            self.child_fork(tx1, replaced_id, "nested", replaced_nested_id, tx1),

            self.child_fork(tx0, scion_id, "remove", None, None),
            self.child_fork(tx1, scion_id, "remove", None, None),

            self.child_fork(tx0, to_remove_id, "recursive", None, None),

            self.child_fork(tx0, scion_id, "create", created_1_id, tx0),
            self.child_fork(tx1, scion_id, "create", created_2_id, tx0),

            self.child_fork(tx1, scion_id, "remove_recreate", None, None),
        ], tx_mapping)

        commit_transaction(tx1)

        self.check_resolve_table(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(scion_id, "//tmp/s"),

            self.node_id_to_path(to_replace_id, "//tmp/s/replace"),
            self.node_id_to_path(to_replace_id, "//tmp/s/replace", tx0, "tombstone"),

            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested"),
            self.node_id_to_path(to_replace_nested_id, "//tmp/s/replace/nested", tx0, "tombstone"),

            self.node_id_to_path(replaced_id, "//tmp/s/replace", tx0),

            self.node_id_to_path(replaced_nested_id, "//tmp/s/replace/nested", tx0),

            self.node_id_to_path(to_remove_id, "//tmp/s/remove"),
            self.node_id_to_path(to_remove_id, "//tmp/s/remove", tx0, "tombstone"),

            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive"),
            self.node_id_to_path(to_remove_recursive_id, "//tmp/s/remove/recursive", tx0, "tombstone"),

            self.node_id_to_path(created_2_id, "//tmp/s/create", tx0),

            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate"),
            self.node_id_to_path(remove_recreate_1_id, "//tmp/s/remove_recreate", tx0, "tombstone"),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, [
            self.path_to_node_id("//tmp/s", scion_id),

            self.path_to_node_id("//tmp/s/replace", to_replace_id),
            self.path_to_node_id("//tmp/s/replace", replaced_id, tx0),
            self.path_to_node_id("//tmp/s/replace/nested", to_replace_nested_id),
            self.path_to_node_id("//tmp/s/replace/nested", replaced_nested_id, tx0),

            self.path_to_node_id("//tmp/s/remove", to_remove_id),
            self.path_to_node_id("//tmp/s/remove", None, tx0),

            self.path_to_node_id("//tmp/s/remove/recursive", to_remove_recursive_id),
            self.path_to_node_id("//tmp/s/remove/recursive", None, tx0),

            self.path_to_node_id("//tmp/s/create", created_2_id, tx0),

            self.path_to_node_id("//tmp/s/remove_recreate", remove_recreate_1_id),
            self.path_to_node_id("//tmp/s/remove_recreate", None, tx0),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.child_node, [
            self.child_node(scion_id, "replace", to_replace_id),
            self.child_node(scion_id, "replace", replaced_id, tx0),

            self.child_node(to_replace_id, "nested", to_replace_nested_id),
            self.child_node(to_replace_id, "nested", None, tx0),

            self.child_node(replaced_id, "nested", replaced_nested_id, tx0),

            self.child_node(scion_id, "remove", to_remove_id),
            self.child_node(scion_id, "remove", None, tx0),

            self.child_node(to_remove_id, "recursive", to_remove_recursive_id),
            self.child_node(to_remove_id, "recursive", None, tx0),

            self.child_node(scion_id, "create", created_2_id, tx0),

            self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
            self.child_node(scion_id, "remove_recreate", None, tx0),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.node_forks, [
            self.node_fork(tx0, to_replace_id, "//tmp/s/replace", None),
            self.node_fork(tx0, to_replace_nested_id, "//tmp/s/replace/nested", None),
            self.node_fork(tx0, replaced_id, "//tmp/s/replace", tx0),
            self.node_fork(tx0, replaced_nested_id, "//tmp/s/replace/nested", tx0),
            self.node_fork(tx0, to_remove_id, "//tmp/s/remove", None),
            self.node_fork(tx0, to_remove_recursive_id, "//tmp/s/remove/recursive", None),
            self.node_fork(tx0, created_2_id, "//tmp/s/create", tx0),
            self.node_fork(tx0, remove_recreate_1_id, "//tmp/s/remove_recreate", None),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.path_forks, [
            self.path_fork(tx0, "//tmp/s/replace", replaced_id, None),
            self.path_fork(tx0, "//tmp/s/replace/nested", replaced_nested_id, None),
            self.path_fork(tx0, "//tmp/s/remove", None, None),
            self.path_fork(tx0, "//tmp/s/remove/recursive", None, None),
            self.path_fork(tx0, "//tmp/s/create", created_2_id, tx0),
            self.path_fork(tx0, "//tmp/s/remove_recreate", None, None),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.child_forks, [
            self.child_fork(tx0, scion_id, "replace", replaced_id, None),
            self.child_fork(tx0, to_replace_id, "nested", None, None),
            self.child_fork(tx0, replaced_id, "nested", replaced_nested_id, tx0),
            self.child_fork(tx0, scion_id, "remove", None, None),
            self.child_fork(tx0, to_remove_id, "recursive", None, None),
            self.child_fork(tx0, scion_id, "create", created_2_id, tx0),
            self.child_fork(tx0, scion_id, "remove_recreate", None, None),
        ], tx_mapping)

        commit_transaction(tx0)

        self.check_resolve_table(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(scion_id, "//tmp/s"),
            self.node_id_to_path(replaced_id, "//tmp/s/replace"),
            self.node_id_to_path(replaced_nested_id, "//tmp/s/replace/nested"),
            self.node_id_to_path(created_2_id, "//tmp/s/create"),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, [
            self.path_to_node_id("//tmp/s", scion_id),
            self.path_to_node_id("//tmp/s/replace", replaced_id),
            self.path_to_node_id("//tmp/s/replace/nested", replaced_nested_id),
            self.path_to_node_id("//tmp/s/create", created_2_id),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.child_node, [
            self.child_node(scion_id, "replace", replaced_id),
            self.child_node(replaced_id, "nested", replaced_nested_id),
            self.child_node(scion_id, "create", created_2_id),
        ], tx_mapping)

        self.check_resolve_table(DESCRIPTORS.node_forks, [], tx_mapping)
        self.check_resolve_table(DESCRIPTORS.path_forks, [], tx_mapping)
        self.check_resolve_table(DESCRIPTORS.child_forks, [], tx_mapping)
        self.check_resolve_table(DESCRIPTORS.node_snapshots, [], tx_mapping)


@authors("kvk1920")
@pytest.mark.enabled_multidaemon
class TestSequoiaNodeVersioningSimulation(SequoiaNodeVersioningBase):
    ENABLE_MULTIDAEMON = True
    # We need only the primary master with tx coordinator role.
    NUM_SECONDARY_MASTER_CELLS = 0

    def teardown_method(self, method):
        clear_table_in_ground(DESCRIPTORS.node_id_to_path)
        clear_table_in_ground(DESCRIPTORS.path_to_node_id)
        clear_table_in_ground(DESCRIPTORS.child_node)
        clear_table_in_ground(DESCRIPTORS.node_forks)
        clear_table_in_ground(DESCRIPTORS.node_snapshots)
        clear_table_in_ground(DESCRIPTORS.path_forks)
        clear_table_in_ground(DESCRIPTORS.child_forks)

        super(TestSequoiaNodeVersioningSimulation, self).teardown_method(method)

    @override
    def create_node(self, node_type, path, tx=None):
        # Object types.
        types = {
            "map_node": 303,
            "table": 401,
            "rootstock": 12001,  # NB: It's scion type.
        }

        cell_tag = 10
        parts = [
            randint(0, 2**30 - 1) | (2**30),  # Sequoia bit.
            randint(0, 2**32 - 1),
            (cell_tag << 16) | types[node_type],
            randint(0, 2**31 - 1)]
        return "-".join(map(lambda part: f"{part:x}", parts))

    @override
    def remove_node(self, node_id, tx):
        pass

    @override
    def fill_or_check_resolve_table(self, table_descriptor, rows, tx_mapping=None):
        insert_rows_to_ground(table_descriptor, rows=rows)

    @override
    def lock_node(self, node_id, tx, mode):
        pass


@authors("kvk1920")
@pytest.mark.enabled_multidaemon
class TestSequoiaNodeVersioningReal(SequoiaNodeVersioningBase):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_CYPRESS_PROXIES = 1
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["sequoia_node_host", "cypress_node_host", "chunk_host"]},
        "12": {"roles": ["transaction_coordinator", "cypress_node_host"]},
        "13": {"roles": ["chunk_host", "cypress_node_host"]}
    }

    @override
    def create_node(self, node_type, path, tx=None):
        node_id = create(node_type, path, tx=tx or "0-0-0-0", force=True)
        return node_id if node_type != "rootstock" else get(f"#{node_id}&/@scion_id")

    @override
    def remove_node(self, path, tx):
        assert tx
        remove(path, tx=tx)

    @override
    def lock_node(self, node_id, tx, mode):
        lock(f"#{node_id}", tx=tx, mode=mode)

    @override
    def fill_or_check_resolve_table(self, table_descriptor, rows, tx_mapping=None):
        self.check_resolve_table(table_descriptor, rows, tx_mapping)

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_remove_and_get(self, finish_tx):
        create("rootstock", "//tmp/s")
        initial_tree = {
            "a": {
                "b1": {"b2": {}},
                "c1": {"c2": {}},
                "d1": {"d2": {}},
            },
        }

        set("//tmp/s", initial_tree, force=True)

        scion_id = get("//tmp/s/@id")
        a_id = get("//tmp/s/a/@id")
        b1_id = get("//tmp/s/a/b1/@id")
        b2_id = get("//tmp/s/a/b1/b2/@id")
        c1_id = get("//tmp/s/a/c1/@id")
        c2_id = get("//tmp/s/a/c1/c2/@id")
        d1_id = get("//tmp/s/a/d1/@id")
        d2_id = get("//tmp/s/a/d1/d2/@id")

        initial_rows = [
            self.child_node(scion_id, "a", a_id),
            self.child_node(a_id, "b1", b1_id),
            self.child_node(b1_id, "b2", b2_id),
            self.child_node(a_id, "c1", c1_id),
            self.child_node(c1_id, "c2", c2_id),
            self.child_node(a_id, "d1", d1_id),
            self.child_node(d1_id, "d2", d2_id),
        ]

        self.check_resolve_table(DESCRIPTORS.child_node, rows=initial_rows)

        tx = start_transaction()
        remove("//tmp/s/a/c1", tx=tx)

        tree_after_remove = deepcopy(initial_tree)
        del tree_after_remove["a"]["c1"]

        rows_after_remove = initial_rows + [
            self.child_node(a_id, "c1", None, tx),
            self.child_node(c1_id, "c2", None, tx),
        ]

        self.check_resolve_table(DESCRIPTORS.child_node, rows=rows_after_remove)

        assert get("//tmp/s") == initial_tree
        assert get("//tmp/s", tx=tx) == tree_after_remove

        finish_tx(tx)

        if finish_tx is commit_transaction:
            final_rows = [
                self.child_node(scion_id, "a", a_id),
                self.child_node(a_id, "b1", b1_id),
                self.child_node(b1_id, "b2", b2_id),
                self.child_node(a_id, "d1", d1_id),
                self.child_node(d1_id, "d2", d2_id),
            ]
            final_tree = tree_after_remove
        else:
            final_rows = initial_rows
            final_tree = initial_tree

        assert get("//tmp/s") == final_tree
        self.check_resolve_table(DESCRIPTORS.child_node, rows=final_rows)

    def test_remove_lock_conflict(self):
        create("rootstock", "//tmp/s")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        a = [{"key": "a", "value": 1}]
        b = [{"key": "b", "value": 2}]
        c = [{"key": "c", "value": 3}]

        tab_id = create("table", "//tmp/s/tab")
        write_table("//tmp/s/tab", a)
        write_table("<append=%true>//tmp/s/tab", b, tx=tx1)
        write_table("<append=%true>//tmp/s/tab", c, tx=tx2)

        assert read_table("//tmp/s/tab") == a
        assert read_table("//tmp/s/tab", tx=tx1) == a + b
        assert read_table("//tmp/s/tab", tx=tx2) == a + b + c

        commit_transaction(tx2)

        assert read_table("//tmp/s/tab") == a
        assert read_table("//tmp/s/tab", tx=tx1) == a + b + c

        with raises_yt_error("by concurrent"):
            remove("//tmp/s/tab")

        remove("//tmp/s/tab", tx=tx1)

        assert read_table("//tmp/s/tab") == a

        with raises_yt_error("no child with key \"tab\""):
            read_table("//tmp/s/tab", tx=tx1)

        commit_transaction(tx1)

        with raises_yt_error("no child with key \"tab\""):
            read_table("//tmp/s/tab")

        with raises_yt_error("No such"):
            read_table(f"#{tab_id}")

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_snapshot_outlives_originator(self, finish_tx):
        tx = start_transaction(timeout=100000)

        create("rootstock", "//tmp/scion")
        scion_id = get("//tmp/scion/@id")
        table_id = create("table", "//tmp/scion/t")

        content = [{"a": 1, "b": "1"}, {"a": 2, "b": 2}]
        write_table("//tmp/scion/t", content)

        self.lock_node(table_id, tx, "snapshot")

        write_table("<append=%true>//tmp/scion/t", content)

        assert read_table("//tmp/scion/t") == content * 2
        assert read_table(f"#{table_id}", tx=tx) == content

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, rows=[
            self.path_to_node_id("//tmp/scion", scion_id),
            self.path_to_node_id("//tmp/scion/t", table_id),
        ])
        self.check_resolve_table(DESCRIPTORS.node_id_to_path, rows=[
            self.node_id_to_path(scion_id, "//tmp/scion"),
            self.node_id_to_path(table_id, "//tmp/scion/t"),
            self.node_id_to_path(table_id, "//tmp/scion/t", tx, "snapshot"),
        ])
        self.check_resolve_table(DESCRIPTORS.child_node, rows=[
            self.child_node(scion_id, "t", table_id),
        ])
        self.check_resolve_table(DESCRIPTORS.node_snapshots, rows=[
            self.node_snapshot(tx, table_id),
        ], tx_mapping={tx: "tx"})

        remove("//tmp/scion")

        assert exists(f"#{table_id}", tx=tx)

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, [])
        self.check_resolve_table(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(table_id, "//tmp/scion/t", tx, "snapshot"),
        ], tx_mapping={tx: "tx"})
        self.check_resolve_table(DESCRIPTORS.node_snapshots, rows=[
            self.node_snapshot(tx, table_id),
        ], tx_mapping={tx: "tx"})

        assert read_table(f"#{table_id}", tx=tx) == content

        finish_tx(tx)

        assert not exists(f"#{table_id}")

        self.check_resolve_table(DESCRIPTORS.node_id_to_path, [])

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_set_attribute_under_tx(self, finish_tx):
        create("table", "//tmp/t")
        tx = start_transaction()

        set("//tmp/t/@my_attribute", 123, tx=tx)
        with raises_yt_error("Attribute \"my_attribute\" is not found"):
            get("//tmp/t/@my_attribute")

        assert get("//tmp/t/@my_attribute", tx=tx) == 123

        with raises_yt_error(f"this attribute is locked by concurrent transaction {tx}"):
            set("//tmp/t/@my_attribute", 321)

        finish_tx(tx)

        if finish_tx is commit_transaction:
            assert get("//tmp/t/@my_attribute") == 123
        else:
            with raises_yt_error("Attribute \"my_attribute\" is not found"):
                get("//tmp/t/@my_attribute")

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_set_value_under_tx(self, finish_tx):
        create("int64_node", "//tmp/x")
        tx = start_transaction()

        set("//tmp/x", 1)
        set("//tmp/x", 2, tx=tx)
        assert get("//tmp/x") == 1
        assert get("//tmp/x", tx=tx) == 2

        # TODO(kvk1920): unwrap error becuse for now it looks like "Internal RPC
        # call failed: failed to commit Sequoia tx: prepare failed: ...
        # <actual error message>".
        with raises_yt_error(f" since \"exclusive\" lock is taken by concurrent transaction {tx}"):
            set("//tmp/x", 3)

        finish_tx(tx)

        if finish_tx is commit_transaction:
            assert get("//tmp/x") == 2
        else:
            assert get("//tmp/x") == 1

    @pytest.mark.parametrize("finish_tx,do_copy", itertools.product(
        [commit_transaction, abort_transaction],
        [copy, move],
    ))
    def test_move_to_itself_under_tx(self, finish_tx, do_copy):
        # m1 - m2 - t
        #    ` i
        # copy/move m2 -> m1
        # m1 - t
        create("rootstock", "//tmp/scion")
        scion = get("//tmp/scion/@id")
        m1 = create("map_node", "//tmp/scion/m1")
        m2 = create("map_node", "//tmp/scion/m1/m2")
        t = create("table", "//tmp/scion/m1/m2/t")
        i = create("int64_node", "//tmp/scion/m1/i")

        content = [{"key": 1, "value": "1"}, {"key": 12, "value": "34"}]

        set(f"#{t}/@vital", False)
        set(f"#{t}/@my_attr", 123)
        write_table(f"#{t}", content)

        origin_path_to_node_id = [
            self.path_to_node_id("//tmp/scion", scion),
            self.path_to_node_id("//tmp/scion/m1", m1),
            self.path_to_node_id("//tmp/scion/m1/i", i),
            self.path_to_node_id("//tmp/scion/m1/m2", m2),
            self.path_to_node_id("//tmp/scion/m1/m2/t", t),
        ]

        origin_node_id_to_path = [
            self.node_id_to_path(scion, "//tmp/scion"),
            self.node_id_to_path(m1, "//tmp/scion/m1"),
            self.node_id_to_path(m2, "//tmp/scion/m1/m2"),
            self.node_id_to_path(t, "//tmp/scion/m1/m2/t"),
            self.node_id_to_path(i, "//tmp/scion/m1/i"),
        ]

        origin_child_node = [
            self.child_node(scion, "m1", m1),
            self.child_node(m1, "i", i),
            self.child_node(m1, "m2", m2),
            self.child_node(m2, "t", t),
        ]

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, rows=origin_path_to_node_id)
        self.check_resolve_table(DESCRIPTORS.node_id_to_path, rows=origin_node_id_to_path)
        self.check_resolve_table(DESCRIPTORS.child_node, rows=origin_child_node)
        self.check_resolve_table(DESCRIPTORS.path_forks, rows=[])
        self.check_resolve_table(DESCRIPTORS.node_forks, rows=[])
        self.check_resolve_table(DESCRIPTORS.child_forks, rows=[])

        def check_table(path, tx=None):
            tx = {} if tx is None else {"tx": tx}
            assert read_table(path, **tx) == content
            assert not get(f"{path}/@vital", **tx)
            assert get(f"{path}/@my_attr", **tx) == 123

        check_table("//tmp/scion/m1/m2/t")

        tx = start_transaction()

        new_m1 = do_copy("//tmp/scion/m1/m2", "//tmp/scion/m1", force=True, tx=tx)
        new_t = get("//tmp/scion/m1/t/@id", tx=tx)

        self.check_resolve_table(DESCRIPTORS.path_to_node_id, rows=origin_path_to_node_id + [
            self.path_to_node_id("//tmp/scion/m1", new_m1, tx),
            self.path_to_node_id("//tmp/scion/m1/i", None, tx),
            self.path_to_node_id("//tmp/scion/m1/m2", None, tx),
            self.path_to_node_id("//tmp/scion/m1/m2/t", None, tx),
            self.path_to_node_id("//tmp/scion/m1/t", new_t, tx),
        ])
        self.check_resolve_table(DESCRIPTORS.node_id_to_path, rows=origin_node_id_to_path + [
            self.node_id_to_path(m1, "//tmp/scion/m1", tx, "tombstone"),
            self.node_id_to_path(m2, "//tmp/scion/m1/m2", tx, "tombstone"),
            self.node_id_to_path(t, "//tmp/scion/m1/m2/t", tx, "tombstone"),
            self.node_id_to_path(i, "//tmp/scion/m1/i", tx, "tombstone"),
            self.node_id_to_path(new_m1, "//tmp/scion/m1", tx),
            self.node_id_to_path(new_t, "//tmp/scion/m1/t", tx),
        ])
        self.check_resolve_table(DESCRIPTORS.child_node, rows=origin_child_node + [
            self.child_node(scion, "m1", new_m1, tx),
            self.child_node(m1, "i", None, tx),
            self.child_node(m1, "m2", None, tx),
            self.child_node(m2, "t", None, tx),
            self.child_node(new_m1, "t", new_t, tx),
        ])
        self.check_resolve_table(DESCRIPTORS.path_forks, rows=[
            self.path_fork(tx, "//tmp/scion/m1", new_m1, None),
            self.path_fork(tx, "//tmp/scion/m1/i", None, None),
            self.path_fork(tx, "//tmp/scion/m1/m2", None, None),
            self.path_fork(tx, "//tmp/scion/m1/m2/t", None, None),
            self.path_fork(tx, "//tmp/scion/m1/t", new_t, tx),
        ])
        self.check_resolve_table(DESCRIPTORS.node_forks, rows=[
            self.node_fork(tx, m1, "//tmp/scion/m1", None),
            self.node_fork(tx, i, "//tmp/scion/m1/i", None),
            self.node_fork(tx, m2, "//tmp/scion/m1/m2", None),
            self.node_fork(tx, t, "//tmp/scion/m1/m2/t", None),
            self.node_fork(tx, new_m1, "//tmp/scion/m1", tx),
            self.node_fork(tx, new_t, "//tmp/scion/m1/t", tx),
        ])
        self.check_resolve_table(DESCRIPTORS.child_forks, rows=[
            self.child_fork(tx, scion, "m1", new_m1, None),
            self.child_fork(tx, m1, "i", None, None),
            self.child_fork(tx, m1, "m2", None, None),
            self.child_fork(tx, m2, "t", None, None),
            self.child_fork(tx, new_m1, "t", new_t, tx),
        ])

        check_table("//tmp/scion/m1/t", tx)
        check_table("//tmp/scion/m1/m2/t")

        finish_tx(tx)

        if finish_tx is abort_transaction:
            self.check_resolve_table(DESCRIPTORS.path_to_node_id, rows=origin_path_to_node_id)
            self.check_resolve_table(DESCRIPTORS.node_id_to_path, rows=origin_node_id_to_path)
            self.check_resolve_table(DESCRIPTORS.child_node, rows=origin_child_node)

            check_table("//tmp/scion/m1/m2/t")
        else:
            self.check_resolve_table(DESCRIPTORS.path_to_node_id, rows=[
                self.path_to_node_id("//tmp/scion", scion),
                self.path_to_node_id("//tmp/scion/m1", new_m1),
                self.path_to_node_id("//tmp/scion/m1/t", new_t),
            ])
            self.check_resolve_table(DESCRIPTORS.node_id_to_path, rows=[
                self.node_id_to_path(scion, "//tmp/scion"),
                self.node_id_to_path(new_m1, "//tmp/scion/m1"),
                self.node_id_to_path(new_t, "//tmp/scion/m1/t"),
            ])
            self.check_resolve_table(DESCRIPTORS.child_node, rows=[
                self.child_node(scion, "m1", new_m1),
                self.child_node(new_m1, "t", new_t),
            ])

            check_table("//tmp/scion/m1/t")

        self.check_resolve_table(DESCRIPTORS.path_forks, rows=[])
        self.check_resolve_table(DESCRIPTORS.node_forks, rows=[])
        self.check_resolve_table(DESCRIPTORS.child_forks, rows=[])

    @pytest.mark.parametrize("finish_tx,do_copy", itertools.product(
        [commit_transaction, abort_transaction],
        [copy, move],
    ))
    def test_copy_under_tx(self, finish_tx, do_copy):
        # source (under tx1):
        # a-b-c
        #  `d
        # destination (trunk):
        # e-f-g
        #  `h-i

        create("rootstock", "//tmp/scion")
        scion = get("//tmp/scion/@id")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        tx_mapping = {tx1: "topmost-tx", tx2: "nested-tx"}

        # source subtree
        a = create("map_node", "//tmp/scion/a", tx=tx1)
        b = create("map_node", "//tmp/scion/a/b", tx=tx1)
        c = create("map_node", "//tmp/scion/a/b/c", tx=tx1)
        d = create("map_node", "//tmp/scion/a/d", tx=tx1)

        # destination subtree
        e = create("map_node", "//tmp/scion/e")
        f = create("map_node", "//tmp/scion/e/f")
        g = create("map_node", "//tmp/scion/e/f/g")
        h = create("map_node", "//tmp/scion/e/h")
        i = create("map_node", "//tmp/scion/e/h/i")

        path_to_node_id_source = [
            self.path_to_node_id("//tmp/scion/a", a, tx1),
            self.path_to_node_id("//tmp/scion/a/b", b, tx1),
            self.path_to_node_id("//tmp/scion/a/b/c", c, tx1),
            self.path_to_node_id("//tmp/scion/a/d", d, tx1),
        ]

        path_to_node_id_destination = [
            self.path_to_node_id("//tmp/scion/e", e),
            self.path_to_node_id("//tmp/scion/e/f", f),
            self.path_to_node_id("//tmp/scion/e/f/g", g),
            self.path_to_node_id("//tmp/scion/e/h", h),
            self.path_to_node_id("//tmp/scion/e/h/i", i),
        ]

        node_id_to_path_source = [
            self.node_id_to_path(a, "//tmp/scion/a", tx1),
            self.node_id_to_path(b, "//tmp/scion/a/b", tx1),
            self.node_id_to_path(c, "//tmp/scion/a/b/c", tx1),
            self.node_id_to_path(d, "//tmp/scion/a/d", tx1),
        ]

        node_id_to_path_destination = [
            self.node_id_to_path(e, "//tmp/scion/e"),
            self.node_id_to_path(f, "//tmp/scion/e/f"),
            self.node_id_to_path(g, "//tmp/scion/e/f/g"),
            self.node_id_to_path(h, "//tmp/scion/e/h"),
            self.node_id_to_path(i, "//tmp/scion/e/h/i"),
        ]

        child_node_source = [
            self.child_node(scion, "a", a, tx1),
            self.child_node(a, "b", b, tx1),
            self.child_node(b, "c", c, tx1),
            self.child_node(a, "d", d, tx1),
        ]

        child_node_destination = [
            self.child_node(scion, "e", e),
            self.child_node(e, "f", f),
            self.child_node(f, "g", g),
            self.child_node(e, "h", h),
            self.child_node(h, "i", i),
        ]

        self.check_resolve_table(
            DESCRIPTORS.path_to_node_id,
            [self.path_to_node_id("//tmp/scion", scion)]
            + path_to_node_id_source
            + path_to_node_id_destination,
            tx_mapping)

        self.check_resolve_table(
            DESCRIPTORS.node_id_to_path,
            [self.node_id_to_path(scion, "//tmp/scion")]
            + node_id_to_path_source
            + node_id_to_path_destination,
            tx_mapping)

        self.check_resolve_table(
            DESCRIPTORS.child_node,
            child_node_source + child_node_destination,
            tx_mapping)

        with raises_yt_error("Node //tmp/scion/e already exists"):
            do_copy("//tmp/scion/a", "//tmp/scion/e", tx=tx2)

        do_copy("//tmp/scion/a", "//tmp/scion/e", tx=tx2, force=True)

        e2 = get("//tmp/scion/e/@id", tx=tx2)
        new_ids = {
            suffix: get(f"//tmp/scion/e/{suffix}/@id", tx=tx2)
            for suffix in ["b", "b/c", "d"]
        }

        path_to_node_id_delta = [
            # Target content is removed.
            r | {"transaction_id": tx2, "node_id": None}
            for r in path_to_node_id_destination
            if not r["path"].endswith("/e/")
        ] + [
            # Target node is recreated.
            self.path_to_node_id("//tmp/scion/e", e2, tx2)
        ] + [
            # New target content is created.
            self.path_to_node_id(f"//tmp/scion/e/{suffix}", new_ids[suffix], tx2)
            for suffix in ("b", "b/c", "d")
        ]
        node_id_to_path_delta = [
            r | {"transaction_id": tx2, "fork_kind": "tombstone"}
            for r in node_id_to_path_destination
        ] + [
            self.node_id_to_path(e2, "//tmp/scion/e", tx=tx2),
        ] + [
            self.node_id_to_path(new_ids[suffix], f"//tmp/scion/e/{suffix}", tx2)
            for suffix in ("b", "b/c", "d")
        ]
        child_node_delta = [
            r | {"transaction_id": tx2, "child_id": None}
            for r in child_node_destination
            if r["child_key"] != "e"
        ] + [
            self.child_node(scion, "e", e2, tx2),
        ] + [
            self.child_node(e2, "b", new_ids["b"], tx2),
            self.child_node(new_ids["b"], "c", new_ids["b/c"], tx2),
            self.child_node(e2, "d", new_ids["d"], tx2),
        ]

        if do_copy is move:
            # Source is removed.
            path_to_node_id_delta += [
                r | {"transaction_id": tx2, "node_id": None}
                for r in path_to_node_id_source
            ]
            node_id_to_path_delta += [
                r | {"transaction_id": tx2, "fork_kind": "tombstone"}
                for r in node_id_to_path_source
            ]
            child_node_delta += [
                r | {"child_id": None, "transaction_id": tx2}
                for r in child_node_source
            ]

        self.check_resolve_table(
            DESCRIPTORS.path_to_node_id,
            [self.path_to_node_id("//tmp/scion", scion)]
            + path_to_node_id_source
            + path_to_node_id_destination
            + path_to_node_id_delta,
            tx_mapping)

        self.check_resolve_table(
            DESCRIPTORS.node_id_to_path,
            [self.node_id_to_path(scion, "//tmp/scion")]
            + node_id_to_path_source
            + node_id_to_path_destination
            + node_id_to_path_delta,
            tx_mapping)

        self.check_resolve_table(
            DESCRIPTORS.child_node,
            child_node_source + child_node_destination + child_node_delta,
            tx_mapping)

        finish_tx(tx2)
        finish_tx(tx1)

        if finish_tx is abort_transaction:
            # Source sexists only under tx1.
            expected_path_to_node_id = \
                [self.path_to_node_id("//tmp/scion", scion)] \
                + path_to_node_id_destination
            expected_node_id_to_path = \
                [self.node_id_to_path(scion, "//tmp/scion")] \
                + node_id_to_path_destination
            expected_child_node = child_node_destination
        else:
            expected_path_to_node_id = [
                self.path_to_node_id("//tmp/scion", scion),
                self.path_to_node_id("//tmp/scion/e", e2),
                self.path_to_node_id("//tmp/scion/e/b", new_ids["b"]),
                self.path_to_node_id("//tmp/scion/e/b/c", new_ids["b/c"]),
                self.path_to_node_id("//tmp/scion/e/d", new_ids["d"]),
            ]
            expected_node_id_to_path = [
                self.node_id_to_path(scion, "//tmp/scion"),
                self.node_id_to_path(e2, "//tmp/scion/e"),
                self.node_id_to_path(new_ids["b"], "//tmp/scion/e/b"),
                self.node_id_to_path(new_ids["b/c"], "//tmp/scion/e/b/c"),
                self.node_id_to_path(new_ids["d"], "//tmp/scion/e/d"),
            ]
            expected_child_node = [
                self.child_node(scion, "e", e2),
                self.child_node(e2, "b", new_ids["b"]),
                self.child_node(new_ids["b"], "c", new_ids["b/c"]),
                self.child_node(e2, "d", new_ids["d"]),
            ]

            if do_copy is copy:
                expected_path_to_node_id += [
                    r | {"transaction_id": None}
                    for r in path_to_node_id_source
                ]
                expected_node_id_to_path += [
                    r | {"transaction_id": None}
                    for r in node_id_to_path_source
                ]
                expected_child_node += [
                    r | {"transaction_id": None}
                    for r in child_node_source
                ]

        self.check_resolve_table(
            DESCRIPTORS.path_to_node_id,
            expected_path_to_node_id,
            tx_mapping)
        self.check_resolve_table(
            DESCRIPTORS.node_id_to_path,
            expected_node_id_to_path,
            tx_mapping)
        self.check_resolve_table(
            DESCRIPTORS.child_node,
            expected_child_node,
            tx_mapping)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSequoiaMultipleCypressProxies(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 2
    NUM_SECONDARY_MASTER_CELLS = 0

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "user_directory_synchronizer": {
            "sync_period": 100,
        },
    }

    @authors("danilalexeev")
    @flaky(max_runs=3)
    def test_request_throttling(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"10": 100})

        set("//sys/cypress_proxies/@config", {
            "object_service": {
                "distributed_throttler": {
                    "member_client": {
                        "attribute_update_period": 300,
                        "heartbeat_period": 50,
                    },
                    "limit_update_period": 100,
                    "leader_update_period": 1500,
                },
            }
        })

        NUM_REQUESTS = 10

        def measure_read_time():
            start_time = datetime.now()
            for _ in range(NUM_REQUESTS):
                read_table("//tmp/t", authenticated_user="u")
            return (datetime.now() - start_time).total_seconds()

        # register user at both proxies
        assert measure_read_time() < 2

        # TODO(danilalexeev or aleksandra-zh): Change to 1 once fractional limits are fixed.
        set("//sys/users/u/@request_limits/read_request_rate/default", self.NUM_CYPRESS_PROXIES)
        sleep(1)

        cypress_proxy_address = ls("//sys/cypress_proxies")[0]
        profiler = profiler_factory().at_cypress_proxy(cypress_proxy_address)
        value_counter = profiler.counter("cypress_proxy/distributed_throttler/usage", tags={"throttler_id": "u_read_weight_throttler"})

        assert measure_read_time() * self.NUM_CYPRESS_PROXIES > NUM_REQUESTS * 0.8

        wait(lambda: value_counter.get() > 0, ignore_exceptions=True)

        set("//sys/users/u/@request_limits/read_request_rate/default", 100)
        sleep(1)

        assert measure_read_time() < 2


##################################################################


@authors("kvk1920")
@pytest.mark.enabled_multidaemon
class TestSequoiaTmpCleanup(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 0
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }

    PATHS_TO_CHECK = []

    def _do_test(self):
        for path in TestSequoiaTmpCleanup.PATHS_TO_CHECK:
            assert not exists(path)
        TestSequoiaTmpCleanup.PATHS_TO_CHECK = []

        def make_path(node_id):
            return "#" + node_id

        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/b")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/c")))
        TestSequoiaTmpCleanup.PATHS_TO_CHECK.append(make_path(get("//tmp/@rootstock_id")) + "&")

        for path in TestSequoiaTmpCleanup.PATHS_TO_CHECK:
            assert exists(path)

    def test1(self):
        self._do_test()

    def test2(self):
        self._do_test()
