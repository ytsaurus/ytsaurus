from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, ls, get, remove, build_master_snapshots, raises_yt_error,
    exists, set, copy, move, gc_collect, write_table, read_table, create_user,
    start_transaction, abort_transaction, commit_transaction, link, wait,
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
from datetime import datetime, timedelta
import functools
from random import randint
from time import sleep


##################################################################


class TestSequoiaEnvSetup(YTEnvSetup):
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


class TestSequoiaInternals(YTEnvSetup):
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


class TestSequoiaResolve(TestSequoiaInternals):
    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }


##################################################################


class TestSequoiaCypressTransactions(YTEnvSetup):
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
        # NB: every transaction should be present on primary cell since portal
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

        # NB: t5 is already replicated to cell 13.
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


@authors("kvk1920")
class TestSequoiaNodeVersioning(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    ENABLE_TMP_ROOTSTOCK = True
    NUM_TEST_PARTITIONS = 6

    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["sequoia_node_host", "cypress_node_host", "chunk_host"]},
        "12": {"roles": ["transaction_coordinator", "cypress_node_host"]},
        "13": {"roles": ["chunk_host", "cypress_node_host"]},
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

        # TODO(kvk1920): unwrap error becuse for now it looks like "Internal RPC call failed: failed
        # to commit Sequoia tx: prepare failed: ... <actual error message>".
        with raises_yt_error(f" since \"exclusive\" lock is taken by concurrent transaction {tx}"):
            set("//tmp/x", 3)

        finish_tx(tx)

        if finish_tx is commit_transaction:
            assert get("//tmp/x") == 2
        else:
            assert get("//tmp/x") == 1


##################################################################


@authors("kvk1920")
class TestSequoiaNodeVersioningOnTxFinish(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False

    NUM_SECONDARY_MASTER_CELLS = 0

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

    def teardown_method(self, method):
        clear_table_in_ground(DESCRIPTORS.node_id_to_path)
        clear_table_in_ground(DESCRIPTORS.path_to_node_id)
        clear_table_in_ground(DESCRIPTORS.child_node)
        clear_table_in_ground(DESCRIPTORS.node_forks)
        clear_table_in_ground(DESCRIPTORS.node_snapshots)
        clear_table_in_ground(DESCRIPTORS.path_forks)
        clear_table_in_ground(DESCRIPTORS.child_forks)

        super(TestSequoiaNodeVersioningOnTxFinish, self).teardown_method(method)

    # Object types.
    MAP_NODE = 303
    TABLE = 401
    SCION = 12001

    def make_id(self, object_type):
        cell_tag = 10
        parts = [
            randint(0, 2**30 - 1) | (2**30),  # Sequoia bit.
            randint(0, 2**32 - 1),
            (cell_tag << 16) | object_type,
            randint(0, 2**31 - 1)]
        return "-".join(map(lambda x: hex(x)[2:], parts))

    # Record helpers.

    def node_id_to_path(self, node, path, tx=None, fork="regular"):
        assert node is not None
        return {
            "node_id": node,
            "transaction_id": tx,
            "path": path,
            "target_path": "",
            "fork_kind": fork,
        }

    def path_to_node_id(self, path, node, tx=None):
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

    def node_fork(self, tx, node, path, topmost_tx):
        assert node is not None
        assert tx is not None
        return {
            "transaction_id": tx,
            "node_id": node,
            "path": path,
            "target_path": "",
            "topmost_transaction_id": topmost_tx,
        }

    def path_fork(self, tx, path, node, topmost_tx):
        assert tx is not None
        return {
            "transaction_id": tx,
            "path": mangle_sequoia_path(path),
            "node_id": node,
            "topmost_transaction_id": topmost_tx,
        }

    def child_fork(self, tx, parent, key, child, topmost_tx):
        assert tx is not None
        return {
            "transaction_id": tx,
            "parent_id": parent,
            "child_key": key,
            "child_id": child,
            "topmost_transaction_id": topmost_tx,
        }

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
            print_debug("actual:", actual)
            print_debug("exepcted:", expected)
        assert actual == expected

    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_snapshot(self, finish_tx):
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)
        tx2 = start_transaction(tx=tx1, timeout=100000)
        tx3 = start_transaction(tx=tx1, timeout=100000)
        tx4 = start_transaction(prerequisite_transaction_ids=[tx3, tx0], timeout=100000)
        tx5 = start_transaction(prerequisite_transaction_ids=[tx0], timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1", tx2: "tx2", tx3: "tx3", tx4: "tx4", tx5: "tx5"}

        node_id = self.make_id(self.TABLE)
        insert_rows_to_ground(DESCRIPTORS.node_id_to_path, rows=[
            self.node_id_to_path(node_id, "//scion/t", tx=tx, fork="snapshot")
            for tx in [tx0, tx1, tx2, tx3, tx4, tx5]
        ])
        insert_rows_to_ground(DESCRIPTORS.node_snapshots, rows=[
            self.node_snapshot(tx, node_id)
            for tx in [tx0, tx1, tx2, tx3, tx4, tx5]
        ])

        finish_tx(tx1)

        # All snapshot branches are created under tx1, tx2 and tx3 must be
        # removed. Snapshot branches under tx4 should be removed too because
        # prerequisite of tx4 is finished.

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"select * from [{DESCRIPTORS.node_snapshots.get_default_path()}]"),
            [self.node_snapshot(t, node_id) for t in (tx0, tx5)])
        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"select * from [{DESCRIPTORS.node_id_to_path.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [self.node_id_to_path(node_id, "//scion/t", tx=t, fork="snapshot") for t in (tx0, tx5)])

    @pytest.mark.parametrize("originator", ["trunk", "transaction"])
    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_subtree_replacement(self, originator, finish_tx):
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1"}

        # Originator (either trunk or tx0).
        tx_or = None if originator == "trunk" else tx0

        # trunk:
        #   s-a-b-c-d-e
        #      `f
        # tx1: remove //a/b and //a/f, then create [//a/b, //a/b/c, //a/b/g]
        #   s-a-b-c
        #        `g

        scion_id = self.make_id(self.SCION)
        a_id = self.make_id(self.MAP_NODE)
        b_id = self.make_id(self.MAP_NODE)
        c_id = self.make_id(self.MAP_NODE)
        d_id = self.make_id(self.MAP_NODE)
        e_id = self.make_id(self.TABLE)
        b_new_id = self.make_id(self.MAP_NODE)
        c_new_id = self.make_id(self.TABLE)
        f_id = self.make_id(self.TABLE)
        g_id = self.make_id(self.TABLE)

        old_node_id_to_path = [
            self.node_id_to_path(scion_id, "//s"),

            self.node_id_to_path(a_id, "//s/a", tx=tx_or),
            self.node_id_to_path(f_id, "//s/a/f", tx=tx_or),
            self.node_id_to_path(b_id, "//s/a/b", tx=tx_or),
            self.node_id_to_path(c_id, "//s/a/b/c", tx=tx_or),
            self.node_id_to_path(d_id, "//s/a/b/c/d", tx=tx_or),
            self.node_id_to_path(e_id, "//s/a/b/c/d/e", tx=tx_or),

            self.node_id_to_path(f_id, "//s/a/f", tx=tx1, fork="tombstone"),
            self.node_id_to_path(b_id, "//s/a/b", tx=tx1, fork="tombstone"),
            self.node_id_to_path(c_id, "//s/a/b/c", tx=tx1, fork="tombstone"),
            self.node_id_to_path(d_id, "//s/a/b/c/d", tx=tx1, fork="tombstone"),
            self.node_id_to_path(e_id, "//s/a/b/c/d/e", tx=tx1, fork="tombstone"),

            self.node_id_to_path(b_new_id, "//s/a/b", tx=tx1),
            self.node_id_to_path(c_new_id, "//s/a/b/c", tx=tx1),
            self.node_id_to_path(g_id, "//s/a/b/g", tx=tx1),
        ]
        old_path_to_node_id = [
            self.path_to_node_id("//s", scion_id),

            self.path_to_node_id("//s/a", a_id, tx=tx_or),
            self.path_to_node_id("//s/a/f", f_id, tx=tx_or),
            self.path_to_node_id("//s/a/b", b_id, tx=tx_or),
            self.path_to_node_id("//s/a/b/c", c_id, tx=tx_or),
            self.path_to_node_id("//s/a/b/c/d", d_id, tx=tx_or),
            self.path_to_node_id("//s/a/b/c/d/e", e_id, tx=tx_or),

            self.path_to_node_id("//s/a/b", b_new_id, tx=tx1),
            self.path_to_node_id("//s/a/b/c", c_new_id, tx=tx1),
            self.path_to_node_id("//s/a/b/g", g_id, tx=tx1),

            self.path_to_node_id("//s/a/f", None, tx=tx1),
            self.path_to_node_id("//s/a/b/c/d", None, tx=tx1),

            # NB: tomstones for nested removed nodes can be useful for resolve
            # optimization (i.e. don't lookup nodes near the scion).
            self.path_to_node_id("//s/a/b/c/d/e", None, tx=tx1),
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

            # NB: tombstones for the children of removed node are useless. There
            # is even no node with ID |b_id|. But we still keep them to simplify
            # resolve table modification: it's simplier to not distinguish
            # nested node and subtree root removal.
            self.child_node(b_id, "c", None, tx=tx1),
            self.child_node(c_id, "d", None, tx=tx1),
            self.child_node(d_id, "e", None, tx=tx1),
        ]
        old_node_forks = [
            self.node_fork(tx1, f_id, "//s/a/f", tx_or),
            self.node_fork(tx1, b_id, "//s/a/b", tx_or),
            self.node_fork(tx1, c_id, "//s/a/b/c", tx_or),
            self.node_fork(tx1, d_id, "//s/a/b/c/d", tx_or),
            self.node_fork(tx1, e_id, "//s/a/b/c/d/e", tx_or),
            self.node_fork(tx1, b_new_id, "//s/a/b", tx1),
            self.node_fork(tx1, c_new_id, "//s/a/b/c", tx1),
            self.node_fork(tx1, g_id, "//s/a/b/g", tx1),
        ]
        if originator == "transaction":
            old_node_forks += [
                self.node_fork(tx0, a_id, "//s/a", tx0),
                self.node_fork(tx0, f_id, "//s/a/f", tx0),
                self.node_fork(tx0, b_id, "//s/a/b", tx0),
                self.node_fork(tx0, c_id, "//s/a/b/c", tx0),
                self.node_fork(tx0, d_id, "//s/a/b/c/d", tx0),
                self.node_fork(tx0, e_id, "//s/a/b/c/d/e", tx0),
            ]
        old_path_forks = [
            self.path_fork(tx1, "//s/a/f", None, tx_or),
            self.path_fork(tx1, "//s/a/b", b_new_id, tx_or),
            self.path_fork(tx1, "//s/a/b/c", c_new_id, tx_or),
            self.path_fork(tx1, "//s/a/b/c/d", None, tx_or),
            self.path_fork(tx1, "//s/a/b/c/d/e", None, tx_or),
            self.path_fork(tx1, "//s/a/b/g", g_id, tx1),
        ]
        if originator == "transaction":
            old_path_forks += [
                self.path_fork(tx0, "//s/a", a_id, tx0),
                self.path_fork(tx0, "//s/a/f", f_id, tx0),
                self.path_fork(tx0, "//s/a/b", b_id, tx0),
                self.path_fork(tx0, "//s/a/b/c", c_id, tx0),
                self.path_fork(tx0, "//s/a/b/c/d", d_id, tx0),
                self.path_fork(tx0, "//s/a/b/c/d/e", d_id, tx0),
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

        insert_rows_to_ground(DESCRIPTORS.node_id_to_path, old_node_id_to_path)
        insert_rows_to_ground(DESCRIPTORS.path_to_node_id, old_path_to_node_id)
        insert_rows_to_ground(DESCRIPTORS.child_node, old_child_node)
        insert_rows_to_ground(DESCRIPTORS.node_forks, old_node_forks)
        insert_rows_to_ground(DESCRIPTORS.path_forks, old_path_forks)
        insert_rows_to_ground(DESCRIPTORS.child_forks, old_child_forks)

        finish_tx(tx1)

        # NB: we have to filter out symlinks which are mirrored to Sequoia.
        new_node_id_to_path = select_rows_from_ground(
            f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] "
            "where not is_prefix('//sys', path)")
        new_path_to_node_id = select_rows_from_ground(
            f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] "
            "where not is_prefix('//sys', path)")
        new_child_node = select_rows_from_ground(
            f"* from [{DESCRIPTORS.child_node.get_default_path()}]")
        new_node_forks = select_rows_from_ground(
            f"* from [{DESCRIPTORS.node_forks.get_default_path()}]")
        new_path_forks = select_rows_from_ground(
            f"* from [{DESCRIPTORS.path_forks.get_default_path()}]")
        new_child_forks = select_rows_from_ground(
            f"* from [{DESCRIPTORS.child_forks.get_default_path()}]")

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
                self.node_id_to_path(scion_id, "//s"),
                self.node_id_to_path(a_id, "//s/a", tx=tx0),
                self.node_id_to_path(b_new_id, "//s/a/b", tx=tx0),
                self.node_id_to_path(c_new_id, "//s/a/b/c", tx=tx0),
                self.node_id_to_path(g_id, "//s/a/b/g", tx=tx0),
            ]
            expected_path_to_node_id = [
                self.path_to_node_id("//s", scion_id),
                self.path_to_node_id("//s/a", a_id, tx=tx0),
                self.path_to_node_id("//s/a/b", b_new_id, tx=tx0),
                self.path_to_node_id("//s/a/b/c", c_new_id, tx=tx0),
                self.path_to_node_id("//s/a/b/g", g_id, tx=tx0),
            ]
            expected_child_node = [
                self.child_node(scion_id, "a", a_id, tx=tx0),
                self.child_node(a_id, "b", b_new_id, tx=tx0),
                self.child_node(b_new_id, "c", c_new_id, tx=tx0),
                self.child_node(b_new_id, "g", g_id, tx=tx0),
            ]
            expected_node_forks = [
                self.node_fork(tx0, a_id, "//s/a", tx0),
                self.node_fork(tx0, b_new_id, "//s/a/b", tx0),
                self.node_fork(tx0, c_new_id, "//s/a/b/c", tx0),
                self.node_fork(tx0, g_id, "//s/a/b/g", tx0),
            ]
            expected_path_forks = [
                self.path_fork(tx0, "//s/a", a_id, tx0),
                self.path_fork(tx0, "//s/a/b", b_new_id, tx0),
                self.path_fork(tx0, "//s/a/b/c", c_new_id, tx0),
                self.path_fork(tx0, "//s/a/b/g", g_id, tx0),
            ]
            expected_child_forks = [
                self.child_fork(tx0, scion_id, "a", a_id, tx0),
                self.child_fork(tx0, a_id, "b", b_new_id, tx0),
                self.child_fork(tx0, b_new_id, "c", c_new_id, tx0),
                self.child_fork(tx0, b_new_id, "g", g_id, tx0)
            ]

        self.assert_equal(tx_mapping, new_node_id_to_path, expected_node_id_to_path)
        self.assert_equal(tx_mapping, new_path_to_node_id, expected_path_to_node_id)
        self.assert_equal(tx_mapping, new_child_node, expected_child_node)
        self.assert_equal(tx_mapping, new_node_forks, expected_node_forks)
        self.assert_equal(tx_mapping, new_path_forks, expected_path_forks)
        self.assert_equal(tx_mapping, new_child_forks, expected_child_forks)

    def test_merge_committed_changes(self):
        tx0 = start_transaction(timeout=100000)
        tx1 = start_transaction(tx=tx0, timeout=100000)
        tx2 = start_transaction(tx=tx1, timeout=100000)

        tx_mapping = {tx0: "tx0", tx1: "tx1", tx2: "tx2"}

        scion_id = self.make_id(self.SCION)

        # trunk: //s/replace/nested
        # tx0: remove //s/replace/nested
        # tx1: remove //s/replace
        # tx2: create [//s/replace, //s/replace/nested]
        to_replace_id = self.make_id(self.MAP_NODE)
        to_replace_nested_id = self.make_id(self.TABLE)
        replaced_id = self.make_id(self.MAP_NODE)
        replaced_nested_id = self.make_id(self.TABLE)

        # trunk: //s/remove/recursive
        # tx0: remove //s/remove
        # tx1: create //s/remove + create //s/remove/nested
        # tx2: remove //s/remove
        to_remove_id = self.make_id(self.TABLE)
        recreated_id = self.make_id(self.MAP_NODE)
        recreated_nested_id = self.make_id(self.TABLE)

        # trunk: None
        # tx0: create //s/create
        # tx1: remove //s/create
        # tx2: create //s/create
        created_1_id = self.make_id(self.TABLE)
        created_2_id = self.make_id(self.TABLE)

        # trunk: //remove_recreate
        # tx1: replace //remove_recreate
        # tx2: remove //remove_recreate
        remove_recreate_1_id = self.make_id(self.TABLE)
        remove_recreate_2_id = self.make_id(self.TABLE)

        # node_id, path, tx_id, fork_kind="regular"
        insert_rows_to_ground(DESCRIPTORS.node_id_to_path, [
            self.node_id_to_path(scion_id, "//s"),

            self.node_id_to_path(to_replace_id, "//s/replace"),
            self.node_id_to_path(to_replace_id, "//s/replace", tx1, "tombstone"),

            self.node_id_to_path(to_replace_nested_id, "//s/replace/nested"),
            self.node_id_to_path(to_replace_nested_id, "//s/replace/nested", tx0, "tombstone"),

            self.node_id_to_path(replaced_id, "//s/replace", tx2),

            self.node_id_to_path(replaced_nested_id, "//s/replace/nested", tx2),

            self.node_id_to_path(to_remove_id, "//s/remove"),
            self.node_id_to_path(to_remove_id, "//s/remove", tx0, "tombstone"),

            self.node_id_to_path(recreated_id, "//s/remove", tx1),
            self.node_id_to_path(recreated_id, "//s/remove", tx2),

            self.node_id_to_path(recreated_nested_id, "//s/remove/nested", tx1),
            self.node_id_to_path(recreated_nested_id, "//s/remove/nested", tx2, "tombstone"),

            self.node_id_to_path(created_1_id, "//s/create", tx0),
            self.node_id_to_path(created_1_id, "//s/create", tx1, "tombstone"),

            self.node_id_to_path(created_2_id, "//s/create", tx2),

            self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate"),
            self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate", tx1, "tombstone"),

            self.node_id_to_path(remove_recreate_2_id, "//s/remove_recreate", tx1),
            self.node_id_to_path(remove_recreate_2_id, "//s/remove_recreate", tx2, "tombstone"),
        ])

        # path, node_id, tx_id=None
        insert_rows_to_ground(DESCRIPTORS.path_to_node_id, [
            self.path_to_node_id("//s", scion_id),

            self.path_to_node_id("//s/replace", to_replace_id),
            self.path_to_node_id("//s/replace", None, tx1),
            self.path_to_node_id("//s/replace", replaced_id, tx2),
            self.path_to_node_id("//s/replace/nested", to_replace_nested_id),
            self.path_to_node_id("//s/replace/nested", None, tx0),
            self.path_to_node_id("//s/replace/nested", replaced_nested_id, tx2),

            self.path_to_node_id("//s/remove", to_remove_id),
            self.path_to_node_id("//s/remove", None, tx0),
            self.path_to_node_id("//s/remove", recreated_id, tx1),
            self.path_to_node_id("//s/remove", None, tx2),
            self.path_to_node_id("//s/remove/nested", recreated_nested_id, tx1),
            self.path_to_node_id("//s/remove/nested", None, tx2),

            self.path_to_node_id("//s/create", created_1_id, tx0),
            self.path_to_node_id("//s/create", None, tx1),
            self.path_to_node_id("//s/create", created_2_id, tx2),

            self.path_to_node_id("//s/remove_recreate", remove_recreate_1_id),
            self.path_to_node_id("//s/remove_recreate", remove_recreate_2_id, tx1),
            self.path_to_node_id("//s/remove_recreate", None, tx2),
        ])

        # parent_id, path, node_id, tx_id=None
        insert_rows_to_ground(DESCRIPTORS.child_node, [
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

            self.child_node(recreated_id, "nested", recreated_nested_id, tx1),
            self.child_node(recreated_id, "nested", None, tx2),

            self.child_node(scion_id, "create", created_1_id, tx0),
            self.child_node(scion_id, "create", None, tx1),
            self.child_node(scion_id, "create", created_2_id, tx2),

            self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
            self.child_node(scion_id, "remove_recreate", remove_recreate_2_id, tx1),
            self.child_node(scion_id, "remove_recreate", None, tx2),
        ])

        # tx_id, node_id, path, topmost_tx_id
        insert_rows_to_ground(DESCRIPTORS.node_forks, [
            self.node_fork(tx1, to_replace_id, "//s/replace", None),

            self.node_fork(tx0, to_replace_nested_id, "//s/replace/nested", None),

            self.node_fork(tx2, replaced_id, "//s/replace", tx2),

            self.node_fork(tx2, replaced_nested_id, "//s/replace/nested", tx2),

            self.node_fork(tx0, to_remove_id, "//s/remove", None),

            self.node_fork(tx1, recreated_id, "//s/remove", tx1),
            self.node_fork(tx2, recreated_id, "//s/remove", tx1),

            self.node_fork(tx1, recreated_nested_id, "//s/remove/nested", tx1),
            self.node_fork(tx2, recreated_nested_id, "//s/remove/nested", tx1),

            self.node_fork(tx0, created_1_id, "//s/create", tx0),
            self.node_fork(tx1, created_1_id, "//s/create", tx0),

            self.node_fork(tx2, created_2_id, "//s/create", tx2),

            self.node_fork(tx1, remove_recreate_1_id, "//s/remove_recreate", None),

            self.node_fork(tx1, remove_recreate_2_id, "//s/remove_recreate", tx1),
            self.node_fork(tx2, remove_recreate_2_id, "//s/remove_recreate", tx1),
        ])

        # tx_id, path, node_id, parent_id, topmost_tx_id
        insert_rows_to_ground(DESCRIPTORS.path_forks, [
            self.path_fork(tx1, "//s/replace", None, None),
            self.path_fork(tx2, "//s/replace", replaced_id, None),

            self.path_fork(tx0, "//s/replace/nested", None, None),
            self.path_fork(tx2, "//s/replace/nested", replaced_nested_id, None),

            self.path_fork(tx0, "//s/remove", None, None),
            self.path_fork(tx1, "//s/remove", recreated_id, None),
            self.path_fork(tx2, "//s/remove", None, None),

            self.path_fork(tx1, "//s/remove/nested", recreated_nested_id, tx1),
            self.path_fork(tx2, "//s/remove/nested", None, tx1),

            self.path_fork(tx0, "//s/create", created_1_id, tx0),
            self.path_fork(tx1, "//s/create", None, tx0),
            self.path_fork(tx2, "//s/create", created_2_id, tx0),

            self.path_fork(tx1, "//s/remove_recreate", remove_recreate_2_id, None),
            self.path_fork(tx2, "//s/remove_recreate", None, None),
        ])

        # tx_id, parent, key, child, topmost_tx
        insert_rows_to_ground(DESCRIPTORS.child_forks, [
            self.child_fork(tx1, scion_id, "replace", None, None),
            self.child_fork(tx2, scion_id, "replace", replaced_id, None),

            self.child_fork(tx0, to_replace_id, "nested", None, None),

            self.child_fork(tx2, replaced_id, "nested", replaced_nested_id, tx2),

            self.child_fork(tx0, scion_id, "remove", None, None),
            self.child_fork(tx1, scion_id, "remove", recreated_id, None),
            self.child_fork(tx2, scion_id, "remove", None, None),

            self.child_fork(tx1, recreated_id, "nested", recreated_nested_id, tx1),
            self.child_fork(tx2, recreated_id, "nested", None, tx1),

            self.child_fork(tx0, scion_id, "create", created_1_id, tx0),
            self.child_fork(tx1, scion_id, "create", None, tx0),
            self.child_fork(tx2, scion_id, "create", created_2_id, tx0),

            self.child_fork(tx1, scion_id, "remove_recreate", remove_recreate_2_id, None),
            self.child_fork(tx2, scion_id, "remove_recreate", None, None),
        ])

        commit_transaction(tx2)

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.node_id_to_path(scion_id, "//s"),

                self.node_id_to_path(to_replace_id, "//s/replace"),
                self.node_id_to_path(to_replace_id, "//s/replace", tx1, "tombstone"),

                self.node_id_to_path(to_replace_nested_id, "//s/replace/nested"),
                self.node_id_to_path(to_replace_nested_id, "//s/replace/nested", tx0, "tombstone"),

                self.node_id_to_path(replaced_id, "//s/replace", tx1),

                self.node_id_to_path(replaced_nested_id, "//s/replace/nested", tx1),

                self.node_id_to_path(to_remove_id, "//s/remove"),
                self.node_id_to_path(to_remove_id, "//s/remove", tx0, "tombstone"),

                self.node_id_to_path(created_1_id, "//s/create", tx0),
                self.node_id_to_path(created_1_id, "//s/create", tx1, "tombstone"),

                self.node_id_to_path(created_2_id, "//s/create", tx1),

                self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate"),
                self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate", tx1, "tombstone"),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.path_to_node_id("//s", scion_id),

                self.path_to_node_id("//s/replace", to_replace_id),
                self.path_to_node_id("//s/replace", replaced_id, tx1),
                self.path_to_node_id("//s/replace/nested", to_replace_nested_id),
                self.path_to_node_id("//s/replace/nested", None, tx0),
                self.path_to_node_id("//s/replace/nested", replaced_nested_id, tx1),

                self.path_to_node_id("//s/remove", to_remove_id),
                self.path_to_node_id("//s/remove", None, tx0),
                self.path_to_node_id("//s/remove", None, tx1),

                self.path_to_node_id("//s/create", created_1_id, tx0),
                self.path_to_node_id("//s/create", created_2_id, tx1),

                self.path_to_node_id("//s/remove_recreate", remove_recreate_1_id),
                self.path_to_node_id("//s/remove_recreate", None, tx1),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_node.get_default_path()}]"),
            [
                self.child_node(scion_id, "replace", to_replace_id),
                self.child_node(scion_id, "replace", replaced_id, tx1),

                self.child_node(to_replace_id, "nested", to_replace_nested_id),
                self.child_node(to_replace_id, "nested", None, tx0),

                self.child_node(replaced_id, "nested", replaced_nested_id, tx1),

                self.child_node(scion_id, "remove", to_remove_id),
                self.child_node(scion_id, "remove", None, tx0),
                self.child_node(scion_id, "remove", None, tx1),

                self.child_node(scion_id, "create", created_1_id, tx0),
                self.child_node(scion_id, "create", created_2_id, tx1),

                self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
                self.child_node(scion_id, "remove_recreate", None, tx1),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.node_forks.get_default_path()}]"),
            [
                self.node_fork(tx1, to_replace_id, "//s/replace", None),

                self.node_fork(tx0, to_replace_nested_id, "//s/replace/nested", None),

                self.node_fork(tx1, replaced_id, "//s/replace", tx1),

                self.node_fork(tx1, replaced_nested_id, "//s/replace/nested", tx1),

                self.node_fork(tx0, to_remove_id, "//s/remove", None),

                self.node_fork(tx0, created_1_id, "//s/create", tx0),
                self.node_fork(tx1, created_1_id, "//s/create", tx0),

                self.node_fork(tx1, created_2_id, "//s/create", tx1),

                self.node_fork(tx1, remove_recreate_1_id, "//s/remove_recreate", None),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.path_forks.get_default_path()}]"),
            [
                self.path_fork(tx1, "//s/replace", replaced_id, None),

                self.path_fork(tx0, "//s/replace/nested", None, None),
                self.path_fork(tx1, "//s/replace/nested", replaced_nested_id, None),

                self.path_fork(tx0, "//s/remove", None, None),
                self.path_fork(tx1, "//s/remove", None, None),

                self.path_fork(tx0, "//s/create", created_1_id, tx0),
                self.path_fork(tx1, "//s/create", created_2_id, tx0),

                self.path_fork(tx1, "//s/remove_recreate", None, None),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_forks.get_default_path()}]"),
            [
                self.child_fork(tx1, scion_id, "replace", replaced_id, None),

                self.child_fork(tx0, to_replace_id, "nested", None, None),

                self.child_fork(tx1, replaced_id, "nested", replaced_nested_id, tx1),

                self.child_fork(tx0, scion_id, "remove", None, None),
                self.child_fork(tx1, scion_id, "remove", None, None),

                self.child_fork(tx0, scion_id, "create", created_1_id, tx0),
                self.child_fork(tx1, scion_id, "create", created_2_id, tx0),

                self.child_fork(tx1, scion_id, "remove_recreate", None, None),
            ])

        commit_transaction(tx1)

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.node_id_to_path(scion_id, "//s"),

                self.node_id_to_path(to_replace_id, "//s/replace"),
                self.node_id_to_path(to_replace_id, "//s/replace", tx0, "tombstone"),

                self.node_id_to_path(to_replace_nested_id, "//s/replace/nested"),
                self.node_id_to_path(to_replace_nested_id, "//s/replace/nested", tx0, "tombstone"),

                self.node_id_to_path(replaced_id, "//s/replace", tx0),

                self.node_id_to_path(replaced_nested_id, "//s/replace/nested", tx0),

                self.node_id_to_path(to_remove_id, "//s/remove"),
                self.node_id_to_path(to_remove_id, "//s/remove", tx0, "tombstone"),

                self.node_id_to_path(created_2_id, "//s/create", tx0),

                self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate"),
                self.node_id_to_path(remove_recreate_1_id, "//s/remove_recreate", tx0, "tombstone"),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.path_to_node_id("//s", scion_id),

                self.path_to_node_id("//s/replace", to_replace_id),
                self.path_to_node_id("//s/replace", replaced_id, tx0),
                self.path_to_node_id("//s/replace/nested", to_replace_nested_id),
                self.path_to_node_id("//s/replace/nested", replaced_nested_id, tx0),

                self.path_to_node_id("//s/remove", to_remove_id),
                self.path_to_node_id("//s/remove", None, tx0),

                self.path_to_node_id("//s/create", created_2_id, tx0),

                self.path_to_node_id("//s/remove_recreate", remove_recreate_1_id),
                self.path_to_node_id("//s/remove_recreate", None, tx0),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_node.get_default_path()}]"),
            [
                self.child_node(scion_id, "replace", to_replace_id),
                self.child_node(scion_id, "replace", replaced_id, tx0),

                self.child_node(to_replace_id, "nested", to_replace_nested_id),
                self.child_node(to_replace_id, "nested", None, tx0),

                self.child_node(replaced_id, "nested", replaced_nested_id, tx0),

                self.child_node(scion_id, "remove", to_remove_id),
                self.child_node(scion_id, "remove", None, tx0),

                self.child_node(scion_id, "create", created_2_id, tx0),

                self.child_node(scion_id, "remove_recreate", remove_recreate_1_id),
                self.child_node(scion_id, "remove_recreate", None, tx0),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.node_forks.get_default_path()}]"),
            [
                self.node_fork(tx0, to_replace_id, "//s/replace", None),
                self.node_fork(tx0, to_replace_nested_id, "//s/replace/nested", None),
                self.node_fork(tx0, replaced_id, "//s/replace", tx0),
                self.node_fork(tx0, replaced_nested_id, "//s/replace/nested", tx0),
                self.node_fork(tx0, to_remove_id, "//s/remove", None),
                self.node_fork(tx0, created_2_id, "//s/create", tx0),
                self.node_fork(tx0, remove_recreate_1_id, "//s/remove_recreate", None),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.path_forks.get_default_path()}]"),
            [
                self.path_fork(tx0, "//s/replace", replaced_id, None),
                self.path_fork(tx0, "//s/replace/nested", replaced_nested_id, None),
                self.path_fork(tx0, "//s/remove", None, None),
                self.path_fork(tx0, "//s/create", created_2_id, tx0),
                self.path_fork(tx0, "//s/remove_recreate", None, None),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_forks.get_default_path()}]"),
            [
                self.child_fork(tx0, scion_id, "replace", replaced_id, None),
                self.child_fork(tx0, to_replace_id, "nested", None, None),
                self.child_fork(tx0, replaced_id, "nested", replaced_nested_id, tx0),
                self.child_fork(tx0, scion_id, "remove", None, None),
                self.child_fork(tx0, scion_id, "create", created_2_id, tx0),
                self.child_fork(tx0, scion_id, "remove_recreate", None, None),
            ])

        commit_transaction(tx0)

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.node_id_to_path(scion_id, "//s"),
                self.node_id_to_path(replaced_id, "//s/replace"),
                self.node_id_to_path(replaced_nested_id, "//s/replace/nested"),
                self.node_id_to_path(created_2_id, "//s/create"),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(
                f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] "
                "where not is_prefix('//sys', path)"),
            [
                self.path_to_node_id("//s", scion_id),
                self.path_to_node_id("//s/replace", replaced_id),
                self.path_to_node_id("//s/replace/nested", replaced_nested_id),
                self.path_to_node_id("//s/create", created_2_id),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_node.get_default_path()}]"),
            [
                self.child_node(scion_id, "replace", replaced_id),
                self.child_node(replaced_id, "nested", replaced_nested_id),
                self.child_node(scion_id, "create", created_2_id),
            ])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.node_forks.get_default_path()}]"),
            [])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.path_forks.get_default_path()}]"),
            [])

        self.assert_equal(
            tx_mapping,
            select_rows_from_ground(f"* from [{DESCRIPTORS.child_forks.get_default_path()}]"),
            [])


##################################################################


class TestSequoiaMultipleCypressProxies(YTEnvSetup):
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
class TestSequoiaTmpCleanup(YTEnvSetup):
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
