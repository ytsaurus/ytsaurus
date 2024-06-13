from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, ls, get, remove, build_master_snapshots, raises_yt_error,
    exists, set, copy, move, gc_collect, write_table, read_table, create_user,
    start_transaction, abort_transaction, commit_transaction, link, wait
)

from yt_sequoia_helpers import (
    resolve_sequoia_id, resolve_sequoia_path, select_rows_from_ground,
    select_paths_from_ground,
    lookup_cypress_transaction, select_cypress_transaction_replicas,
    select_cypress_transaction_descendants, clear_table_in_ground,
    select_cypress_transaction_prerequisites, lookup_rows_in_ground,
    mangle_sequoia_path
)

from yt.sequoia_tools import DESCRIPTORS

import yt.yson as yson
from yt.common import YtError

import pytest
import builtins

from time import sleep
from datetime import datetime, timedelta


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

    def lookup_path_to_node_id(self, path):
        return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

    @authors("kvk1920")
    def test_map_not_set_doesnt_cause_detach(self):
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
        assert get("//tmp") == {"some_dir": {"table": yson.YsonEntity()}}
        write_table("//tmp/some_dir/table", [{"x": "hello"}])

        # We should not read anything from table with get.
        assert get("//tmp") == {"some_dir": {"table": yson.YsonEntity()}}
        assert read_table("//tmp/some_dir/table") == [{"x": "hello"}]

    @authors("h0pless")
    def test_get(self):
        create("map_node", "//tmp/test_node")
        assert get("//tmp") == {"test_node": {}}
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
        with pytest.raises(YtError):
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
            {'path': '//tmp/'},
            {'path': '//tmp/other/'},
            {'path': '//tmp/other/s1/'},
            {'path': '//tmp/other/s2/'},
        ]

        if copy_mode == "copy":
            copy("//tmp/strings", "//tmp/other")
            assert select_paths_from_ground() == COMMON_ROWS + [
                {'path': '//tmp/strings/'},
                {'path': '//tmp/strings/s1/'},
                {'path': '//tmp/strings/s2/'},
            ]

            # Let's do it twice for good measure.
            copy("//tmp/strings", "//tmp/other_other")
            assert select_paths_from_ground() == COMMON_ROWS + [
                {'path': '//tmp/other_other/'},
                {'path': '//tmp/other_other/s1/'},
                {'path': '//tmp/other_other/s2/'},
                {'path': '//tmp/strings/'},
                {'path': '//tmp/strings/s1/'},
                {'path': '//tmp/strings/s2/'},
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
            {'path': '//tmp/'},
            {'path': '//tmp/d/'},
            {'path': '//tmp/d/s/'},
            {'path': '//tmp/d/s/t/'},
            {'path': '//tmp/d/s/t/a/'},
            {'path': '//tmp/d/s/t/a/b/'},
            {'path': '//tmp/d/s/t/a/c/'},
            {'path': '//tmp/d/s/t/d/'},
        ]

        if copy_mode == "copy":
            copy("//tmp/src", "//tmp/d/s/t", recursive=True)
            assert select_paths_from_ground() == COMMON_ROWS + [
                {'path': '//tmp/src/'},
                {'path': '//tmp/src/a/'},
                {'path': '//tmp/src/a/b/'},
                {'path': '//tmp/src/a/c/'},
                {'path': '//tmp/src/d/'},
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
            {'path': '//tmp/'},
            {'path': '//tmp/dst/'},
            {'path': '//tmp/dst/a/'},
            {'path': '//tmp/dst/a/b/'},
            {'path': '//tmp/dst/a/c/'},
            {'path': '//tmp/dst/d/'},
        ]

        if not is_excessive:
            create("map_node", "//tmp/dst/some/unimportant/stuff/to/overwrite", recursive=True)

        if copy_mode == "copy":
            copy("//tmp/src", "//tmp/dst", force=True)
            assert select_paths_from_ground() == COMMON_ROWS + [
                {'path': '//tmp/src/'},
                {'path': '//tmp/src/a/'},
                {'path': '//tmp/src/a/b/'},
                {'path': '//tmp/src/a/c/'},
                {'path': '//tmp/src/d/'},
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
            {'path': '//tmp/'},
            {'path': '//tmp/dst/'},
            {'path': '//tmp/dst/a/'},
            {'path': '//tmp/dst/a/b/'},
            {'path': '//tmp/dst/a/c/'},
            {'path': '//tmp/dst/d/'},
        ]

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
        with pytest.raises(YtError):
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
        with pytest.raises(YtError, match="List nodes cannot be created inside Sequoia"):
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
        with raises_yt_error("forbidden"):
            set("//tmp/m", {"a": 0})
        set("//tmp/m", {"a": 0}, force=True)
        assert ls("//tmp/m") == ["a"]
        assert exists(f"#{node_id}")
        assert get("//tmp/m/@id") == node_id

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
        with pytest.raises(YtError):
            create("map_node", "//tmp/special@&*[{symbols")
        path = r"//tmp/special\\\/\@\&\*\[\{symbols"
        create("map_node", path + "/m", recursive=True)
        assert r"special\/@&*[{symbols" in ls("//tmp")
        assert "m" in ls(path)
        child_id = create("map_node", r"//tmp/m\@1")
        assert get(r"//tmp/m\@1/@id") == child_id

    @authors("danilalexeev")
    def test_link_through_sequoia(self):
        id1 = create("table", "//home/t1", recursive=True)
        link("//home/t1", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        link("//tmp/l2", "//home/l3")
        wait(lambda: len(self.lookup_path_to_node_id('//home/l3')) == 1)
        link("//home/l3", "//home/l4")
        wait(lambda: len(self.lookup_path_to_node_id('//home/l4')) == 1)
        assert get("//tmp/l1/@id") == id1
        assert get("//tmp/l2/@id") == id1
        assert get("//home/l3/@id") == id1
        assert get("//home/l4/@id") == id1
        remove("//home", force=True, recursive=True)

    @authors("danilalexeev")
    def test_cyclic_link_through_sequoia(self):
        link("//home/l2", "//tmp/l1", force=True)
        link("//tmp/l3", "//home/l2", force=True, recursive=True)
        wait(lambda: len(self.lookup_path_to_node_id('//home/l2')) == 1)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/l1", "//tmp/l3", force=True)
        remove("//home", force=True, recursive=True)

    @authors("danilalexeev")
    def test_broken_links(self):
        set("//tmp/t1", 1)
        set("//home/t2", 2, recursive=True)
        link("//home/t2", "//tmp/l1")
        link("//tmp/t1", "//home/l2")
        assert not get("//tmp/l1&/@broken")
        assert not get("//home/l2&/@broken")
        remove("//tmp/t1")
        remove("//home/t2")
        assert get("//tmp/l1&/@broken")
        assert get("//home/l2&/@broken")
        remove("//home", force=True, recursive=True)

    @authors("kvk1920", "gritukan")
    def test_create_map_node(self):
        m_id = create("map_node", "//tmp/m")
        # TODO(kvk1920): Support attribute setting.
        # set(f"#{m_id}/@foo", "bar")

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

            # TODO(kvk1920): Support attribute setting.
            # assert get(f"#{m_id}/@foo") == "bar"

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
