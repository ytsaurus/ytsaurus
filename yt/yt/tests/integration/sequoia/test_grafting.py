from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, exists, copy, move, link, remove, wait,
    start_transaction, commit_transaction, abort_all_transactions,
    raises_yt_error,
)

from yt_sequoia_helpers import (
    resolve_sequoia_id, resolve_sequoia_path, select_rows_from_ground, select_paths_from_ground,
    lookup_rows_in_ground, mangle_sequoia_path,
)

from yt.sequoia_tools import DESCRIPTORS

from yt.common import YtError

import pytest


################################################################################


@pytest.mark.enabled_multidaemon
class TestGrafting(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_CLOCKS = 3

    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["sequoia_node_host"]},
    }

    @authors("kvk1920", "gritukan")
    def test_cannot_create_scion(self):
        with pytest.raises(YtError):
            create("scion", "//tmp/s")

    @authors("kvk1920", "gritukan")
    def test_create_rootstock(self):
        rootstock_id = create("rootstock", "//tmp/r")
        scion_id = get("//tmp/r&/@scion_id")

        assert get("//tmp/r&/@parent_id") == get("//tmp/@id")
        assert get("//tmp/r/@parent_id") == get("//tmp/@id")

        assert select_paths_from_ground() == ["//tmp/r/"]

        assert get(f"#{rootstock_id}&/@type") == "rootstock"
        assert get(f"#{rootstock_id}&/@scion_id") == scion_id
        assert get(f"#{scion_id}/@type") == "scion"
        assert get(f"#{scion_id}/@rootstock_id") == rootstock_id

        assert get("//tmp/r&/@type") == "rootstock"
        assert get("//tmp/r&/@scion_id") == scion_id
        assert get("//tmp/r/@type") == "scion"
        assert get("//tmp/r/@rootstock_id") == rootstock_id

        assert exists(f"//sys/rootstocks/{rootstock_id}")
        assert exists(f"//sys/scions/{scion_id}")

        assert resolve_sequoia_path("//tmp/r") == scion_id
        assert resolve_sequoia_id(scion_id) == "//tmp/r"

        remove(f"#{scion_id}")

        wait(lambda: not exists("//tmp/r&"))
        wait(lambda: not exists(f"#{rootstock_id}"))
        wait(lambda: not exists(f"#{scion_id}"))

        wait(lambda: resolve_sequoia_path("//tmp/r") is None)

    @authors("kvk1920", "gritukan")
    def test_cannot_create_rootstock_in_transaction(self):
        tx = start_transaction()
        with pytest.raises(YtError):
            create("rootstock", "//tmp/p", tx=tx)

    @authors("kvk1920", "gritukan")
    def test_cannot_copy_move_rootstock(self):
        create("rootstock", "//tmp/r")
        with pytest.raises(YtError):
            copy("//tmp/r&", "//tmp/r2")
        with pytest.raises(YtError):
            move("//tmp/r&", "//tmp/r2")

    @authors("kvk1920")
    def test_resolve(self):
        rootstock_id = create("rootstock", "//tmp/r")
        scion_id = get(f"#{rootstock_id}&/@scion_id")
        assert get("//tmp/r/@id") == scion_id

    @authors("kvk1920")
    def test_scion_removal(self):
        rootstock_id = create("rootstock", "//tmp/sequoia")
        create("map_node", "//tmp/sequoia/m1")
        create("map_node", "//tmp/sequoia/m1/m2")
        remove("//tmp/sequoia", recursive=True)
        assert select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] where not is_substr('//sys', path)") == []
        assert select_rows_from_ground(f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] where not is_substr('//sys', path)") == []
        assert not exists(f"#{rootstock_id}")

    @authors("kvk1920")
    def test_sequoia_map_node_explicit_creation_is_forbidden(self):
        create("rootstock", "//tmp/sequoia")
        with raises_yt_error("is internal type and should not be used directly"):
            create("sequoia_map_node", "//tmp/sequoia/m")

    @authors("kvk1920")
    def test_create_rootstock_in_unexisting_map_node(self):
        with raises_yt_error("Node //tmp has no child with key \"unexisting\""):
            create("rootstock", "//tmp/unexisting/r")

    @authors("kvk1920")
    def test_scion_properties(self):
        create("rootstock", "//tmp/sequoia1")
        create("rootstock", "//tmp/sequoia2")
        with raises_yt_error("Scion cannot be cloned"):
            copy("//tmp/sequoia1", "//tmp/sequoia2")


##################################################################


@authors("kvk1920")
@pytest.mark.enabled_multidaemon
class TestGraftingTmpCleanup(YTEnvSetup):
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
        for path in TestGraftingTmpCleanup.PATHS_TO_CHECK:
            assert not exists(path)
        TestGraftingTmpCleanup.PATHS_TO_CHECK = []

        def make_path(node_id):
            return "#" + node_id

        TestGraftingTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a")))
        TestGraftingTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/b")))
        TestGraftingTmpCleanup.PATHS_TO_CHECK.append(make_path(create("map_node", "//tmp/a/c")))
        TestGraftingTmpCleanup.PATHS_TO_CHECK.append(make_path(get("//tmp/@rootstock_id")) + "&")

        for path in TestGraftingTmpCleanup.PATHS_TO_CHECK:
            assert exists(path)

    def test1(self):
        self._do_test()

    def test2(self):
        self._do_test()


##################################################################


def lookup_path_to_node_id(path, tx=None):
    key = {"path": mangle_sequoia_path(path)}
    if tx is not None:
        key["transaction_id"] = tx
    return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [key])


@pytest.mark.enabled_multidaemon
class TestSequoiaSymlinks(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = True
    NUM_CYPRESS_PROXIES = 1
    USE_CYPRESS_DIR = True

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

    def setup_method(self, method):
        super().setup_method(method)

        create(
            "map_node",
            "//cypress",
            attributes={
                "account": "tmp",
                "acl": [
                    {
                        "action": "allow",
                        "permissions": ["read", "write", "remove"],
                        "subjects": ["users"],
                    }
                ],
                "opaque": True,
            },
            force=True)

    def teardown_method(self, method):
        abort_all_transactions()
        remove("//cypress", force=True)
        super().teardown_method(method)

    @authors("danilalexeev")
    def test_link_through_sequoia(self):
        id1 = create("table", "//cypress/t1")
        link("//cypress/t1", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        link("//tmp/l2", "//cypress/l3")
        wait(lambda: len(lookup_path_to_node_id('//cypress/l3')) == 1)
        link("//cypress/l3", "//cypress/l4")
        wait(lambda: len(lookup_path_to_node_id('//cypress/l4')) == 1)
        assert get("//tmp/l1/@id") == id1
        assert get("//tmp/l2/@id") == id1
        assert get("//cypress/l3/@id") == id1
        assert get("//cypress/l4/@id") == id1

    @authors("danilalexeev")
    def test_cypress_link_branch(self):
        set("//tmp/n1", 1)
        set("//tmp/n2", 2)

        link("//tmp/n1", "//cypress/link")
        wait(lambda: len(lookup_path_to_node_id("//cypress/link")) == 1)
        assert get("//cypress/link") == 1

        tx0 = start_transaction(timeout=180000)
        remove("//cypress/link&", tx=tx0)
        wait(lambda: len(lookup_path_to_node_id("//cypress/link", tx=tx0)) == 0)
        with raises_yt_error('Node //cypress has no child with key "link"'):
            get("//cypress/link", tx=tx0)

        tx1 = start_transaction(tx=tx0, timeout=180000)
        link("//tmp/n2", "//cypress/link", tx=tx1)
        wait(lambda: len(lookup_path_to_node_id("//cypress/link", tx=tx1)) == 1)
        assert get("//cypress/link", tx=tx1) == 2

        commit_transaction(tx1)
        # TODO(danilalexeev): Remove once GUQM sync is implemented.
        wait(lambda: len(lookup_path_to_node_id("//cypress/link", tx=tx0)) == 1)
        assert get("//cypress/link", tx=tx0) == 2

    @authors("danilalexeev")
    def test_cyclic_link_through_sequoia(self):
        link("//cypress/l2", "//tmp/l1", force=True)
        link("//tmp/l3", "//cypress/l2", force=True)
        wait(lambda: len(lookup_path_to_node_id("//cypress/l2")) == 1)
        with raises_yt_error("Failed to create link: link is cyclic"):
            link("//tmp/l1", "//tmp/l3", force=True)

    @authors("danilalexeev")
    def test_broken_links(self):
        set("//tmp/t1", 1)
        set("//cypress/t2", 2)
        link("//tmp/t1", "//cypress/l1")
        link("//cypress/t2", "//tmp/l2")
        assert not get("//cypress/l1&/@broken")
        assert not get("//tmp/l2&/@broken")
        remove("//tmp/t1")
        remove("//cypress/t2")
        assert get("//cypress/l1&/@broken")
        assert get("//tmp/l2&/@broken")
