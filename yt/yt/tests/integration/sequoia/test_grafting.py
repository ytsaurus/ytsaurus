from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, exists, copy, move, link, remove, wait,
    start_transaction, commit_transaction, abort_all_transactions,
    raises_yt_error, create_user, ls, lock,
)

from yt_sequoia_helpers import (
    resolve_sequoia_id, resolve_sequoia_path, select_rows_from_ground, select_paths_from_ground,
)

from yt.sequoia_tools import DESCRIPTORS

from yt.common import YtError

import pytest


################################################################################


@pytest.mark.enabled_multidaemon
class TestGrafting(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_CLOCKS = 3

    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["sequoia_node_host"]},
    }

    @authors("kvk1920")
    def test_create_rootstock_over_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("rootstock", "//tmp/p", force=True)
        assert get("//tmp/p/@type") == "scion"

    @authors("kvk1920", "gritukan")
    def test_cannot_create_scion(self):
        with raises_yt_error("Nodes of type \"scion\" cannot be created explicitly"):
            create("scion", "//tmp/s")

    @authors("kvk1920", "gritukan")
    def test_create_rootstock(self):
        rootstock_id = create("rootstock", "//tmp/r")
        scion_id = get("//tmp/r&/@scion_id")

        assert get("//tmp/r&/@parent_id") == get("//tmp/@id")
        assert get("//tmp/r/@parent_id") == get("//tmp/@id")

        assert select_paths_from_ground() == ["//tmp/r"]

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
        with raises_yt_error("Cannot clone a rootstock"):
            copy("//tmp/r&", "//tmp/r2")
        with raises_yt_error("Cannot clone a rootstock"):
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

    @authors("kvk1920")
    def test_rootstock_removal_under_tx(self):
        create("rootstock", "//tmp/sequoia")
        tx = start_transaction()
        with raises_yt_error("Rootstock cannot be removed under transaction"):
            remove("//tmp/sequoia", tx=tx)


##################################################################


@authors("kvk1920")
@pytest.mark.enabled_multidaemon
class TestGraftingTmpCleanup(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
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


@pytest.mark.enabled_multidaemon
class TestSequoiaSymlinks(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = True

    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host", "sequoia_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["sequoia_node_host"]},
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
    def test_link_through_sequoia1(self):
        id1 = create("table", "//cypress/t1")
        link("//cypress/t1", "//tmp/l1")
        link("//tmp/l1", "//tmp/l2")
        link("//tmp/l2", "//cypress/l3")
        link("//cypress/l3", "//cypress/l4")
        assert get("//tmp/l1/@id") == id1
        assert get("//tmp/l2/@id") == id1
        assert get("//cypress/l3/@id") == id1
        assert get("//cypress/l4/@id") == id1

    @authors("kvk1920", "danilalexeev")
    def test_link_through_sequoia2(self):
        m1 = create("map_node", "//cypress/m1")
        m2 = create("map_node", "//cypress/m1/m2")
        link(f"#{m1}", "//tmp/link_to_cypress")
        assert m2 == get("//tmp/link_to_cypress/m2/@id")

        s = create("map_node", "//tmp/s")
        link("//tmp/s", "//cypress/m1/m2/link_to_sequoia")
        assert s == get("//tmp/link_to_cypress/m2/link_to_sequoia/@id")

        tx = start_transaction()
        lock("//tmp/link_to_cypress/m2/link_to_sequoia", mode="snapshot", tx=tx)
        assert s in {r["node_id"] for r in select_rows_from_ground(f"* from [{DESCRIPTORS.node_snapshots.get_default_path()}]")}

    @authors("danilalexeev")
    def test_cypress_link_branch(self):
        set("//tmp/n1", 1)
        set("//tmp/n2", 2)

        link("//tmp/n1", "//cypress/link")
        assert get("//cypress/link") == 1

        tx0 = start_transaction(timeout=180000)
        remove("//cypress/link&", tx=tx0)
        with raises_yt_error('Node //cypress has no child with key "link"'):
            get("//cypress/link", tx=tx0)

        tx1 = start_transaction(tx=tx0, timeout=180000)
        link("//tmp/n2", "//cypress/link", tx=tx1)
        assert get("//cypress/link", tx=tx1) == 2

        commit_transaction(tx1)
        assert get("//cypress/link", tx=tx0) == 2

    @authors("danilalexeev")
    @pytest.mark.parametrize("from_object_id_resolve", [False, True])
    def test_cyclic_link_through_sequoia(self, from_object_id_resolve):
        cypress_id = get("//cypress&/@id")
        rootstock_id = get("//tmp&/@id")

        def adjust_path(path: str) -> str:
            if from_object_id_resolve:
                return (path.replace("//cypress", f"#{cypress_id}")
                        .replace("//tmp", f"#{rootstock_id}"))
            else:
                return path

        def make_link(target_path, link_path):
            link(adjust_path(target_path), adjust_path(link_path), force=True)

        with raises_yt_error("Failed to create link: link is cyclic"):
            make_link("//cypress/l", "//cypress/l")

        make_link("//cypress/l2", "//tmp/l1")
        make_link("//tmp/l3", "//cypress/l2")
        with raises_yt_error("Failed to create link: link is cyclic"):
            make_link("//tmp/l1", "//tmp/l3")

        make_link("//cypress/b", "//cypress/a")
        make_link("//tmp/c", "//cypress/b")
        with raises_yt_error("Failed to create link: link is cyclic"):
            make_link("//cypress/a", "//tmp/c")

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

    @authors("cherepashka", "danilalexeev")
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

    @authors("cherepashka")
    @pytest.mark.parametrize("make_link", [False, True])
    def test_prerequisite_revisions_restriction(self, make_link):
        create("map_node", "//cypress/test_node", recursive=True, force=True)
        revision_node_id = None
        if make_link:
            create("table", "//cypress/original_revision_node", recursive=True, force=True)
            revision_node_id = link("//cypress/original_revision_node", "//cypress/revision_node", force=True)
        else:
            revision_node_id = create("table", "//cypress/revision_node", recursive=True, force=True)
        home_id = get("//cypress/@id")
        revision = get("//cypress/revision_node/@revision")

        forbidden_paths = [
            "//cypress/revision_node",
            f"#{revision_node_id}",
            f"#{home_id}/revision_node",
        ]

        for prerequitise_path in forbidden_paths:
            with raises_yt_error("Requests with prerequisite paths different from target paths are prohibited in Cypress"):
                set(
                    "//tmp/test_node",
                    "test",
                    prerequisite_revisions=[
                        {
                            "path": prerequitise_path,
                            "revision": revision,
                        }
                    ],
                )
        with raises_yt_error("Requests with prerequisite paths different from target paths are prohibited in Cypress"):
            copy(
                "//cypress/test_node",
                "//cypress/test_node2",
                prerequisite_revisions=[
                    {
                        "path": "//cypress/revision_node",
                        "revision": revision,
                    }
                ],
            )

        fine_paths = [
            "//cypress/revision_node", "//cypress/revision_node&",
        ]

        for path in fine_paths:
            revision = get(f"{path}/@revision")
            prerequisite_revisions = [
                {
                    "path": path,
                    "revision": revision,
                },
            ]
            # Shouldn't throw.
            get(path, prerequisite_revisions=prerequisite_revisions)

        if not self.USE_SEQUOIA and get("//cypress/revision_node/@native_cell_tag") == get("//tmp/@native_cell_tag"):
            copy(
                "//cypress/revision_node",
                "//tmp/revision_node",
                prerequisite_revisions=[
                    {
                        "path": "//cypress/revision_node",
                        "revision": revision,
                    }
                ])
        else:
            with raises_yt_error("Cross-cell \"copy\"/\"move\" command does not support prerequisite revisions"):
                copy(
                    "//cypress/revision_node",
                    "//tmp/revision_node",
                    prerequisite_revisions=[
                        {
                            "path": "//cypress/revision_node",
                            "revision": revision,
                        }
                    ])
