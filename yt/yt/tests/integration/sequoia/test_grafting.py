from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, create, get, set, exists, copy, move,
    remove, wait, start_transaction, lookup_rows, select_rows,
    raises_yt_error, build_master_snapshots
)

from time import sleep

from yt.common import YtError

import pytest


##################################################################


def mangle_path(path):
    return path + "/"


def demangle_path(path):
    assert path.endswith("/")
    return path[:-1]

################################################################################


class TestSequoiaEnvSetup(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_MASTERS = 1
    NUM_CLOCKS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_REMOTE_CLUSTERS = 2
    USE_SEQUOIA_REMOTE_0 = False
    GROUND_INDEX_OFFSET = 10

    @authors("h0pless")
    def test1(self):
        sleep(10)  # Just don't crash...
        assert True

    @authors("h0pless")
    def test2(self):
        sleep(10)  # Just don't crash... (again)
        assert True

################################################################################


class TestGrafting(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_CLOCKS = 3

    NUM_SECONDARY_MASTER_CELLS = 3

    def _resolve_path(self, path):
        ground_driver = self.get_ground_driver()
        rows = lookup_rows("//sys/sequoia/resolve_node", [{"path": mangle_path(path)}], driver=ground_driver)
        if len(rows) == 0:
            return None
        assert len(rows) == 1
        return rows[0]["node_id"]

    def _resolve_id(self, node_id):
        ground_driver = self.get_ground_driver()
        rows = lookup_rows("//sys/sequoia/reverse_resolve_node", [{"node_id": node_id}], driver=ground_driver)
        assert len(rows) <= 1
        return rows[0]["path"] if rows else None

    @authors("kvk1920", "gritukan")
    def test_cannot_create_scion(self):
        with pytest.raises(YtError):
            create("scion", "//tmp/s")

    @authors("kvk1920", "gritukan")
    @pytest.mark.parametrize("rootstock_cell_tag", [10, 11])
    def test_create_rootstock(self, rootstock_cell_tag):
        rootstock_id = create("rootstock", "//tmp/r",
                              attributes={"scion_cell_tag": rootstock_cell_tag})
        scion_id = get("//tmp/r&/@scion_id")

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

        assert self._resolve_path("//tmp/r") == scion_id
        assert self._resolve_id(scion_id) == "//tmp/r"

        remove(f"#{scion_id}")

        wait(lambda: not exists("//tmp/r&"))
        wait(lambda: not exists(f"#{rootstock_id}"))
        wait(lambda: not exists(f"#{scion_id}"))

        wait(lambda: self._resolve_path("//tmp/r") is None)

    @authors("gritukan")
    def test_cannot_create_rootstock_in_transaction(self):
        tx = start_transaction()
        with pytest.raises(YtError):
            create("rootstock", "//tmp/p", attributes={"scion_cell_tag": 11}, tx=tx)

    @authors("gritukan")
    def test_cannot_copy_move_rootstock(self):
        create("rootstock", "//tmp/r", attributes={"scion_cell_tag": 11})
        with pytest.raises(YtError):
            copy("//tmp/r&", "//tmp/r2")
        with pytest.raises(YtError):
            move("//tmp/r&", "//tmp/r2")

    @authors("kvk1920", "gritukan")
    def test_create_map_node(self):
        create("rootstock", "//tmp/r", attributes={"scion_cell_tag": 10})
        m_id = create("map_node", "//tmp/r/m")
        # TODO(kvk1920): Use path when `set` will be implemented in Sequoia.
        set(f"#{m_id}/@foo", "bar")

        def check_everything():
            assert self._resolve_path("//tmp/r") == get("//tmp/r&/@scion_id")
            assert self._resolve_id(get("//tmp/r&/@scion_id")) == "//tmp/r"
            assert self._resolve_path("//tmp/r/m") == m_id
            assert get(f"#{m_id}/@path") == "//tmp/r/m"
            assert get(f"#{m_id}/@key") == "m"
            assert get("//tmp/r/m/@path") == "//tmp/r/m"
            assert get("//tmp/r/m/@key") == "m"

            # TODO(kvk1920): Use attribute filter when it will be implemented in Sequoia.
            assert get(f"#{m_id}/@type") == "map_node"
            assert get(f"#{m_id}/@sequoia")

            assert get(f"#{m_id}/@foo") == "bar"

        check_everything()

        build_master_snapshots()

        # TODO(kvk1920): Move it to TestMasterSnapshots.
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        check_everything()

    @authors("kvk1920")
    def test_resolve(self):
        rootstock_id = create("rootstock", "//tmp/r", attributes={"scion_cell_tag": 10})
        scion_id = get(f"#{rootstock_id}&/@scion_id")
        assert get("//tmp/r/@id") == scion_id

    @authors("kvk1920")
    def test_scion_removal(self):
        rootstock_id = create("rootstock", "//tmp/sequoia", attributes={"scion_cell_tag": 10})
        create("map_node", "//tmp/sequoia/m1")
        create("map_node", "//tmp/sequoia/m1/m2")
        remove("//tmp/sequoia", recursive=True)
        ground_driver = self.get_ground_driver()
        assert select_rows("* from [//sys/sequoia/resolve_node]", driver=ground_driver) == []
        assert select_rows("* from [//sys/sequoia/reverse_resolve_node]", driver=ground_driver) == []
        assert not exists(f"#{rootstock_id}")

    @authors("kvk1920")
    def test_sequoia_map_node_explicit_creation_is_forbidden(self):
        create("rootstock", "//tmp/sequoia", attributes={"scion_cell_tag": 10})
        with raises_yt_error("is internal type and should not be used directly"):
            create("sequoia_map_node", "//tmp/sequoia/m")


##################################################################


@authors("kvk1920")
class TestGraftingTmpCleanup(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 0

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
