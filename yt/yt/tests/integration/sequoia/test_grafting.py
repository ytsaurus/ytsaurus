from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, exists, copy, move,
    remove, wait, start_transaction,
    raises_yt_error,
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
