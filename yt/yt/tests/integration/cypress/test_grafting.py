from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, exists, copy, move,
    remove, wait, start_transaction, lookup_rows, select_rows,
    gc_collect, delete_rows,
)

from yt.common import YtError

import pytest

##################################################################


class TestGrafting(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1

    NUM_SECONDARY_MASTER_CELLS = 3

    # TODO: Drop when removal will be implemented.
    def teardown_method(self, method):
        super(TestGrafting, self).teardown_method(method)

        gc_collect()
        wait(lambda: select_rows("* from [//sys/sequoia/resolve_node]") == [])

    def _resolve_path(self, path):
        rows = lookup_rows("//sys/sequoia/resolve_node", [{"path": path}])
        if len(rows) == 0:
            return None
        assert len(rows) == 1
        return rows[0]["node_id"]

    @authors("gritukan")
    def test_cannot_create_scion(self):
        with pytest.raises(YtError):
            create("scion", "//tmp/s")

    @authors("gritukan")
    @pytest.mark.parametrize("rootstock_cell_tag", [10, 11])
    @pytest.mark.parametrize("remove_first", ["rootstock", "scion"])
    def test_create_rootstock(self, rootstock_cell_tag, remove_first):
        rootstock_id = create("rootstock", "//tmp/r",
                              attributes={"scion_cell_tag": rootstock_cell_tag})
        scion_id = get("//tmp/r&/@scion_id")

        # TODO: Use paths here when resolver will be ready
        assert get(f"#{rootstock_id}&/@type") == "rootstock"
        assert get(f"#{rootstock_id}&/@scion_id") == scion_id
        assert get(f"#{scion_id}/@type") == "scion"
        assert get(f"#{scion_id}/@rootstock_id") == rootstock_id

        assert exists(f"//sys/rootstocks/{rootstock_id}")
        assert exists(f"//sys/scions/{scion_id}")

        assert self._resolve_path("//tmp/r") == scion_id

        if remove_first == "rootstock":
            remove(f"#{rootstock_id}&")
        else:
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

    @authors("gritukan")
    def test_create_map_node(self):
        create("rootstock", "//tmp/r", attributes={"scion_cell_tag": 10})
        assert self._resolve_path("//tmp/r") is not None
        m_id = create("map_node", "//tmp/r/m")
        assert self._resolve_path("//tmp/r/m") == m_id
        assert get(f"#{m_id}/@path") == "//tmp/r/m"
        assert get(f"#{m_id}/@key") == "m"

        set(f"#{m_id}/@foo", "bar")
        assert get(f"#{m_id}/@foo") == "bar"

        # GC does not work, let's help it.
        delete_rows("//sys/sequoia/resolve_node", [{"path": "//tmp/r/m"}])
