from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, exists, copy, move, select_rows,
    remove, wait, start_transaction,
)

from yt.common import YtError

import pytest

##################################################################


class TestGrafting(YTEnvSetup):
    USE_SEQUOIA = True

    NUM_SECONDARY_MASTER_CELLS = 3

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
        scion_id = get("//tmp/r/@scion_id")

        # TODO: Use paths here when resolver will be ready
        assert get(f"#{rootstock_id}/@type") == "rootstock"
        assert get(f"#{rootstock_id}/@scion_id") == scion_id
        assert get(f"#{scion_id}/@type") == "scion"
        assert get(f"#{scion_id}/@rootstock_id") == rootstock_id

        assert exists(f"//sys/rootstocks/{rootstock_id}")
        assert exists(f"//sys/scions/{scion_id}")

        assert select_rows("* from [//sys/sequoia/resolve_node]") == [{
            "path": "//tmp/r",
            "node_id": scion_id,
        }]

        if remove_first == "rootstock":
            remove(f"#{rootstock_id}")
        else:
            remove(f"#{scion_id}")

        wait(lambda: not exists("//tmp/r"))
        wait(lambda: not exists(f"#{rootstock_id}"))
        wait(lambda: not exists(f"#{scion_id}"))

        wait(lambda: select_rows("* from [//sys/sequoia/resolve_node]") == [])

    @authors("gritukan")
    def test_cannot_create_rootstock_in_transaction(self):
        tx = start_transaction()
        with pytest.raises(YtError):
            create("rootstock", "//tmp/p", attributes={"scion_cell_tag": 11}, tx=tx)

    @authors("gritukan")
    def test_cannot_copy_move_rootstock(self):
        create("rootstock", "//tmp/r", attributes={"scion_cell_tag": 11})
        with pytest.raises(YtError):
            copy("//tmp/r", "//tmp/r2")
        with pytest.raises(YtError):
            move("//tmp/r", "//tmp/r2")
