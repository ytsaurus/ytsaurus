import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.environment.helpers import assert_items_equal

##################################################################

class TestOrderedTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 0

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000
        }
    }

    def _create_simple_table(self, path):
        create("table", path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "double"},
                    {"name": "c", "type": "string"}]
            })


    def test_mount(self):
        self.sync_create_cells(1, 1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert not "pivot_key" in tablet
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    def test_unmount(self):
        self.sync_create_cells(1, 1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        self.sync_mount_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_insert(self):
        self.sync_create_cells(1, 1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

    def test_flush(self):
        self.sync_create_cells(1, 1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@row_count") == 100

##################################################################

class TestOrderedTabletsMulticell(TestOrderedTablets):
    NUM_SECONDARY_MASTER_CELLS = 2
