from yt_commands import (
    authors, get, insert_rows, select_rows, mount_table, reshard_table, sync_create_cells,
    sync_mount_table, sync_flush_table,
    create_table_backup, restore_table_backup, raises_yt_error)

from test_dynamic_tables import DynamicTablesBase

from yt.environment.helpers import assert_items_equal
import yt.yson as yson

##################################################################


@authors("ifsmirnov")
class TestBackups(DynamicTablesBase):
    def test_basic_backup(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)
        assert get("//tmp/t/@backup_state") == "none"

        create_table_backup(["//tmp/t", "//tmp/bak"])
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

        with raises_yt_error():
            restore_table_backup(["//tmp/bak", "//tmp/res"])

        sync_flush_table("//tmp/t")

        with raises_yt_error():
            mount_table("//tmp/bak")
        with raises_yt_error():
            reshard_table("//tmp/bak", [[], [1], [2]])

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@tablet_backup_state") == "none"
        assert get("//tmp/res/@backup_state") == "restored_with_restrictions"
        sync_mount_table("//tmp/res")
        assert_items_equal(select_rows("* from [//tmp/res]"), rows)


##################################################################

@authors("ifsmirnov")
class TestBackupsMulticell(TestBackups):
    NUM_SECONDARY_MASTER_CELLS = 2
