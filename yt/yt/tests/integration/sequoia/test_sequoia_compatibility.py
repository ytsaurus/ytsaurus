from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, alter_table, sync_mount_table, remove,
    sync_unmount_table, ls, set, wait, raises_yt_error
)

from yt_sequoia_helpers import get_ground_driver

import pytest


##################################################################


@pytest.mark.enabled_multidaemon
class TestSequoiaCompatibility(YTEnvSetup):
    ENABLE_MULTIDAEMON = True

    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = True
    NUM_CYPRESS_PROXIES = 1

    # To flush tablet store.
    NUM_NODES_GROUND = 3

    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        # Master cell with tag 11 is reserved for portals.
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["sequoia_node_host"]},
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "logging": {
            "abort_on_alert": False,  # Incompatible schemas of Sequoia table.
        }
    }

    # To observe table schema change.
    ENABLE_GROUND_TABLE_MOUNT_CACHE = False

    @authors("kvk1920")
    @pytest.mark.parametrize("bypass_master_resolve", [False, True])
    def test_sequoia_table_schema_validation(self, bypass_master_resolve):
        CONFIG = {
            "object_service": {
                "allow_bypass_master_resolve": bypass_master_resolve
            }
        }
        set("//sys/cypress_proxies/@config", CONFIG)

        def check_config():
            for proxy in ls("//sys/cypress_proxies"):
                if get(f"//sys/cypress_proxies/{proxy}/orchid/dynamic_config_manager/applied_config") != CONFIG:
                    return False
            return True

        wait(check_config)

        TABLE_PATH = "//sys/sequoia/child_nodes"
        TABLE_NAME = "child_nodes"

        ground_driver = get_ground_driver()
        schema = get(TABLE_PATH + "/@schema", driver=ground_driver)
        new_schema = schema + [{"name": "invalid_column", "type": "string"}]

        try:
            sync_unmount_table(TABLE_PATH, driver=ground_driver)
            alter_table(TABLE_PATH, schema=new_schema, driver=ground_driver)
            sync_mount_table(TABLE_PATH, driver=ground_driver)

            with raises_yt_error(f"Sequoia table \"{TABLE_NAME}\" has unexpected schema"):
                node_id = create("int64_node", "//tmp/some_node")
                get(f"#{node_id}/@path") == "//tmp/some_node"

            # Cypress still works.
            assert len(ls("//sys")) > 0
        finally:
            sync_unmount_table(TABLE_PATH, driver=ground_driver)
            remove(TABLE_PATH, driver=ground_driver)
            create(
                "table",
                TABLE_PATH,
                attributes={
                    "dynamic": True,
                    "schema": schema,
                    "tablet_cell_bundle": "sequoia-cypress",
                    "account": "sequoia",
                    "in_memory_mode": "uncompressed",
                },
                driver=ground_driver)
            sync_mount_table(TABLE_PATH, driver=ground_driver)

        node_id = create("int64_node", "//tmp/some_node")
        assert get(f"#{node_id}/@path") == "//tmp/some_node"
