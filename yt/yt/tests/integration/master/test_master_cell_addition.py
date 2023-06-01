from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
)

from yt_commands import (
    authors, wait,
    exists, get, set, ls, create, remove,
    create_account, create_domestic_medium, remove_account,
    start_transaction, abort_transaction,
    create_area, remove_area,
    create_rack, create_data_center,
    assert_true_for_all_cells,
    assert_true_for_secondary_cells,
    build_snapshot, get_driver,
    create_user, make_ace,
    create_access_control_object_namespace, create_access_control_object,
    print_debug, decommission_node)

from yt.test_helpers import assert_items_equal

import pytest

from copy import deepcopy
import builtins
import inspect

################################################################################


class TestMasterCellAddition(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 3
    DEFER_SECONDARY_CELL_START = True

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000
        }
    }

    DELTA_DRIVER_CONFIG = {
        "cell_directory_synchronizer": {
            "sync_cells_with_secondary_masters": False,
        },
    }

    NUM_NODES = 3
    DEFER_NODE_START = True

    NUM_SCHEDULERS = 1
    DEFER_SCHEDULER_START = True

    DEFER_CONTROLLER_AGENT_START = True
    NUM_CONTROLLER_AGENTS = 1

    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    @classmethod
    def setup_class(cls):
        super(TestMasterCellAddition, cls).setup_class()
        # NB: the last secondary cell is not started here.
        for cell_index in range(1, cls.NUM_SECONDARY_MASTER_CELLS):
            cls.Env.start_master_cell(cell_index)

        cls.Env.start_nodes()
        cls.Env.start_schedulers()
        cls.Env.start_controller_agents()

    @classmethod
    def _disable_last_cell_and_stash_config(cls, config):
        # Stash the last cell's config and remove it for the time being.
        cls.STASHED_CELL_CONFIGS.append(deepcopy(config["secondary_masters"][2]))
        del config["secondary_masters"][2]
        cls.PATCHED_CONFIGS.append(config)

        cls.CELL_IDS.add(config["primary_master"]["cell_id"])
        for secondary_master in config["secondary_masters"]:
            cls.CELL_IDS.add(secondary_master["cell_id"])

        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

    @classmethod
    def _enable_last_cell(cls):
        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

        with Restarter(cls.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE]):
            for cell_id in cls.CELL_IDS:
                build_snapshot(cell_id=cell_id, set_read_only=True)

            with Restarter(cls.Env, MASTERS_SERVICE):
                for i in range(len(cls.PATCHED_CONFIGS)):
                    cls.PATCHED_CONFIGS[i]["secondary_masters"].append(cls.STASHED_CELL_CONFIGS[i])

                cls.Env.rewrite_master_configs()

            cls.Env.rewrite_node_configs()
            cls.Env.rewrite_scheduler_configs()
            cls.Env.rewrite_controller_agent_configs()

        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "World initialization" in title:
                abort_transaction(id)

    @classmethod
    def modify_master_config(cls, config, tag, index):
        cls._disable_last_cell_and_stash_config(config)

    @classmethod
    def modify_scheduler_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    @classmethod
    def modify_controller_agent_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    @classmethod
    def modify_node_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    def _do_for_cell(self, cell_index, callback):
        return callback(get_driver(cell_index))

    def check_media(self):
        create_domestic_medium("ssd")
        create_account("a")
        set("//sys/accounts/a/@resource_limits/disk_space_per_medium/ssd", 42)

        yield

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: exists("//sys/media/ssd", driver=driver))
        assert exists("//sys/media/ssd")

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/accounts/a/@resource_limits/disk_space_per_medium/ssd", driver=driver)) == 42
        assert get("//sys/accounts/a/@resource_limits/disk_space_per_medium/ssd") == 42

    def check_accounts(self):
        create_account("acc_sync_create")

        create_account("acc_async_remove")
        create(
            "table",
            "//tmp/t",
            attributes={"account": "acc_async_remove", "external_cell_tag": 11},
        )

        create_account("acc_sync_remove")
        remove_account("acc_sync_remove")

        create_account("acc_async_create", sync=False)
        remove_account("acc_async_remove", sync=False)

        yield

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: not exists("//sys/accounts/acc_sync_remove", driver=driver),
        )
        assert not exists("//sys/accounts/acc_sync_remove")

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/accounts/acc_sync_create/@life_stage", driver=driver) == "creation_committed",
        )
        assert get("//sys/accounts/acc_sync_create/@life_stage") == "creation_committed"

        wait(lambda: get("//sys/accounts/acc_async_create/@life_stage") == "creation_committed")
        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/accounts/acc_async_create/@life_stage", driver=driver) == "creation_committed",
        )

        assert get("//sys/accounts/acc_async_remove/@life_stage") == "removal_started"
        wait(
            lambda: self._do_for_cell(
                1,
                lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver),
            )
            == "removal_started"
        )
        wait(
            lambda: self._do_for_cell(
                2,
                lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver),
            )
            == "removal_pre_committed"
        )
        wait(
            lambda: self._do_for_cell(
                3,
                lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver),
            )
            == "removal_pre_committed"
        )

        remove("//tmp/t")

        wait(lambda: not exists("//sys/accounts/acc_async_remove"))
        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: not exists("//sys/accounts/acc_async_remove", driver=driver),
        )

    def check_sys_masters_node(self):
        def check(cell_ids):
            secondary_masters = get("//sys/secondary_masters")
            if sorted(secondary_masters.keys()) != sorted(cell_ids):
                return False

            for cell_id in cell_ids:
                assert len(secondary_masters[cell_id]) == 3

            return True

        assert check(["11", "12"])

        yield

        wait(lambda: check(["11", "12", "13"]))

    def check_transactions(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
        tx = start_transaction(timeout=120000)
        create("table", "//tmp/p1/t", tx=tx)  # replicate tx to cell 12
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12]

        yield

        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
        create("table", "//tmp/p2/t", tx=tx)  # replicate tx to cell 13
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12, 13]

    def check_areas(self):
        default_bundle_id = get("//sys/tablet_cell_bundles/default/@id")
        default_area_id = get("//sys/tablet_cell_bundles/default/@areas/default/id")
        set("#{0}/@node_tag_filter".format(default_area_id), "default")
        custom_area_id = create_area("custom", cell_bundle_id=default_bundle_id, node_tag_filter="custom")

        yield

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/tablet_cell_bundles/default/@areas/default/node_tag_filter", driver=driver) == "default",
        )

        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/tablet_cell_bundles/default/@areas/custom/node_tag_filter", driver=driver) == "custom",
        )

        # cleanup just in case
        remove_area(custom_area_id)
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "")

    def check_builtin_object_attributes(self):
        master_memory = 10**9 + 123
        set("//sys/accounts/sys/@resource_limits/master_memory/total", master_memory)
        set("//sys/accounts/sys/@foo", "bar")

        yield

        assert get("//sys/accounts/sys/@resource_limits/master_memory/total") == master_memory
        assert get("//sys/accounts/sys/@foo") == "bar"
        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/accounts/sys/@resource_limits/master_memory/total", driver=driver) == master_memory,
        )
        assert_true_for_secondary_cells(
            self.Env,
            lambda driver: get("//sys/accounts/sys/@foo", driver=driver) == "bar",
        )

    def check_cluster_node_hierarchy(self):
        host = node = ls("//sys/cluster_nodes")[0]
        create_rack("r")
        create_data_center("d")

        set("//sys/hosts/{}/@rack".format(host), "r")
        set("//sys/racks/r/@data_center", "d")

        decommission_node(host, "check clster node hierarchy")
        set("//sys/racks/r/@foo", "oof")
        set("//sys/data_centers/d/@bar", "rab")

        def check_everything(driver=None):
            assert get("//sys/cluster_nodes/{}/@host".format(node), driver=driver) == host
            assert get("//sys/hosts/{}/@nodes".format(host), driver=driver) == [node]
            assert get("//sys/hosts/{}/@rack".format(host), driver=driver) == "r"
            assert get("//sys/racks/r/@hosts", driver=driver) == [host]
            assert get("//sys/racks/r/@data_center", driver=driver) == "d"
            assert get("//sys/data_centers/d/@racks", driver=driver) == ["r"]

            assert get("//sys/cluster_nodes/{}/@decommissioned".format(host), driver=driver)
            assert get("//sys/racks/r/@foo", driver=driver) == "oof"
            assert get("//sys/data_centers/d/@bar", driver=driver) == "rab"

            return True

        check_everything()

        yield

        assert_true_for_all_cells(self.Env, lambda driver: check_everything(driver))

    def check_chunk_locations(self):
        node_to_location_uuids = {}

        nodes = ls("//sys/cluster_nodes", attributes=["chunk_locations"])
        for node in nodes:
            node_address = str(node)
            location_uuids = node.attributes["chunk_locations"].keys()
            node_to_location_uuids[node_address] = location_uuids

        create_domestic_medium("nvme_override")
        overridden_node_address = str(nodes[0])
        overridden_location_uuids = node_to_location_uuids[overridden_node_address]
        for location_uuid in overridden_location_uuids:
            set("//sys/chunk_locations/{}/@medium_override".format(location_uuid), "nvme_override")

        def check_everything(driver=None):
            for node_address, location_uuids in node_to_location_uuids.items():
                assert exists("//sys/cluster_nodes/{}".format(node_address), driver=driver)
                found_location_uuids = get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address), driver=driver).keys()
                assert_items_equal(location_uuids, found_location_uuids)

            assert exists("//sys/media/nvme_override", driver=driver)

            for location_uuid in overridden_location_uuids:
                assert get("//sys/chunk_locations/{}/@medium_override".format(location_uuid), driver=driver) == "nvme_override"

            return True

        check_everything()

        yield

        assert_true_for_all_cells(self.Env, lambda driver: check_everything(driver))

    def check_access_control_objects(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("tom", "cats")
        create_access_control_object("garfield", "cats")
        create_user("tom")
        acl = [make_ace("allow", "tom", "read")]
        set("//sys/access_control_object_namespaces/cats/garfield/principal/@acl", acl)
        remove("//sys/access_control_object_namespaces/cats/tom/principal/@owner")

        yield

        def check(driver=None):
            assert exists("//sys/access_control_object_namespaces/cats/tom", driver=driver)
            assert exists("//sys/access_control_object_namespaces/cats/garfield", driver=driver)
            assert get("//sys/access_control_object_namespaces/cats/garfield/principal/@acl", driver=driver) == acl
            assert not exists("//sys/access_control_object_namespaces/cats/tom/principal/@owner", driver=driver)
            return True

        assert_true_for_all_cells(self.Env, check)

    @authors("shakurov")
    def test_add_new_cell(self):
        checker_names = [attr for attr in dir(self) if attr.startswith('check_') and inspect.ismethod(getattr(self, attr))]

        print_debug("Checkers: ", checker_names)

        checkers = [getattr(self, attr) for attr in checker_names]
        checker_state_list = [iter(c()) for c in checkers]
        for s in checker_state_list:
            next(s)

        TestMasterCellAddition._enable_last_cell()

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
