from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
    CHAOS_NODES_SERVICE
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
    print_debug, decommission_node,
    sync_create_chaos_cell, generate_chaos_cell_id, align_chaos_cell_tag)

from yt_helpers import master_exit_read_only_sync
from yt.test_helpers import assert_items_equal

import pytest

from copy import deepcopy
import builtins
import inspect

################################################################################


class TestMasterCellAdditionBase(YTEnvSetup):
    DEFER_SECONDARY_CELL_START = True
    DEFER_NODE_START = True
    DEFER_SCHEDULER_START = True
    DEFER_CONTROLLER_AGENT_START = True
    DEFER_CHAOS_NODE_START = True

    PRIMARY_CLUSTER_INDEX = 0

    @classmethod
    def setup_class(cls):
        super(TestMasterCellAdditionBase, cls).setup_class()

        for cluster_index, env in enumerate([cls.Env] + cls.remote_envs):
            secondary_master_cells_to_start = cls.get_param("NUM_SECONDARY_MASTER_CELLS", cluster_index)
            if cluster_index == cls.PRIMARY_CLUSTER_INDEX:
                secondary_master_cells_to_start -= 1

            assert secondary_master_cells_to_start >= 0

            # NB: the last secondary cell on primary is not started here.
            for cell_index in range(secondary_master_cells_to_start):
                env.start_master_cell(cell_index + 1)

            if cls.get_param("NUM_NODES", cluster_index) != 0:
                env.start_nodes()
            if cls.get_param("NUM_SCHEDULERS", cluster_index) != 0:
                env.start_schedulers()
            if cls.get_param("NUM_CONTROLLER_AGENTS", cluster_index) != 0:
                env.start_controller_agents()
            if cls.get_param("NUM_CHAOS_NODES", cluster_index) != 0:
                env.start_chaos_nodes()

    @classmethod
    def modify_master_config(cls, config, tag, peer_index, cluster_index):
        cls._disable_last_cell_and_stash_config(config, cluster_index)
        cluster_connection = config["cluster_connection"]
        if len(cluster_connection["secondary_masters"]) == 3:
            # Prevent cluster nodes from finding out about the "new" cell via cell directory synchronizer.
            cls._disable_last_cell_and_stash_config(cluster_connection, cluster_index)

    @classmethod
    def modify_scheduler_config(cls, config, cluster_index):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"], cluster_index)

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"], cluster_index)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"], cluster_index)

    @classmethod
    def modify_chaos_node_config(cls, config, cluster_index):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"], cluster_index)

    @classmethod
    def _disable_last_cell_and_stash_config(cls, config, cluster_index):
        if cluster_index != cls.PRIMARY_CLUSTER_INDEX:
            return

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

        optional_services = [
            SCHEDULERS_SERVICE if cls.NUM_SCHEDULERS != 0 else None,
            CONTROLLER_AGENTS_SERVICE if cls.NUM_CONTROLLER_AGENTS != 0 else None,
            NODES_SERVICE if cls.NUM_NODES != 0 else None,
            CHAOS_NODES_SERVICE if cls.NUM_CHAOS_NODES != 0 else None,
        ]
        services = [service for service in optional_services if service is not None]

        with Restarter(cls.Env, services):
            for cell_id in cls.CELL_IDS:
                build_snapshot(cell_id=cell_id, set_read_only=True)

            with Restarter(cls.Env, MASTERS_SERVICE, sync=False):
                for i in range(len(cls.PATCHED_CONFIGS)):
                    cls.PATCHED_CONFIGS[i]["secondary_masters"].append(cls.STASHED_CELL_CONFIGS[i])

                cls.Env.rewrite_master_configs()

            master_exit_read_only_sync()
            cls.Env.synchronize()

            cls.Env.rewrite_node_configs()
            cls.Env.rewrite_scheduler_configs()
            cls.Env.rewrite_controller_agent_configs()
            cls.Env.rewrite_chaos_node_configs()

        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "World initialization" in title:
                abort_transaction(id)

    def _do_for_cell(self, cell_index, callback):
        return callback(get_driver(cell_index))

    def _execute_checks_with_cell_addition(self):
        checker_names = [attr for attr in dir(self) if attr.startswith('check_') and inspect.ismethod(getattr(self, attr))]

        print_debug("Checkers: ", checker_names)

        checkers = [getattr(self, attr) for attr in checker_names]
        checker_state_list = [iter(c()) for c in checkers]
        for s in checker_state_list:
            next(s)

        type(self)._enable_last_cell()

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


class TestMasterCellAddition(TestMasterCellAdditionBase):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3

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
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

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
        self._execute_checks_with_cell_addition()


class TestMasterCellAdditionChaosMultiCluster(TestMasterCellAdditionBase):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 3
    NUM_CHAOS_NODES = 1

    NUM_REMOTE_CLUSTERS = 2
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 0
    NUM_SECONDARY_MASTER_CELLS_REMOTE_1 = 0

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

    # Creates bundle in production-like configuration.
    # Cf. YT-20134
    def _create_chaos_bundle(self, chaos_bundle_name):
        metadata_clusters = ["remote_1", "primary"]
        peers = ["remote_0"]
        areas = {
            "beta": {
                "metadata_clusters": ["remote_0", "primary"],
                "peers": ["remote_1"]
            }
        }

        options = {
            "changelog_account": "sys",
            "snapshot_account": "sys",
            "clock_cluster_tag": get("//sys/@primary_cell_tag"),
            "independent_peers": False,
            "peer_count": len(peers)
        }

        clusters = builtins.set(metadata_clusters) | builtins.set(peers)
        assert clusters == {"primary", "remote_0", "remote_1"}

        for cluster in clusters:
            attributes = {
                "name": chaos_bundle_name,
                "options": options,
                "chaos_options": {
                    "peers": [{} if peer == cluster else {"alien_cluster": peer} for peer in peers],
                }
            }

            bundle_path = f"//sys/chaos_cell_bundles/{chaos_bundle_name}"
            driver = get_driver(cluster=cluster)

            assert not exists(bundle_path, driver=driver)
            create("chaos_cell_bundle", path="", attributes=attributes, driver=driver)

            cell_bundle_id = get(f"{bundle_path}/@id", driver=driver)
            for area in areas:
                area_info = areas[area]
                attributes = {
                    "name": area,
                    "cell_bundle_id": cell_bundle_id,
                    "chaos_options": {
                        "peers": [{} if peer == cluster else {"alien_cluster": peer} for peer in area_info["peers"]],
                    }
                }
                create("area", path="", attributes=attributes, driver=driver)

        align_chaos_cell_tag()
        cell_id = generate_chaos_cell_id()
        beta_cell_id = generate_chaos_cell_id()

        sync_create_chaos_cell(
            chaos_bundle_name,
            cell_id,
            ["remote_0"],
            meta_cluster_names=["primary", "remote_1"],
            area="default",
        )
        sync_create_chaos_cell(
            chaos_bundle_name,
            beta_cell_id,
            ["remote_1"],
            meta_cluster_names=["primary", "remote_0"],
            area="beta",
        )
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_ids", [cell_id, beta_cell_id])

        return cell_id, beta_cell_id

    def check_chaos_cells(self):
        chaos_bundle_name = "chaos_bundle"
        cell_id, beta_cell_id = self._create_chaos_bundle(chaos_bundle_name)
        bundle_path = f"//sys/chaos_cell_bundles/{chaos_bundle_name}"

        area_id = get(f"//sys/chaos_cells/{cell_id}/@area_id")
        beta_area_id = get(f"//sys/chaos_cells/{beta_cell_id}/@area_id")

        bundle_chaos_options = {"peers": [{"alien_cluster": "remote_0"}]}
        beta_chaos_options = {"peers": [{"alien_cluster": "remote_1"}]}

        def _check(driver=None):
            assert get(bundle_path + "/@chaos_options", driver=driver) == bundle_chaos_options
            assert not exists(f"#{area_id}" + "/@chaos_options", driver=driver)
            assert get(f"#{beta_area_id}" + "/@chaos_options", driver=driver) == beta_chaos_options
            assert get(f"#{cell_id}/@area_id", driver=driver) == area_id
            assert get(f"#{cell_id}/@area", driver=driver) == "default"
            assert get(f"#{beta_cell_id}/@area_id", driver=driver) == beta_area_id
            assert get(f"#{beta_cell_id}/@area", driver=driver) == "beta"
            return True

        _check()

        yield

        assert_true_for_all_cells(self.Env, _check)

    @authors("ponasenko-rs")
    @pytest.mark.timeout(300)
    def test_add_new_cell(self):
        set("//sys/@config/chaos_manager/alien_cell_synchronizer", {
            "enable": True,
            "sync_period": 100,
            "full_sync_period": 200,
        })

        self._execute_checks_with_cell_addition()
