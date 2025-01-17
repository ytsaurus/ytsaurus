from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
    CHAOS_NODES_SERVICE,
)

from yt_commands import (
    align_chaos_cell_tag, generate_chaos_cell_id, map_reduce, master_exit_read_only, read_table, sync_create_chaos_cell, wait, init_drivers, wait_drivers,
    exists, get, set, ls, create, remove, create_account, create_domestic_medium, remove_account,
    start_transaction, abort_transaction, create_area, remove_area, create_rack, create_data_center, assert_true_for_all_cells,
    assert_true_for_secondary_cells, build_snapshot, get_driver, create_user, make_ace,
    create_access_control_object_namespace, create_access_control_object,
    print_debug, decommission_node, write_table)

from yt_helpers import master_exit_read_only_sync, wait_no_peers_in_read_only
from yt.test_helpers import assert_items_equal

import builtins
import pytest
import os
import shutil
import inspect

from copy import deepcopy


class MasterCellAdditionBase(YTEnvSetup):
    DEFER_SECONDARY_CELL_START = True
    DEFER_NODE_START = True
    DEFER_SCHEDULER_START = True
    DEFER_CONTROLLER_AGENT_START = True
    DEFER_CHAOS_NODE_START = True

    PRIMARY_CLUSTER_INDEX = 0

    REMOVE_LAST_MASTER_BEFORE_START = True

    @classmethod
    def setup_class(cls):
        super(MasterCellAdditionBase, cls).setup_class()
        remove_master_before_start = cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cls.PRIMARY_CLUSTER_INDEX)

        for cluster_index, env in enumerate([cls.Env] + cls.remote_envs):
            secondary_master_cells_to_start = cls.get_param("NUM_SECONDARY_MASTER_CELLS", cluster_index)
            if cluster_index == cls.PRIMARY_CLUSTER_INDEX and remove_master_before_start:
                secondary_master_cells_to_start -= 1

            assert secondary_master_cells_to_start >= 0

            # NB: The last secondary master cell on primary cluster is not started here.
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
        cls.proceed_master_config(config, cluster_index, cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def proceed_master_config(cls, config, cluster_index, remove_last_master):
        cls._collect_cell_ids_and_maybe_stash_last_cell(config, cluster_index, remove_last_master)
        cluster_connection = config["cluster_connection"]
        if len(cluster_connection["secondary_masters"]) == 3:
            # Prevent cluster nodes from finding out about the "new" cell via cell directory synchronizer.
            cls._collect_cell_ids_and_maybe_stash_last_cell(cluster_connection, cluster_index, remove_last_master)

    @classmethod
    def modify_scheduler_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_chaos_node_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_driver_config(cls, config):
        cls.proceed_driver_config(config, cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cls.PRIMARY_CLUSTER_INDEX))

    @classmethod
    def proceed_driver_config(cls, config, remove_last_master):
        if "secondary_masters" in config.keys() and config["cluster_name"] == "primary":
            # Prevent drivers from crashing after discovery of the "new" cell via cell directory synchronizer.
            cls._collect_cell_ids_and_maybe_stash_last_cell(config, cls.PRIMARY_CLUSTER_INDEX, remove_last_master)

    @classmethod
    def _collect_cell_ids_and_maybe_stash_last_cell(cls, config, cluster_index, remove_last_master):
        if cluster_index != cls.PRIMARY_CLUSTER_INDEX:
            return

        cls._collect_cell_ids(config)
        if remove_last_master:
            cls._remove_last_cell_from_config(config)

    @classmethod
    def _collect_cell_ids(cls, config):
        cls.CELL_IDS.add(config["primary_master"]["cell_id"])
        for secondary_master in config["secondary_masters"]:
            cls.CELL_IDS.add(secondary_master["cell_id"])

    @classmethod
    def _remove_last_cell_from_config(cls, config):
        # Stash the last cell's config and remove it for the time being.
        cls.STASHED_CELL_CONFIGS.append(deepcopy(config["secondary_masters"][2]))
        removed_cell_id = config["secondary_masters"][2]["cell_id"]
        del config["secondary_masters"][2]
        cls.PATCHED_CONFIGS.append(config)

        if removed_cell_id in cls.CELL_IDS:
            cls.CELL_IDS.remove(removed_cell_id)

        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

    @classmethod
    def _disable_last_cell(cls):
        optional_services = [
            SCHEDULERS_SERVICE if cls.NUM_SCHEDULERS != 0 else None,
            CONTROLLER_AGENTS_SERVICE if cls.NUM_CONTROLLER_AGENTS != 0 else None,
            NODES_SERVICE if cls.NUM_NODES != 0 else None,
            CHAOS_NODES_SERVICE if cls.NUM_CHAOS_NODES != 0 else None,
        ]
        services = [service for service in optional_services if service is not None]

        with Restarter(cls.Env, services):
            drivers = ["driver"]
            cls.Env.kill_service("driver")
            for i in range(cls.Env.yt_config.secondary_cell_count):
                cls.Env.kill_service(f"driver_secondary_{i}")
                drivers.append(f"driver_secondary_{i}")

            for cell_id in cls.CELL_IDS:
                build_snapshot(cell_id=cell_id, set_read_only=True)
            cls.Env.kill_all_masters()

            # Patch static configs for all components.
            configs = cls.Env._cluster_configuration
            for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
                for peer_index, _ in enumerate(configs["master"][tag]):
                    cls.proceed_master_config(configs["master"][tag][peer_index], cls.PRIMARY_CLUSTER_INDEX, remove_last_master=True)
                    configs["master"][tag][peer_index]["hive_manager"]["allowed_for_removal_master_cells"] = [13]
            for service in services:
                # Config keys are services in the singular, drop plural.
                service_name_singular = service[:-1]
                for _, config in enumerate(configs[service_name_singular]):
                    cls._collect_cell_ids_and_maybe_stash_last_cell(config["cluster_connection"], cls.PRIMARY_CLUSTER_INDEX, remove_last_master=True)
                    if service == NODES_SERVICE:
                        if "tablet_node" in config.keys():
                            config["tablet_node"]["hive_manager"]["allowed_for_removal_master_cells"] = [13]
                    elif service == CHAOS_NODES_SERVICE:
                        config["cellar_node"]["cellar_manager"]["cellars"]["chaos"]["occupant"]["hive_manager"]["allowed_for_removal_master_cells"] = [13]
            for driver in drivers:
                cls.proceed_driver_config(cls.Env.configs[driver], remove_last_master=True)

            init_drivers([cls.Env])
            cls.Env.rewrite_master_configs()
            cls.Env.rewrite_node_configs()
            cls.Env.rewrite_chaos_node_configs()
            cls.Env.rewrite_scheduler_configs()
            cls.Env.rewrite_controller_agent_configs()

            cls.Env.start_master_cell(cell_index=0, set_config=False)
            secondary_masters_to_start = cls.get_param("NUM_SECONDARY_MASTER_CELLS", cls.PRIMARY_CLUSTER_INDEX) - 1
            for cell_index in range(secondary_masters_to_start):
                cls.Env.start_master_cell(cell_index=cell_index + 1, set_config=False)
            wait_drivers()
            master_exit_read_only()
            # Cell 13 was dropped.
            wait_no_peers_in_read_only(secondary_cell_tags=["11", "12"])
            cls.Env.synchronize()

        def _move_files(directory):
            files = os.listdir(directory)
            for file in files:
                file_path = os.path.join(directory, file)
                shutil.move(file_path, f"{file_path}.bad")

        # Move changelogs and snapshots of 13 cell, to make it's directory clear when it will be added further as new cell.
        master_dirs = cls.Env._directories["master"][3]
        for master_dir in master_dirs:
            snapshots_path = os.path.join(master_dir, "snapshots")
            changelogs_path = os.path.join(master_dir, "changelogs")
            _move_files(snapshots_path)
            _move_files(changelogs_path)

    @classmethod
    def _enable_last_cell(cls, downtime):
        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

        optional_services = [
            SCHEDULERS_SERVICE if cls.NUM_SCHEDULERS != 0 else None,
            CONTROLLER_AGENTS_SERVICE if cls.NUM_CONTROLLER_AGENTS != 0 else None,
            NODES_SERVICE if cls.NUM_NODES != 0 else None,
            CHAOS_NODES_SERVICE if cls.NUM_CHAOS_NODES != 0 else None,
        ]
        services = [service for service in optional_services if downtime and service is not None]

        with Restarter(cls.Env, services):
            for cell_id in cls.CELL_IDS:
                build_snapshot(cell_id=cell_id, set_read_only=True)

            # Restart drivers to apply new master cells configuration.
            cls.Env.kill_service("driver")
            for i in range(cls.Env.yt_config.secondary_cell_count):
                cls.Env.kill_service(f"driver_secondary_{i}")

            with Restarter(cls.Env, MASTERS_SERVICE, sync=False):
                for i in range(len(cls.PATCHED_CONFIGS)):
                    cls.PATCHED_CONFIGS[i]["secondary_masters"].append(cls.STASHED_CELL_CONFIGS[i])
                init_drivers([cls.Env])

                cls.Env.rewrite_master_configs()
            wait_drivers()

            master_exit_read_only_sync()
            cls.Env.synchronize()

            if downtime:
                cls.Env.rewrite_node_configs()
                cls.Env.rewrite_chaos_node_configs()
                cls.Env.rewrite_scheduler_configs()
                cls.Env.rewrite_controller_agent_configs()

        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "World initialization" in title:
                abort_transaction(id)

    def _do_for_cell(self, cell_index, callback):
        return callback(get_driver(cell_index))

    def run_checkers_iteration(self, checker_state_list, final_iteration=False):
        for s in checker_state_list:
            if final_iteration:
                with pytest.raises(StopIteration):
                    next(s)
            else:
                next(s)

    def execute_checks_with_cell_addition(self, downtime, after_first_checkers_lambda=None):
        if not self.get_param("REMOVE_LAST_MASTER_BEFORE_START", self.PRIMARY_CLUSTER_INDEX):
            set("//sys/@config/multicell_manager/testing/allow_master_cell_removal", True)
            self._disable_last_cell()

        checker_names = [attr for attr in dir(self) if attr.startswith('check_') and inspect.ismethod(getattr(self, attr))]

        print_debug("Checkers: ", checker_names)

        checkers = [getattr(self, attr) for attr in checker_names]
        checker_state_list = [iter(c()) for c in checkers]

        self.run_checkers_iteration(checker_state_list)

        if after_first_checkers_lambda is not None:
            after_first_checkers_lambda()

        type(self)._enable_last_cell(downtime)

        self.run_checkers_iteration(checker_state_list, True)

    def _get_connected_to_node_secondary_masters(self, node):
        connected_secondary_masters = get(f"//sys/cluster_nodes/{node}/orchid/connected_secondary_masters", driver=get_driver(0))
        return sorted(connected_secondary_masters.keys())

    def _nodes_synchronized_with_masters(self, nodes):
        for node in nodes:
            if "13" not in self._get_connected_to_node_secondary_masters(node):
                return False
        return True


class MasterCellAdditionBaseChecks(MasterCellAdditionBase):
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
    }

    DELTA_DRIVER_CONFIG = {
        "cell_directory_synchronizer": {
            "sync_cells_with_secondary_masters": False,
        },
    }

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

        wait(lambda: len(ls("//sys/accounts/acc_sync_create/@multicell_statistics")) == self.NUM_SECONDARY_MASTER_CELLS + 1)

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
        def check(cell_tags):
            secondary_masters = get("//sys/secondary_masters")
            if sorted(secondary_masters.keys()) != sorted(cell_tags):
                return False

            for cell_tag in cell_tags:
                assert len(secondary_masters[cell_tag]) == 3

            return True

        if self.REMOVE_LAST_MASTER_BEFORE_START:
            assert check(["11", "12"])
        else:
            wait(lambda: check(["11", "12"]))

        yield

        wait(lambda: check(["11", "12", "13"]))

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

        # Cleanup just in case.
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

        decommission_node(host, "check cluster node hierarchy")
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
                assert_items_equal(found_location_uuids, location_uuids)

            assert exists("//sys/media/nvme_override", driver=driver)

            for location_uuid in overridden_location_uuids:
                assert get("//sys/chunk_locations/{}/@medium_override".format(location_uuid), driver=driver) == "nvme_override"

            return True

        check_everything()

        yield

        for i in range(self.Env.yt_config.secondary_cell_count):
            check_everything(get_driver(i))
        # In case of dynamic reconfiguration need to wait for nodes to recieve new membership
        # before nodes start sending information about chunks to the new cell.
        wait(lambda: self._nodes_synchronized_with_masters(nodes))
        check_everything(get_driver(self.Env.yt_config.secondary_cell_count))

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

    def check_nodes_configuration(self):
        def _check_nodes_state(nodes, driver=None):
            for node in nodes:
                assert get(f"//sys/cluster_nodes/{node}/@state", driver=driver) == "online"
            return True

        nodes = ls("//sys/cluster_nodes")
        _check_nodes_state(nodes)

        yield

        multicell_states = {tag : "online" for tag in ["10", "11", "12", "13"]}
        for node in nodes:
            assert get(f"//sys/cluster_nodes/{node}/@multicell_states") == multicell_states
        assert_true_for_all_cells(self.Env, lambda driver: _check_nodes_state(nodes, driver=driver))

    def check_map_reduce_avialibility(self):
        def _check_basic_map_reduce():
            data = [{"foo": i} for i in range(3)]
            create("table", "//tmp/in")
            write_table("//tmp/in", data)
            assert read_table("//tmp/in") == data
            create("table", "//tmp/out")

            map_reduce(
                mapper_command="cat",
                reducer_command="cat",
                in_="//tmp/in",
                out="//tmp/out",
                sort_by=["foo"]
            )

            remove("//tmp/in")
            remove("//tmp/out")

        _check_basic_map_reduce()

        yield

        _check_basic_map_reduce()

    def check_transactions(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
        tx = start_transaction(timeout=120000)
        create("table", "//tmp/p1/t", tx=tx)  # replicate tx to cell 12
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12]

        yield

        # Make the new master cell "reliable" for other master cells.
        set("//sys/@config/multicell_manager/testing/discovered_masters_cell_tags", [13])
        set("//sys/@config/multicell_manager/cell_descriptors", {"13": {"roles": ["cypress_node_host", "chunk_host"]}})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
        create("table", "//tmp/p2/t", tx=tx)  # replicate tx to cell 13
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12, 13]


class MasterCellAdditionChaosMultiClusterBaseChecks(MasterCellAdditionBase):
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 3
    NUM_CHAOS_NODES = 1

    NUM_REMOTE_CLUSTERS = 2
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 0
    NUM_SECONDARY_MASTER_CELLS_REMOTE_1 = 0

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
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
