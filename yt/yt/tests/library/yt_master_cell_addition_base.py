from yt.common import YtError
from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
    CHAOS_NODES_SERVICE,
    CYPRESS_PROXIES_SERVICE,
    RPC_PROXIES_SERVICE,
    HTTP_PROXIES_SERVICE,
)

from yt_commands import (
    align_chaos_cell_tag, generate_chaos_cell_id, map_reduce, master_exit_read_only, raises_yt_error, read_table, remote_copy, sync_create_chaos_cell, wait, init_drivers, wait_drivers,
    exists, get, set, ls, create, remove, create_account, create_domestic_medium, remove_account,
    start_transaction, abort_transaction, create_area, remove_area, create_rack, create_data_center, assert_true_for_all_cells,
    assert_true_for_secondary_cells, build_snapshot, get_driver, create_user, make_ace,
    create_access_control_object_namespace, create_access_control_object,
    print_debug, decommission_node, write_table, add_maintenance, remove_maintenance, get_singular_chunk_id,
    reset_dynamically_propagated_master_cells)

from yt_helpers import master_exit_read_only_sync, wait_no_peers_in_read_only
from yt.test_helpers import assert_items_equal

import builtins
import pytest
import os
import shutil
import inspect

from copy import deepcopy


class MasterCellAdditionBase(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 3

    DEFER_SECONDARY_CELL_START = True
    DEFER_NODE_START = True
    DEFER_SCHEDULER_START = True
    DEFER_CONTROLLER_AGENT_START = True
    DEFER_CHAOS_NODE_START = True
    # NB: It is impossible to defer start cypress proxies, since some setup handlers rely on their availability.

    PRIMARY_CLUSTER_INDEX = 0

    REMOVE_LAST_MASTER_BEFORE_START = True
    NUM_SECONDARY_MASTER_CELLS = 3

    CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE = {
        "master_cell_directory_synchronizer": {
            "sync_period": 10000,  # 10 sec
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "duplicate_directory_update": True,
        },
    }

    # NB: Patch cluster connection for all possibly known services.
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_CHAOS_NODE_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_SCHEDULER_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_HTTP_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_KAFKA_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_MASTER_CACHE_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_RPC_DRIVER_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_TABLET_BALANCER_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_HTTP_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_NODE_CONFIG = {
        "delay_master_cell_directory_start": True,
        # NB: In real clusters this flag is disabled.
        "data_node": {
            "sync_directories_on_connect": False,
        },
        "sync_directories_on_connect": False,
        "cluster_connection": CLUSTER_CONNECTION_WITH_MASTER_CELL_DIRECTORY_OVERRIDE,
    }

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 500,
            "init_retry_period": 500,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "master_cell_directory_synchronizer": {
            "sync_period": 10000,  # 10 sec
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "duplicate_directory_update": True,
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["chunk_host", "cypress_node_host"]},
        "13": {"roles": []},
    }

    # NB: without this maintenance flag writes are prohibited and replicating
    # flags like "banned" and "decomission" during node replication on cell
    # addition doesn't happen which differs from behavior on most of production
    # clusters.
    TEST_MAINTENANCE_FLAGS = True

    def setup_class(cls):
        super(MasterCellAdditionBase, cls).setup_class()
        remove_master_before_start = cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cls.PRIMARY_CLUSTER_INDEX)

        for cluster_index, env in enumerate([cls.Env] + cls.remote_envs):
            secondary_master_cells_to_start = cls.get_param("NUM_SECONDARY_MASTER_CELLS", cluster_index)
            if cluster_index == cls.PRIMARY_CLUSTER_INDEX and remove_master_before_start:
                secondary_master_cells_to_start -= 1

            assert secondary_master_cells_to_start >= 0

            # Cypress proxies must be available to execute sync requests, so they should be started before masters.
            env.kill_cypress_proxies()
            if cls.get_param("NUM_CYPRESS_PROXIES", cluster_index) != 0:
                env.start_cypress_proxies()

            # NB: The last secondary master cell on primary cluster is not started here.
            # It is important to start cells synchroniously, so the last registered one - is the one we will add/remove.
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

    def _reset_dynamically_propagated_master_cells(self):
        def check_reliability_status(node):
            if get(f"//sys/cluster_nodes/{node}/@state") == "offline":
                return True

            reliabilities = get(f"//sys/cluster_nodes/{node}/@master_cells_reliabilities")
            for master in reliabilities.keys():
                if reliabilities[master] == "during_propagation":
                    return False
            return True

        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            wait(lambda: check_reliability_status(node))

        reset_dynamically_propagated_master_cells()

    def teardown_method(self, method):
        self._reset_dynamically_propagated_master_cells()
        super(MasterCellAdditionBase, self).teardown_method(method)

    @classmethod
    def do_with_retries(self, action):
        try:
            action()
        except YtError as error:
            if error.contains_text("Unknown master cell tag") or error.contains_text("No channel for cell with id"):
                return False
            else:
                raise
        return True

    @classmethod
    def modify_master_config(cls, config, multidaemon_config, cell_index, cell_tag, peer_index, cluster_index):
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
    def modify_cypress_proxy_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_rpc_proxy_config(cls, config, cluster_index, multidaemon_config, proxy_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @classmethod
    def modify_http_proxy_config(cls, config, cluster_index, multidaemon_config, proxy_index):
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
    def modify_cluster_connection_config(cls, config, cluster_index):
        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config,
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
    def _get_optional_services(cls):
        return [
            SCHEDULERS_SERVICE if cls.NUM_SCHEDULERS != 0 else None,
            CONTROLLER_AGENTS_SERVICE if cls.NUM_CONTROLLER_AGENTS != 0 else None,
            NODES_SERVICE if cls.NUM_NODES != 0 else None,
            CHAOS_NODES_SERVICE if cls.NUM_CHAOS_NODES != 0 else None,
            RPC_PROXIES_SERVICE if cls.NUM_RPC_PROXIES != 0 and cls.DRIVER_BACKEND != "native" else None,
            HTTP_PROXIES_SERVICE if cls.NUM_HTTP_PROXIES != 0 and cls.DRIVER_BACKEND != "native" else None,
        ]

    @classmethod
    def _rewrite_optional_services_configs(cls):
        cls.Env.rewrite_node_configs()
        cls.Env.rewrite_chaos_node_configs()
        cls.Env.rewrite_scheduler_configs()
        cls.Env.rewrite_controller_agent_configs()
        cls.Env.rewrite_cypress_proxies_configs()

    @classmethod
    def _abort_world_initializer_transactions(cls):
        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            if "World initialization" in title:
                # Proxy need time to discover new master cell, if it already started world initializer transaction.
                wait(lambda: cls.do_with_retries(lambda: abort_transaction(str(tx))))

    @classmethod
    def _update_cluster_connection(cls):
        cluster_connection_config = cls.Env.get_cluster_configuration()["cluster_connection"]
        set("//sys/@cluster_connection", cluster_connection_config)

    @classmethod
    def _build_master_snapshots(cls, set_read_only):
        # No build_master_snapshots in rpc proxies, may be do something with it.
        for cell_id in cls.CELL_IDS:
            build_snapshot(cell_id=cell_id, set_read_only=set_read_only)

    @classmethod
    def _master_exit_readonly(cls):
        def exit_readonly():
            try:
                master_exit_read_only_sync()
            except YtError as error:
                if error.is_rpc_unavailable() or error.contains_text("Unknown cell"):
                    return False
                else:
                    raise
            return True
        wait(exit_readonly)

    @classmethod
    def _kill_drivers(cls):
        drivers = ["driver"]
        cls.Env.kill_service("driver")
        for i in range(cls.Env.yt_config.secondary_cell_count):
            cls.Env.kill_service(f"driver_secondary_{i}")
            drivers.append(f"driver_secondary_{i}")
        return drivers

    @classmethod
    def _wait_for_nodes_state(cls, expected_state, aggregate_state):
        def check_multicell_states():
            nodes = ls("//sys/cluster_nodes", attributes=["multicell_states", "banned"])
            return all(
                all(
                    "offline" if node.attributes["banned"] else v == expected_state
                    for v in node.attributes["multicell_states"].values()
                )
                for node in nodes if not node.attributes["banned"]
            )

        def check_aggregated():
            nodes = ls("//sys/cluster_nodes", attributes=["state", "banned"])

            def check_node(node):
                if node.attributes["banned"]:
                    return node.attributes["state"] == "offline"
                return node.attributes["state"] == expected_state

            return all(map(check_node, nodes))

        wait(
            check_aggregated if aggregate_state else check_multicell_states,
            ignore_exceptions=True,
            error_message=f"Nodes were not {expected_state} for 30 seconds",
            timeout=30)

    @classmethod
    def _disable_last_cell(cls):
        print_debug("Disabling last master cell")

        optional_services = cls._get_optional_services()
        services = [service for service in optional_services if service is not None]
        services_to_patch = services + [CYPRESS_PROXIES_SERVICE]

        with Restarter(cls.Env, services, sync=False):
            cls.Env.kill_cypress_proxies()
            drivers = cls._kill_drivers()

            cls._build_master_snapshots(set_read_only=True)

            cls.Env.kill_all_masters()

            # Patch static configs for all components.
            configs = cls.Env.get_cluster_configuration()
            for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
                for peer_index, _ in enumerate(configs["master"][tag]):
                    cls.proceed_master_config(configs["master"][tag][peer_index], cls.PRIMARY_CLUSTER_INDEX, remove_last_master=True)
                    configs["master"][tag][peer_index]["hive_manager"]["allowed_for_removal_master_cell_tags"] = [13]
            for service in services_to_patch:
                # Config keys are services in the singular, drop plural.
                service_name_singular = service[:-1]
                if service == CYPRESS_PROXIES_SERVICE:
                    # This one is an exception.
                    service_name_singular = "cypress_proxy"
                for _, config in enumerate(configs[service_name_singular]):
                    cls._collect_cell_ids_and_maybe_stash_last_cell(config["cluster_connection"], cls.PRIMARY_CLUSTER_INDEX, remove_last_master=True)
                    if service == NODES_SERVICE:
                        if "tablet_node" in config.keys():
                            config["tablet_node"]["hive_manager"]["allowed_for_removal_master_cell_tags"] = [13]
                    elif service == CHAOS_NODES_SERVICE:
                        config["cellar_node"]["cellar_manager"]["cellars"]["chaos"]["occupant"]["hive_manager"]["allowed_for_removal_master_cell_tags"] = [13]
            for driver in drivers:
                cls.proceed_driver_config(cls.Env.configs[driver], remove_last_master=True)

            init_drivers([cls.Env])
            cls.Env.rewrite_master_configs()
            cls._rewrite_optional_services_configs()

            cls.Env.start_cypress_proxies()
            cls.Env.start_master_cell(cell_index=0, set_config=False, sync=False)
            secondary_masters_to_start = cls.get_param("NUM_SECONDARY_MASTER_CELLS", cls.PRIMARY_CLUSTER_INDEX) - 1
            for cell_index in range(secondary_masters_to_start):
                cls.Env.start_master_cell(cell_index=cell_index + 1, set_config=False, sync=False)
            master_exit_read_only()
            # Cell 13 was dropped.
            wait_no_peers_in_read_only(secondary_cell_tags=["11", "12"])
            wait_drivers()

        cls.Env.synchronize()
        cls._abort_world_initializer_transactions()
        cls._wait_for_nodes_state("online", aggregate_state=True)

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
    def _enable_last_cell(cls, downtime, wait_for_nodes=True):
        print_debug("Enabling last master cell")

        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

        optional_services = cls._get_optional_services()
        services = [service for service in optional_services if downtime and service is not None]

        with Restarter(cls.Env, services, sync=False):
            # Restart drivers to apply new master cells configuration.
            cls._kill_drivers()

            cls._build_master_snapshots(set_read_only=True)

            with Restarter(cls.Env, MASTERS_SERVICE, sync=False):
                for i in range(len(cls.PATCHED_CONFIGS)):
                    cls.PATCHED_CONFIGS[i]["secondary_masters"].append(cls.STASHED_CELL_CONFIGS[i])
                init_drivers([cls.Env])

                cls.Env.rewrite_master_configs()
            wait_drivers()
            if downtime:
                cls._rewrite_optional_services_configs()

            cls._master_exit_readonly()
        cls.Env.synchronize()

        cls._abort_world_initializer_transactions()
        if wait_for_nodes:
            cls._wait_for_nodes_state("online", aggregate_state=False)
        cls._update_cluster_connection()

        cls.PATCHED_CONFIGS = []
        cls.STASHED_CELL_CONFIGS = []

    def _do_for_cell(self, cell_index, callback):
        return callback(get_driver(cell_index))

    def tablet_cell_is_healthy(self, cell_id):
        multicell_status = get(f"//sys/tablet_cells/{cell_id}/@multicell_status")
        for cell_tag in multicell_status.keys():
            if multicell_status[cell_tag]["health"] != "good":
                return False
        return True

    def run_checkers_iteration(self, checker_state_list, final_iteration=False):
        print_debug("Run {}checkers iteration".format("final " if final_iteration else ""))

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

        with raises_yt_error("Attempted to set master cell roles"):
            set("//sys/@config/multicell_manager/cell_descriptors/13", {"roles": ["cypress_node_host", "chunk_host"]})

        self._reset_dynamically_propagated_master_cells()
        set("//sys/@config/multicell_manager/cell_descriptors/13", {"roles": ["cypress_node_host", "chunk_host"]})

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
    # NB: 1 node will be banned during checks.
    NUM_NODES = 4
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    NUM_MASTERS = 3

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 500,
            "init_retry_period": 500,
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
        def _check_nodes_state(nodes):
            for node in nodes:
                attrs = get(f"//sys/cluster_nodes/{node}/@", attributes=["state", "banned"])
                if attrs["banned"]:
                    assert attrs["state"] == "offline"
                else:
                    assert attrs["state"] == "online"
            return True

        nodes = ls("//sys/cluster_nodes")
        _check_nodes_state(nodes)

        yield

        multicell_states = {tag : "online" for tag in ["10", "11", "12", "13"]}
        for node in nodes:
            if get(f"//sys/cluster_nodes/{node}/@banned"):
                assert get(f"//sys/cluster_nodes/{node}/@multicell_states") == {tag: "offline" for tag in ["10, 11, 12, 13"]}
            else:
                assert get(f"//sys/cluster_nodes/{node}/@multicell_states") == multicell_states
        assert_true_for_all_cells(self.Env, lambda driver: _check_nodes_state(nodes))

    def check_map_reduce_avialibility(self):
        def _check_basic_map_reduce():
            data = [{"foo": i} for i in range(3)]
            create("table", "//tmp/in", force=True)
            write_table("//tmp/in", data)
            assert read_table("//tmp/in") == data
            create("table", "//tmp/out", force=True)

            map_reduce(
                mapper_command="cat",
                reducer_command="cat",
                in_="//tmp/in",
                out="//tmp/out",
                sort_by=["foo"]
            )

        _check_basic_map_reduce()

        yield

        wait(lambda: self.do_with_retries(_check_basic_map_reduce))

    def check_transactions(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
        tx = start_transaction(timeout=120000)
        create("table", "//tmp/p1/t", tx=tx)  # replicate tx to cell 12
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12]

        yield

        self._reset_dynamically_propagated_master_cells()

        set("//sys/@config/multicell_manager/cell_descriptors", {"13": {"roles": ["cypress_node_host", "chunk_host"]}})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
        create("table", "//tmp/p2/t", tx=tx)  # replicate tx to cell 13
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12, 13]

    def check_banned_nodes(self):
        node = ls("//sys/data_nodes")[-1]

        maintenance_id = add_maintenance("cluster_node", node, "ban", "Test")[node]
        wait(lambda: get(f"//sys/data_nodes/{node}/@state") == "offline")

        for tx in ls("//sys/topmost_transactions", attributes=["title"]):
            if node in tx.attributes["title"]:
                abort_transaction(str(tx))

        yield

        remove_maintenance("cluster_node", node, id=maintenance_id)

        def maintenance_request_removed():
            for cell_index in range(4):
                driver = get_driver(cell_index)
                for request in get(f"//sys/data_nodes/{node}/@maintenance_requests", driver=driver).values():
                    if request["type"] == "banned":
                        return False
            return True

        wait(maintenance_request_removed)

        wait(lambda: get(f"//sys/data_nodes/{node}/@state") == "online")
        wait(lambda: get(f"//sys/data_nodes/{node}/@multicell_states") == {
            "10": "online",
            "11": "online",
            "12": "online",
            "13": "online",
        })

    def check_restart_with_snapshot(self):
        yield

        index = None
        for i, master in enumerate(ls("//sys/secondary_masters/13")):
            if get(f"//sys/secondary_masters/13/{master}/orchid/monitoring/hydra/active_follower"):
                index = i
                break

        assert index is not None
        self._build_master_snapshots(set_read_only=False)
        cell_index = self.Env.yt_config.secondary_cell_count + 1
        self.Env.kill_masters_at_cells(cell_indexes=[cell_index], indexes=[index])
        self._build_master_snapshots(set_read_only=False)

    def check_chunks(self):
        yield

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        wait(lambda: self.do_with_retries(lambda: write_table("//tmp/t", [{"x": 1}])))
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == 3)
        stored_replicas = sorted(get("//sys/chunks/{}/@stored_replicas".format(chunk_id)))

        set("//sys/@config/chunk_manager/max_misscheduled_replication_jobs_per_heartbeat", 0)
        set("//sys/@config/chunk_manager/max_misscheduled_removal_jobs_per_heartbeat", 0)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == 3)
        wait(lambda: sorted(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == stored_replicas)


class MasterCellAdditionWithRemoteClustersBaseChecks(MasterCellAdditionBase):
    NUM_NODES = 3
    NUM_CHAOS_NODES = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    NUM_REMOTE_CLUSTERS = 2
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 0
    NUM_SECONDARY_MASTER_CELLS_REMOTE_1 = 0

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 500,
            "init_retry_period": 500,
        },
    }

    DELTA_DRIVER_CONFIG = {
        "cell_directory_synchronizer": {
            "sync_cells_with_secondary_masters": False,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chaos_manager": {
            "alien_cell_synchronizer": {
                "enable": True,
                "sync_period": 100,
                "full_sync_period": 200,
            },
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

    def check_remote_copy(self):
        yield

        remote_driver = get_driver(cluster="remote_0")
        create("table", "//tmp/t1", driver=remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=remote_driver)

        def _check_basic_remote_copy():
            create("table", "//tmp/t2", attributes={"external_cell_tag": 13}, force=True)
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"cluster_name": "remote_0"},
            )

            assert read_table("//tmp/t2") == [{"a": "b"}]

        wait(lambda: self.do_with_retries(_check_basic_remote_copy))
