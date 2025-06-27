from yt_env_setup import YTEnvSetup, update_inplace
from yt.common import YtError
from yt.environment.default_config import get_dynamic_node_config

from yt_commands import (
    authors, create_dynamic_table, get_cell_tag, get_driver, insert_rows, map_reduce, sync_create_cells,
    raises_yt_error, read_table, select_rows, sync_mount_table, wait, get, set, ls, create,
    start_transaction, write_table)

from yt_master_cell_addition_base import MasterCellAdditionBase, MasterCellAdditionBaseChecks, MasterCellAdditionChaosMultiClusterBaseChecks

import time
import pytest
import builtins

##################################################################


class TestMasterCellAddition(MasterCellAdditionBaseChecks):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_TEST_PARTITIONS = 3
    DOWNTIME_ALL_COMPONENTS = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    @authors("shakurov", "cherepashka")
    @pytest.mark.timeout(120)
    def test_add_new_cell(self):
        self.execute_checks_with_cell_addition(downtime=self.DOWNTIME_ALL_COMPONENTS)


class TestMasterCellAdditionWithoutDowntime(TestMasterCellAddition):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DOWNTIME_ALL_COMPONENTS = False


class TestMasterCellsListChangeWithoutDowntime(MasterCellAdditionBaseChecks):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DOWNTIME_ALL_COMPONENTS = False
    REMOVE_LAST_MASTER_BEFORE_START = False

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_removal": True,
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    @authors("shakurov", "cherepashka")
    @pytest.mark.timeout(200)
    def test_add_new_cell(self):
        self.execute_checks_with_cell_addition(downtime=self.DOWNTIME_ALL_COMPONENTS)


class TestMasterCellsListChangeWithoutDowntimeRemoveSecondaryCellDefaultRoles(TestMasterCellsListChangeWithoutDowntime):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_removal": True,
                "allow_master_cell_with_empty_role": True,
            },
            # NB: Allow to remove secondary cell default roles from cells with chunks and nodes behind portal.
            "allow_master_cell_role_invariant_check": False,
            "remove_secondary_cell_default_roles": True,
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["cypress_node_host", "chunk_host"]},
        "12": {"roles": ["cypress_node_host", "chunk_host"]},
    }

##################################################################


class TestMasterCellsMultipleAdditions(MasterCellAdditionBase):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 1
    REMOVE_LAST_MASTER_BEFORE_START = False

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_removal": True,
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
    }

    @authors("cherepashka")
    @pytest.mark.timeout(200)
    def test_add_new_cell(self):
        self._disable_last_cell()

        self._enable_last_cell(downtime=False)
        wait(lambda: sorted(get("//sys/secondary_masters").keys()) == ["11", "12", "13"])

        self._disable_last_cell()
        wait(lambda: sorted(get("//sys/secondary_masters").keys()) == ["11", "12"])

        self._enable_last_cell(downtime=False)
        wait(lambda: sorted(get("//sys/secondary_masters").keys()) == ["11", "12", "13"])

##################################################################


class TestMasterCellAdditionChaosMultiCluster(MasterCellAdditionChaosMultiClusterBaseChecks):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_TEST_PARTITIONS = 3

    DOWNTIME_ALL_COMPONENTS = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    @authors("ponasenko-rs")
    @pytest.mark.timeout(300)
    def test_add_new_cell(self):
        set("//sys/@config/chaos_manager/alien_cell_synchronizer", {
            "enable": True,
            "sync_period": 100,
            "full_sync_period": 200,
        })

        self.execute_checks_with_cell_addition(downtime=self.DOWNTIME_ALL_COMPONENTS)


class TestMasterCellAdditionChaosMultiClusterWithoutDowntime(TestMasterCellAdditionChaosMultiCluster):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DOWNTIME_ALL_COMPONENTS = False


class TestMasterCellsListChangeChaosMultiClusterWithoutDowntime(TestMasterCellAdditionChaosMultiCluster):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DOWNTIME_ALL_COMPONENTS = False
    REMOVE_LAST_MASTER_BEFORE_START = False

    # Overrides the same field from base class.
    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_removal": True,
                "allow_master_cell_with_empty_role": True,
            },
        },
    }


##################################################################


class TestDynamicMasterCellListChangeWithTabletCells(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    USE_DYNAMIC_TABLES = True
    REMOVE_LAST_MASTER_BEFORE_START = False

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cell_master": {
            "logging": {
                "suppressed_messages": [
                    # NB: Tablet cells should not be replicated properly to the new secondary master for now.
                    "Cell prerequisite transaction not found at secondary master",
                ],
            },
        },
        "multicell_manager": {
            "testing": {
                "allow_master_cell_removal": True,
                "allow_master_cell_with_empty_role": True,
            },
        },
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,  # 2 sec
            "peer_revocation_timeout": 3000,  # 3 sec
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    @authors("cherepashka")
    @pytest.mark.timeout(100)
    def test_hive_in_cells(self):
        def check_cell_tags(cell_id, expected_cell_tags):
            actual_cell_tags = get(f"//sys/tablet_cells/{cell_id}/@multicell_status").keys()
            return sorted(actual_cell_tags) == sorted(expected_cell_tags)

        def cell_is_healthy(cell_id):
            multicell_status = get(f"//sys/tablet_cells/{cell_id}/@multicell_status")
            for cell_tag in multicell_status.keys():
                if multicell_status[cell_tag]["health"] != "good":
                    return False
            return True

        def get_cell_peer(cell_id):
            try:
                return get(f"#{cell_id}/@peers/0/address")
            except YtError:
                return None

        def wait_for_cell_to_become_healthy(cell_id):
            # Need to restart tablet nodes (in this test all nodes since they are multiflavored here) to make tablets healthy.
            self.Env.kill_nodes()
            wait(lambda: get_cell_peer(cell_id) is None)
            self.Env.start_nodes()

            wait(lambda: get_cell_peer(cell_id) is not None)
            wait(lambda: cell_is_healthy(cell_id))

        cell_id = sync_create_cells(1)[0]
        assert cell_is_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12", "13"])
        schema = [{"name": "k", "type": "int64", "sort_order": "ascending"}, {"name": "v", "type": "string"}]
        create_dynamic_table("//tmp/dt", schema=schema)
        sync_mount_table("//tmp/dt")
        rows = [{"k": i, "v": f"aba{i}"} for i in range(5)]
        insert_rows("//tmp/dt", rows)

        self._disable_last_cell()

        wait_for_cell_to_become_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12"])
        assert select_rows("* from [//tmp/dt]") == rows

        self._enable_last_cell(downtime=False)

        wait_for_cell_to_become_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12", "13"])
        assert select_rows("* from [//tmp/dt]") == rows


##################################################################


class TestDynamicMasterCellPropagation(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 6
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    DELTA_NODE_CONFIG = {
        "exec_node_is_not_data_node": True,
        "delay_master_cell_directory_start": True,
        # NB: In real clusters this flag is disabled.
        "data_node": {
            "sync_directories_on_connect": False,
        },
        "sync_directories_on_connect": False,
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        node_flavors = [
            ["data"],
            ["exec"],
            ["tablet"],
            ["data", "exec"],
            ["data", "tablet"],
            ["exec", "tablet"],
        ]

        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        config["flavors"] = node_flavors[cls.node_counter]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @authors("cherepashka")
    def test_add_cell(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
        tx = start_transaction(timeout=120000)
        create("table", "//tmp/p1/t", tx=tx)  # replicate tx to cell 12
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12]

        nodes = ls("//sys/cluster_nodes")
        lease_txs = {}
        for node in nodes:
            lease_txs[node] = get(f"//sys/cluster_nodes/{node}/@lease_transaction_id")

        self._enable_last_cell(downtime=False)

        # Make sure nodes have discovered the new cell.
        wait(lambda: self._nodes_synchronized_with_masters(nodes))

        # Nodes should not reregister.
        for node in ls("//sys/cluster_nodes"):
            assert lease_txs[node] == get(f"//sys/cluster_nodes/{node}/@lease_transaction_id")

        with raises_yt_error("not discovered by all nodes"):
            set("//sys/@config/multicell_manager/cell_descriptors", {"13": {"roles": ["cypress_node_host", "chunk_host"]}})

        # Make the new master cell "reliable" for other master cells.
        set("//sys/@config/multicell_manager/testing/discovered_masters_cell_tags", [13])
        set("//sys/@config/multicell_manager/cell_descriptors", {"13": {"roles": ["cypress_node_host", "chunk_host"]}})

        self._wait_for_nodes_state("online")

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        write_table("//tmp/t", [{"a" : "b"}])
        assert read_table("//tmp/t") == [{"a" : "b"}]

        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
        create("table", "//tmp/p2/t", tx=tx)  # replicate tx to cell 13
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12, 13]

        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in", attributes={"external_cell_tag": 11})
        write_table("//tmp/in", data)
        assert read_table("//tmp/in") == data
        create("table", "//tmp/out", attributes={"external_cell_tag": 13})

        map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            sort_by=["foo"]
        )

        # Just in case. Nodes still should not reregister.
        for node in ls("//sys/cluster_nodes"):
            assert lease_txs[node] == get(f"//sys/cluster_nodes/{node}/@lease_transaction_id")


##################################################################


class TestMasterCellDynamicPropagationDuringMultiflavorNodeRegistration(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 4

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "multicell_manager": {
            "testing": {
                "allow_master_cell_with_empty_role": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "13": {"roles": []},
    }

    @authors("cherepashka")
    def test_registration_after_synchronization(self):
        self.Env.kill_nodes()
        self._enable_last_cell(downtime=False, wait_for_nodes=False)
        # Registration on primary master triggers master cell synhronization, which follows receiving new master cell
        # and attempt of starting cellar/data/tablet heartbeats before actual registration.
        # This shouldn't crash node.
        self.Env.start_nodes()


class TestMasterCellDynamicPropagationDuringDataNodeRegistration(TestMasterCellDynamicPropagationDuringMultiflavorNodeRegistration):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DELTA_NODE_CONFIG = {
        "exec_node_is_not_data_node": True,
        "delay_master_cell_directory_start": True,
        # NB: In real clusters this flag is disabled.
        "data_node": {
            "sync_directories_on_connect": False,
        },
        "sync_directories_on_connect": False,
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["flavors"] = ["data"]

        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))


##################################################################


class TestMasterCellsPeersListChange(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are components restarts.
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 1
    DEFER_NODE_START = True

    REMOVED_PEER = None

    def setup_class(cls):
        super(TestMasterCellsPeersListChange, cls).setup_class()

        for cluster_index, env in enumerate([cls.Env] + cls.remote_envs):
            if cls.get_param("NUM_NODES", cluster_index) != 0:
                env.start_nodes()

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if cluster_index != 0:
            return
        if cls.REMOVED_PEER is None:
            cls.REMOVED_PEER = config["cluster_connection"]["secondary_masters"][-1]["peers"][-1]["address"][::]
        assert cls.REMOVED_PEER == config["cluster_connection"]["secondary_masters"][-1]["peers"][-1]["address"]
        assert cls.REMOVED_PEER == config["cluster_connection"]["secondary_masters"][-1]["addresses"][-1]
        del config["cluster_connection"]["secondary_masters"][-1]["peers"][-1]
        del config["cluster_connection"]["secondary_masters"][-1]["addresses"][-1]

    def _get_connected_secondary_masters_addresses(slef, node, cell_id):
        connected_secondary_masters = get(f"//sys/cluster_nodes/{node}/orchid/connected_secondary_masters", driver=get_driver(0))
        connected_secondary_masters = {int(cell_tag) : connection_config for cell_tag, connection_config in connected_secondary_masters.items()}
        cell_tag = get_cell_tag(cell_id)
        if cell_tag not in connected_secondary_masters.keys():
            return None
        return connected_secondary_masters[cell_tag]["addresses"]

    @authors("cherepashka")
    @pytest.mark.parametrize("increase_sync_period", [True, False])
    def test_master_cell_peers_addition_on_node(self, increase_sync_period):
        def _check_for_all_nodes_have_same_host_status(host_should_be_removed=True):
            for node in nodes:
                connected_secondary_masters = self._get_connected_secondary_masters_addresses(node, last_cell["cell_id"])
                if connected_secondary_masters is None:
                    return False
                if host_should_be_removed and self.REMOVED_PEER in connected_secondary_masters:
                    return False
                elif not host_should_be_removed and self.REMOVED_PEER not in connected_secondary_masters:
                    return False
            return True

        nodes = ls("//sys/cluster_nodes")
        secondary_masters = get("//sys/@cluster_connection/secondary_masters")
        last_cell = secondary_masters[-1]
        assert self.REMOVED_PEER == last_cell["peers"][-1]["address"]

        # Remove last peer on master testing override.
        secondary_masters[-1]["addresses"] = secondary_masters[-1]["addresses"][:-1]
        secondary_masters[-1]["peers"] = secondary_masters[-1]["peers"][:-1]
        set("//sys/@config/multicell_manager/testing/master_cell_directory_override", {
            "secondary_masters" :  secondary_masters
        }, driver=get_driver(0))

        # Restart nodes so masters will be able to "remove" last address.
        self.Env.kill_nodes()
        self.Env.start_nodes()

        # Wait for all nodes to receive new master cells configuration.
        wait(lambda: _check_for_all_nodes_have_same_host_status(host_should_be_removed=True))

        if increase_sync_period:
            # Increase synchronization period for discovery of new peers.
            current_config = get_dynamic_node_config()["%true"]
            old_patch = {
                "master_cell_directory_synchronizer": current_config["master_cell_directory_synchronizer"].copy(),
            }
            patch = {
                "master_cell_directory_synchronizer": {
                    "sync_period": 360000,
                    "retry_period": 360000,
                }
            }
            update_inplace(current_config, patch)
            config = {
                "%true": current_config,
            }
            set("//sys/cluster_nodes/@config", config)
            # Wait for running synchronization to finish.
            time.sleep(2)

        # Return last peer.
        secondary_masters[-1]["addresses"].append(self.REMOVED_PEER)
        secondary_masters[-1]["peers"].append(self.REMOVED_PEER)
        set("//sys/@config/multicell_manager/testing/master_cell_directory_override", {
            "secondary_masters" :  secondary_masters
        }, driver=get_driver(0))

        if increase_sync_period:
            time.sleep(2)
            # Synchronization didn't happen yet, host was not added yet.
            assert _check_for_all_nodes_have_same_host_status(host_should_be_removed=True)

            update_inplace(current_config, old_patch)
            config = {
                "%true": current_config,
            }
            set("//sys/cluster_nodes/@config", config)

        # Wait for all nodes to receive new master cells configuration.
        wait(lambda: _check_for_all_nodes_have_same_host_status(host_should_be_removed=False))

    @authors("cherepashka")
    def test_no_master_cell_peers_removals_on_node(self):
        nodes = ls("//sys/cluster_nodes")
        secondary_masters = get("//sys/@cluster_connection/secondary_masters")
        last_cell = secondary_masters[-1]
        assert self.REMOVED_PEER == last_cell["peers"][-1]["address"]

        def _check_for_all_nodes_have_same_hosts():
            for node in nodes:
                connected_secondary_masters = self._get_connected_secondary_masters_addresses(node, last_cell["cell_id"])
                if connected_secondary_masters is None or self.REMOVED_PEER not in connected_secondary_masters:
                    return False
            return True

        # Nodes received atual configuration via master cell directory synchronizer.
        assert _check_for_all_nodes_have_same_hosts()

        # Remove last peer on master testing override.
        secondary_masters[-1]["addresses"] = secondary_masters[-1]["addresses"][:-1]
        secondary_masters[-1]["peers"] = secondary_masters[-1]["peers"][:-1]
        set("//sys/@config/multicell_manager/testing/master_cell_directory_override", {
            "secondary_masters" :  secondary_masters
        }, driver=get_driver(0))

        # Wait for synchronization.
        time.sleep(2)
        assert _check_for_all_nodes_have_same_hosts()


##################################################################
