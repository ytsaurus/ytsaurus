from yt_env_setup import NODES_SERVICE, Restarter, YTEnvSetup, update_inplace
from yt.common import YtError
from yt.environment.helpers import assert_items_equal
from yt.environment.default_config import get_dynamic_node_config

from yt_commands import (
    authors, create_dynamic_table, get_cell_tag, get_driver, get_singular_chunk_id, insert_rows, map_reduce, sync_create_cells,
    read_table, select_rows, sync_mount_table, wait, get, set, ls, create,
    start_transaction, write_table)

from yt_master_cell_addition_base import MasterCellAdditionBase, MasterCellAdditionBaseChecks, MasterCellAdditionWithRemoteClustersBaseChecks

from os import listdir
from os.path import isfile, join
import zstandard as zstd

import io
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


class TestMasterCellAdditionWithoutDowntimeOldProtocolForNodes(TestMasterCellAddition):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()
    DOWNTIME_ALL_COMPONENTS = False
    DELTA_DYNAMIC_MASTER_CONFIG = {
        "node_tracker": {
            "return_master_cells_connection_configs_on_node_registration": False,
            "return_master_cells_connection_configs_on_node_heartbeat": False,
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "data_node": {
                "master_connector": {
                    "check_chunks_cell_tags_before_heartbeats": True,
                    "force_sync_master_cell_directory_before_check_chunks": True,
                    "check_chunks_cell_tags_after_registration_on_primary_master": False,
                    "check_chunks_cell_tags_after_receiving_new_master_cell_configs": False,
                },
            },
        },
    }


class TestMasterCellAdditionWithoutDowntimeRpcProxy(TestMasterCellAdditionWithoutDowntime):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


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
        },
    }

##################################################################


class TestMasterCellsMultipleAdditions(MasterCellAdditionBase):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

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


class TestMasterCellAdditionWithRemoteClusters(MasterCellAdditionWithRemoteClustersBaseChecks):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_TEST_PARTITIONS = 3

    DOWNTIME_ALL_COMPONENTS = True

    @authors("cherepashka", "ponasenko-rs")
    @pytest.mark.timeout(350)
    def test_add_new_cell(self):
        self.execute_checks_with_cell_addition(downtime=self.DOWNTIME_ALL_COMPONENTS)


class TestMasterCellAdditionWithRemoteClustersWithoutDowntime(TestMasterCellAdditionWithRemoteClusters):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DOWNTIME_ALL_COMPONENTS = False


class TestMasterCellAdditionWithRemoteClustersWithoutDowntimeOldProtocolForNodes(TestMasterCellAdditionWithRemoteClusters):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()
    DOWNTIME_ALL_COMPONENTS = False
    DELTA_DYNAMIC_MASTER_CONFIG = {
        "node_tracker": {
            "return_master_cells_connection_configs_on_node_registration": False,
            "return_master_cells_connection_configs_on_node_heartbeat": False,
        },
    }


class TestMasterCellAdditionWithRemoteClustersWithoutDowntimeRpcProxy(TestMasterCellAdditionWithRemoteClustersWithoutDowntime):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


class TestMasterCellsListChangeWithRemoteClustersWithoutDowntime(TestMasterCellAdditionWithRemoteClusters):
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

    @authors("cherepashka")
    @pytest.mark.timeout(100)
    def test_hive_in_cells(self):
        def check_cell_tags(cell_id, expected_cell_tags):
            actual_cell_tags = get(f"//sys/tablet_cells/{cell_id}/@multicell_status").keys()
            return sorted(actual_cell_tags) == sorted(expected_cell_tags)

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
            wait(lambda: self.tablet_cell_is_healthy(cell_id))

        cell_id = sync_create_cells(1)[0]
        assert self.tablet_cell_is_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12", "13"])
        schema = [{"name": "k", "type": "int64", "sort_order": "ascending"}, {"name": "v", "type": "string"}]
        create_dynamic_table("//tmp/dt", schema=schema)
        sync_mount_table("//tmp/dt")
        rows = [{"k": i, "v": f"aba{i}"} for i in range(5)]
        insert_rows("//tmp/dt", rows)

        self._disable_last_cell()

        wait_for_cell_to_become_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12"])
        assert_items_equal(select_rows("* from [//tmp/dt]"), rows)

        self._enable_last_cell(downtime=False)

        wait_for_cell_to_become_healthy(cell_id)
        assert check_cell_tags(cell_id, ["10", "11", "12", "13"])
        assert_items_equal(select_rows("* from [//tmp/dt]"), rows)


##################################################################


class TestDynamicMasterCellPropagation(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_NODES = 6
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

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

    def check_portals(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
        tx = start_transaction(timeout=120000)
        create("table", "//tmp/p1/t", tx=tx)  # replicate tx to cell 12
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12]

        yield

        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
        create("table", "//tmp/p2/t", tx=tx)  # replicate tx to cell 13
        assert get("#{}/@replicated_to_cell_tags".format(tx)) == [12, 13]

    def check_no_nodes_reregistration(self):
        nodes = ls("//sys/cluster_nodes")
        lease_txs = {}
        for node in nodes:
            lease_txs[node] = get(f"//sys/cluster_nodes/{node}/@lease_transaction_id")

        yield

        # Make sure nodes have discovered the new cell and the last master cell receive all heartbetas.
        wait(lambda: self._nodes_synchronized_with_masters(nodes))
        self._wait_for_nodes_state("online", aggregate_state=False)

        # Nodes should not reregister.
        for node in nodes:
            assert lease_txs[node] == get(f"//sys/cluster_nodes/{node}/@lease_transaction_id")

    def check_basic_map_reduce(self):
        yield

        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in", attributes={"external_cell_tag": 11})
        write_table("//tmp/in", data)
        assert read_table("//tmp/in") == data
        create("table", "//tmp/out", attributes={"external_cell_tag": 13})

        def map_reduce_wrapper():
            map_reduce(
                mapper_command="cat",
                reducer_command="cat",
                in_="//tmp/in",
                out="//tmp/out",
                sort_by=["foo"]
            )

        wait(lambda: self.do_with_retries(map_reduce_wrapper))

    def check_basic_tables_operation(self):
        yield

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        wait(lambda: self.do_with_retries(lambda: write_table("//tmp/t", [{"a" : "b"}])))
        assert read_table("//tmp/t") == [{"a" : "b"}]

    def check_basic_dynamic_tables_operations(self):
        yield

        schema = [{"name": "k", "type": "int64", "sort_order": "ascending"}, {"name": "v", "type": "string"}]
        rows = [{"k": i, "v": f"aba{i}"} for i in range(5)]

        cell_id = sync_create_cells(1)[0]
        wait(lambda: self.tablet_cell_is_healthy(cell_id))

        create_dynamic_table("//tmp/dt1", schema=schema, external_cell_tag=13)
        wait(lambda: self.do_with_retries(lambda: sync_mount_table("//tmp/dt1")))
        insert_rows("//tmp/dt1", rows)
        assert_items_equal(select_rows("* from [//tmp/dt1]"), rows)

    @authors("cherepashka")
    def test_add_cell(self):
        self.execute_checks_with_cell_addition(downtime=False)


class TestDynamicMasterCellPropagationRpcProxy(TestDynamicMasterCellPropagation):
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


##################################################################


class TestMasterCellDynamicPropagationDuringNodeRegistration(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_NODES = 4

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
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        if cls.node_counter % 2 == 0:
            config["flavors"] = ["data"]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

        cls._collect_cell_ids_and_maybe_stash_last_cell(
            config["cluster_connection"],
            cluster_index,
            cls.get_param("REMOVE_LAST_MASTER_BEFORE_START", cluster_index))

    @authors("cherepashka")
    def test_registration_after_synchronization(self):
        self.Env.kill_nodes()
        self._enable_last_cell(downtime=False, wait_for_nodes=False)
        # Registration on primary master triggers master cell synhronization, which follows receiving new master cell
        # and attempt of starting cellar/data/tablet heartbeats before actual registration.
        # This shouldn't crash node.
        self.Env.start_nodes()


class TestNodeRestartAfterCellAddition(MasterCellAdditionBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts and defer start.
    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = builtins.set()

    NUM_NODES = 4

    DELTA_NODE_CONFIG = {
        "exec_node_is_not_data_node": True,
        "delay_master_cell_directory_start": True,
        # NB: In real clusters this flag is disabled.
        "data_node": {
            "sync_directories_on_connect": False,
        },
        "sync_directories_on_connect": False,
    }

    @authors("grphil")
    def test_node_restart_after_cell_addition(self):
        self.execute_checks_with_cell_addition(downtime=False)
        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        wait(lambda: self.do_with_retries(lambda: write_table("//tmp/t", [{"a" : "b"}])))
        chunk_id = get_singular_chunk_id("//tmp/t")
        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: chunk_id in get("//sys/lost_chunks"))
            wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 0)
        wait(lambda: chunk_id not in get("//sys/lost_chunks"))
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) > 0)

        def log_contains_text(logs_path, text):
            node_files = [join(logs_path, f) for f in listdir(logs_path) if "node" in f and ".log.zst" in f and isfile(join(logs_path, f))]

            for file_path in node_files:
                with open(file_path, "rb") as log_file:
                    decompressor = zstd.ZstdDecompressor()
                    binary_reader = decompressor.stream_reader(log_file, read_size=8192)
                    text_stream = io.TextIOWrapper(binary_reader, encoding='utf-8')
                    for line in text_stream:
                        if text in line:
                            return True

            return False

        wait(lambda: log_contains_text(self.path_to_run + "/logs", "Chunk from unknown master was scanned"))
        wait(lambda: log_contains_text(self.path_to_run + "/logs", "Chunks cell tags are checked (InvalidCells: 0, InvalidChunkCount: 0)"))


##################################################################


@pytest.mark.skip(reason="after YT-26685 need to rewrite this tests, maybe make it uniitests on master cell directory")
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
