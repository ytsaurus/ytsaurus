from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, MASTERS_SERVICE, with_additional_threads

from yt_commands import (
    authors, create_user, wait, create, ls, get, set, remove, exists,
    start_transaction, insert_rows, build_snapshot, gc_collect, concatenate, create_account, create_rack,
    read_table, write_table, write_journal, merge, sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_control_chunk_replicator, get_singular_chunk_id, multicell_sleep, update_nodes_dynamic_config,
    switch_leader, set_node_banned, add_maintenance, remove_maintenance, set_node_decommissioned, execute_command,
    is_active_primary_master_leader, is_active_primary_master_follower, get_active_primary_master_leader_address,
    get_active_primary_master_follower_address, create_tablet_cell_bundle, get_nodes, raises_yt_error)

from yt_helpers import profiler_factory

from yt.environment.helpers import assert_items_equal
from yt.common import YtError, WaitFailed
import yt.yson as yson

import pytest
from flaky import flaky

from collections import defaultdict

import json
import os
from time import sleep

##################################################################


class TestChunkServer(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 3
    NUM_NODES = 9
    NUM_TEST_PARTITIONS = 4

    DELTA_NODE_CONFIG = {
        "data_node": {
            "disk_health_checker": {
                "check_period": 1000,
            },
        },
    }

    @authors("babenko", "ignat")
    def test_owning_nodes1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@owning_nodes") == ["//tmp/t"]

    @authors("babenko", "ignat")
    def test_owning_nodes2(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        write_table("//tmp/t", {"a": "b"}, tx=tx)
        chunk_id = get_singular_chunk_id("//tmp/t", tx=tx)
        assert get("#" + chunk_id + "/@owning_nodes") == \
            [yson.to_yson_type("//tmp/t", attributes={"transaction_id": tx})]

    @authors("babenko", "shakurov")
    def test_replication(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert get("//tmp/t/@replication_factor") == 3

        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

    def _test_decommission(self, path, replica_count, node_to_decommission_count=1):
        assert replica_count >= node_to_decommission_count

        chunk_id = get_singular_chunk_id(path)

        nodes_to_decommission = self._decommission_chunk_replicas(chunk_id, replica_count, node_to_decommission_count)

        wait(
            lambda: not self._nodes_have_chunk(nodes_to_decommission, chunk_id)
            and len(get(f"#{chunk_id}/@stored_replicas")) == replica_count
        )

    def _decommission_chunk_replicas(self, chunk_id, replica_count, node_to_decommission_count):
        nodes_to_decommission = get(f"#{chunk_id}/@stored_replicas")
        assert len(nodes_to_decommission) == replica_count

        nodes_to_decommission = nodes_to_decommission[:node_to_decommission_count]
        assert self._nodes_have_chunk(nodes_to_decommission, chunk_id)

        for node in nodes_to_decommission:
            set_node_decommissioned(node, True)

        return nodes_to_decommission

    def _nodes_have_chunk(self, nodes, id):
        def id_to_hash(id):
            return id.split("-")[3]

        for node in nodes:
            if not (
                id_to_hash(id) in [id_to_hash(id_) for id_ in ls(f"//sys/cluster_nodes/{node}/orchid/data_node/stored_chunks")]
            ):
                return False
        return True

    @authors("babenko")
    def test_decommission_regular1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})
        self._test_decommission("//tmp/t", 3)

    @authors("shakurov")
    def test_decommission_regular2(self):
        create("table", "//tmp/t", attributes={"replication_factor": 4})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 4})

        chunk_id = get_singular_chunk_id("//tmp/t")

        self._decommission_chunk_replicas(chunk_id, 4, 2)
        set("//tmp/t/@replication_factor", 3)
        # Now 2 replicas are decommissioned and 2 aren't.
        # The chunk should be both under- and overreplicated.

        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)
        nodes = get(f"#{chunk_id}/@stored_replicas")
        for node in nodes:
            assert not get(f"//sys/cluster_nodes/{node}/@decommissioned")

    @authors("babenko")
    def test_decommission_erasure1(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "reed_solomon_3_3")
        write_table("//tmp/t", {"a": "b"})
        self._test_decommission("//tmp/t", 6)

    @authors("shakurov")
    def test_decommission_erasure2(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "reed_solomon_3_3")
        write_table("//tmp/t", {"a": "b"})
        self._test_decommission("//tmp/t", 6, 3)

    @authors("ignat")
    def test_decommission_erasure3(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "reed_solomon_3_3")
        write_table("//tmp/t", {"a": "b"})

        sync_control_chunk_replicator(False)

        chunk_id = get_singular_chunk_id("//tmp/t")
        nodes = get(f"#{chunk_id}/@stored_replicas")

        for index in (1, 3, 4):
            set_node_banned(nodes[index], True, wait_for_master=False)
        set_node_decommissioned(nodes[0], True)

        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        sync_control_chunk_replicator(True)

        wait(lambda: get(f"//sys/cluster_nodes/{nodes[0]}/@decommissioned"))
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 6)

    @authors("babenko")
    def test_decommission_journal(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", [{"payload": "payload" + str(i)} for i in range(0, 10)], enable_chunk_preallocation=False)
        self._test_decommission("//tmp/j", 3)

    @authors("babenko")
    def test_list_chunk_owners(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])
        ls("//sys/chunks", attributes=["owning_nodes"])

    @authors("babenko")
    def test_disable_replicator_when_few_nodes_are_online(self):
        set("//sys/@config/chunk_manager/safe_online_node_count", 3)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 9

        wait(lambda: get("//sys/@chunk_replicator_enabled"))

        for i in range(7):
            set_node_banned(nodes[i], True, wait_for_master=False)

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    @authors("babenko")
    def test_disable_replicator_when_explicitly_requested_so(self):
        wait(lambda: get("//sys/@chunk_replicator_enabled"))

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    @authors("babenko", "ignat")
    def test_hide_chunk_attrs(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunks = ls("//sys/chunks")
        for c in chunks:
            assert len(c.attributes) == 0

        chunks_json = execute_command("list", {"path": "//sys/chunks", "output_format": "json"})
        for c in json.loads(chunks_json):
            assert isinstance(c, str)

    @authors("shakurov")
    def test_chunk_requisition_registry_orchid(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        master = ls("//sys/primary_masters")[0]
        master_orchid_path = "//sys/primary_masters/{0}/orchid/chunk_manager/requisition_registry".format(master)

        known_requisition_indexes = frozenset(ls(master_orchid_path))
        set("//tmp/t/@replication_factor", 4)
        sleep(0.3)
        new_requisition_indexes = frozenset(ls(master_orchid_path)) - known_requisition_indexes
        assert len(new_requisition_indexes) == 1
        new_requisition_index = next(iter(new_requisition_indexes))

        new_requisition = get("{0}/{1}".format(master_orchid_path, new_requisition_index))
        assert new_requisition["ref_counter"] == 2  # one for 'local', one for 'aggregated' requisition
        assert new_requisition["vital"]
        assert len(new_requisition["entries"]) == 1
        assert new_requisition["entries"][0]["replication_policy"]["replication_factor"] == 4

    @authors("shakurov")
    def test_node_chunk_replica_count(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) == 3)

        def check_replica_count():
            nodes = get("#{0}/@stored_replicas".format(chunk_id))
            for node in nodes:
                if get("//sys/cluster_nodes/{0}/@chunk_replica_count/default".format(node)) == 0:
                    return False
            return True

        wait(check_replica_count, sleep_backoff=1.0)

    @authors("babenko", "aleksandra-zh")
    def test_max_replication_factor(self):
        MAX_RF = 5
        set("//sys/media/default/@config/max_replication_factor", MAX_RF)

        multicell_sleep()

        create("table", "//tmp/t", attributes={"replication_factor": 8})
        assert get("//tmp/t/@replication_factor") == 8

        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        def ok_replication_status():
            status = get("#{}/@replication_status/default".format(chunk_id))
            return not status["underreplicated"] and not status["overreplicated"]
        wait(ok_replication_status)

        assert len(get("#{0}/@stored_replicas".format(chunk_id))) == MAX_RF

    @authors("gritukan")
    def test_disable_store_location(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False}})
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) == 3)

        node_id = get("#{0}/@stored_replicas".format(chunk_id))[0]
        location_path = get("//sys/cluster_nodes/{}/orchid/data_node/stored_chunks/{}/location".format(node_id, chunk_id))

        with open("{}/disabled".format(location_path), "wb") as f:
            f.write(b"{foo=bar}")

        wait(lambda: node_id not in get("#{0}/@stored_replicas".format(chunk_id)))
        assert not exists("//sys/cluster_nodes/{}/orchid/data_node/stored_chunks/{}".format(node_id, chunk_id))

        # Repair node for future tests.
        os.remove("{}/disabled".format(location_path))
        with Restarter(self.Env, NODES_SERVICE):
            pass

    @authors("kvk1920")
    def test_last_finished_job(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        def check_if_job_finished():
            jobs = get("//sys/chunks/{}/@jobs".format(chunk_id))
            return len(jobs) == 1 and jobs[0]["state"] in {"completed", "aborted", "failed"}

        wait(check_if_job_finished)

    def _reproduce_missing_requisition_update(self):
        create_account("a")
        create_account("b")
        create("table", "//tmp/a", attributes={"account": "a"})
        write_table("//tmp/a", {"a": 1, "b": 2})
        create("table", "//tmp/b", attributes={"account": "b"})
        concatenate(["//tmp/a"], "//tmp/b")
        chunk = get_singular_chunk_id("//tmp/a")
        assert chunk == get_singular_chunk_id("//tmp/b")
        assert get("//tmp/b/@account") == "b"
        wait(lambda: {req["account"] for req in get(f"#{chunk}/@requisition")} == {"a", "b"})
        write_table("//tmp/a", {"a": 2, "b": 3})
        wait(lambda: {req["account"] for req in get(f"#{chunk}/@requisition")} == {"b"})

    @authors("kvk1920")
    @flaky(max_runs=3)
    def test_missing_requisition_update_yt17756(self):
        with pytest.raises(WaitFailed):
            self._reproduce_missing_requisition_update()

    @authors("kvk1920")
    @flaky(max_runs=3)
    def test_fix_missing_requisition_update_yt17756(self):
        set("//sys/@config/chunk_manager/enable_fix_requisition_update_on_merge", True)
        self._reproduce_missing_requisition_update()

    @authors("gritukan")
    def test_historically_non_vital(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        assert not get(f"#{chunk_id}/@historically_non_vital")

        set("//tmp/t/@replication_factor", 1)
        wait(lambda: get(f"#{chunk_id}/@historically_non_vital"))
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 1)

        node = get(f"#{chunk_id}/@stored_replicas")[0]
        set_node_banned(node, True)

        wait(lambda: chunk_id in get("//sys/lost_chunks"))
        assert get("//sys/@lost_vital_chunk_count") == 0
        assert chunk_id not in get("//sys/lost_vital_chunks")

    @authors("achulkov2")
    def test_historically_non_vital_with_erasure(self):
        create("table", "//tmp/t", attributes={"erasure_codec": "isa_reed_solomon_3_3"})
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        assert get(f"#{chunk_id}/@vital")
        assert not get(f"#{chunk_id}/@historically_non_vital")

        nodes = list(get("//sys/cluster_nodes"))
        for node in nodes:
            set_node_banned(node, True)

        wait(lambda: get("//sys/@lost_vital_chunk_count") == 1)
        wait(lambda: chunk_id in get("//sys/lost_vital_chunks"))

        set("//tmp/t/@vital", False)
        wait(lambda: get("//sys/@lost_vital_chunk_count") == 0)
        wait(lambda: chunk_id not in get("//sys/lost_vital_chunks"))
        assert chunk_id in get("//sys/lost_chunks")

        for node in nodes:
            set_node_banned(node, False)

        wait(lambda: chunk_id not in get("//sys/lost_chunks"))

    @authors("achulkov2")
    def test_historically_non_vital_explicit_vitality_change(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        assert get(f"#{chunk_id}/@vital")
        assert not get(f"#{chunk_id}/@historically_non_vital")

        set("//tmp/t/@vital", False)

        wait(lambda: not get(f"#{chunk_id}/@vital"))
        assert not get(f"#{chunk_id}/@historically_non_vital")

        set("//tmp/t/@vital", True)
        wait(lambda: get(f"#{chunk_id}/@vital"))
        assert not get(f"#{chunk_id}/@historically_non_vital")

        nodes = list(get("//sys/cluster_nodes"))
        for node in nodes:
            set_node_banned(node, True)

        wait(lambda: get("//sys/@lost_vital_chunk_count") == 1)
        wait(lambda: chunk_id in get("//sys/lost_vital_chunks"))

        for node in nodes:
            set_node_banned(node, False)

        wait(lambda: get("//sys/@lost_vital_chunk_count") == 0)
        wait(lambda: chunk_id not in get("//sys/lost_vital_chunks"))

    @authors("danilalexeev")
    def test_unexpected_overreplicated_chunks(self):
        create("table", "//tmp/t", attributes={"replication_factor": 8})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 8})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 8)

        set("//tmp/t/@replication_factor", 3)
        wait(lambda: chunk_id in get("//sys/unexpected_overreplicated_chunks"))

        gc_collect()
        wait(lambda: chunk_id not in get("//sys/unexpected_overreplicated_chunks"))

    @authors("gritukan")
    def test_replication_queue_fairness(self):
        set("//sys/@config/incumbent_manager/scheduler/incumbents/chunk_replicator/use_followers", False)
        sleep(1.0)

        create("table", "//tmp/t1")
        write_table("<append=%true>//tmp/t1", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t1")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        set("//sys/@config/chunk_manager/max_misscheduled_replication_jobs_per_heartbeat", 1)

        create_rack("r1")
        create_rack("r2")

        for idx, node in enumerate(ls("//sys/cluster_nodes")):
            set(f"//sys/cluster_nodes/{node}/@rack", "r1" if idx % 2 == 0 else "r2")
            set(f"//sys/cluster_nodes/{node}/@resource_limits_overrides/replication_slots", 1)
            wait(lambda: get(f"//sys/cluster_nodes/{node}/@resource_limits/replication_slots") == 1)

        # Now we try to replicate at most one chunk per job heartbeat.

        # Make sure that all replication queues are filled with some chunks that
        # cannot be placed safely.
        create("table", "//tmp/t2", attributes={"erasure_codec": "isa_reed_solomon_6_3"})
        chunk_count = 30
        for _ in range(chunk_count):
            write_table("<append=%true>//tmp/t2", {"a": "b"})

        wait(lambda: len(get("//sys/unsafely_placed_chunks")) == chunk_count)

        # Replication should still work.
        set("//tmp/t1/@replication_factor", 4)
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 4)


##################################################################


def _find_median(series):
    sorted_series = sorted(series)
    series_size = len(sorted_series)

    if series_size == 1:
        return series[0]

    return sorted_series[series_size // 2]


def _find_median_absolute_deviation(series):
    median = _find_median(series)
    absolute_deviations = []

    for point in series:
        absolute_deviations.append(abs(point - median))

    absolute_deviations.sort()
    return _find_median(absolute_deviations)


@pytest.mark.enabled_multidaemon
class TestTwoRandomChoicesWriteTargetAllocation(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 10

    @authors("h0pless")
    @pytest.mark.timeout(120)
    def test_power_of_two_choices_write_target_allocation(self):
        set("//sys/@config/chunk_manager/enable_two_random_choices_write_target_allocation", True)
        set("//sys/@config/chunk_manager/nodes_to_check_before_giving_up_on_write_target_allocation", 8)

        for i in range(10):
            create("table", f"//tmp/table{i}")

        for i in range(200):
            write_table(f"<append=%true>//tmp/table{i % 10}", [{"hi": i}])

        sleep(1.0)
        node_to_replica_count = defaultdict(int)
        for i in range(10):
            chunk_ids = get(f"//tmp/table{i}/@chunk_ids")

            for chunk_id in chunk_ids:
                stored_replicas = get(f"#{chunk_id}/@stored_replicas")

                for node in stored_replicas:
                    node_to_replica_count[f"{node}"] += 1

        mad = _find_median_absolute_deviation(node_to_replica_count.values())
        assert mad <= 10


##################################################################


@pytest.mark.enabled_multidaemon
class TestTwoRandomChoicesWriteTargetAllocationMulticell(TestTwoRandomChoicesWriteTargetAllocation):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


class TestNodePendingRestartBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 9
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestNodePendingRestartBase, cls).setup_class()

        for i, node in enumerate(get_nodes()):
            rack = f"r{i}"
            create_rack(rack)
            set("//sys/cluster_nodes/" + node + "/@rack", rack)

        set("//sys/media/default/@config/max_replicas_per_rack", 1)

    def setup_method(self, method):
        super(TestNodePendingRestartBase, self).setup_method(method)
        set("//sys/media/default/@config/max_replicas_per_rack", 1)


class TestNodePendingRestart(TestNodePendingRestartBase):
    ENABLE_MULTIDAEMON = False  # Kill specific components.
    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    @authors("danilalexeev")
    def test_pending_restart_request(self):
        create("table", "//tmp/t", attributes={"replication_factor": 3})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        node = str(get(f"#{chunk_id}/@stored_replicas")[0])
        maintenance_id = add_maintenance("cluster_node", node, "pending_restart", "")[node]

        assert get(f"//sys/cluster_nodes/{node}/@pending_restart")
        wait(lambda: chunk_id in get("//sys/replica_temporarily_unavailable_chunks"))

        assert remove_maintenance("cluster_node", node, id=maintenance_id) == {
            node: {"pending_restart": 1}
        }

        assert not get(f"//sys/cluster_nodes/{node}/@pending_restart")
        wait(lambda: chunk_id not in get("//sys/replica_temporarily_unavailable_chunks"))

    @authors("danilalexeev")
    def test_temporarily_unavailable_regular_replicas(self):
        set("//sys/media/default/@config/max_replicas_per_rack", 2)

        create("table", "//tmp/t", attributes={"replication_factor": 3})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        node = get(f"#{chunk_id}/@stored_replicas")[0]
        add_maintenance("cluster_node", node, "pending_restart", "")

        # 1 temporarily unavailable and 3 active (1 + 2 because of rack awareness)
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 4)
        assert node in get(f"#{chunk_id}/@stored_replicas")

    @authors("danilalexeev")
    def test_temporarily_unavailable_regular_replicas2(self):
        create("table", "//tmp/t", attributes={"replication_factor": 4})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 4})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 4)

        nodes = get(f"#{chunk_id}/@stored_replicas")
        for node in nodes[:2]:
            add_maintenance("cluster_node", node, "pending_restart", "")

        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 5)

    @authors("danilalexeev")
    def test_temporarily_unavailable_erasure_replicas(self):
        create("table", "//tmp/t", attributes={"erasure_codec": "reed_solomon_3_3"})
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 6)

        nodes = get(f"#{chunk_id}/@stored_replicas")
        for node in nodes[:2]:
            add_maintenance("cluster_node", node, "pending_restart", "")

        sleep(0.5)
        assert len(get(f"#{chunk_id}/@stored_replicas")) == 6

        status = get("#" + chunk_id + "/@replication_status/default")
        assert status["temporarily_unavailable"]
        assert not status["data_missing"]
        assert not status["parity_missing"]

        add_maintenance("cluster_node", nodes[3], "pending_restart", "")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 9)

    @authors("danilalexeev")
    def test_decommissioned_and_temporarily_unavailable_replicas(self):
        create("table", "//tmp/t", attributes={"replication_factor": 3})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        nodes = get(f"#{chunk_id}/@stored_replicas")

        add_maintenance("cluster_node", nodes[0], "decommission", "")
        add_maintenance("cluster_node", nodes[1], "pending_restart", "")

        wait(lambda: nodes[0] not in get(f"#{chunk_id}/@stored_replicas"))
        assert nodes[1] in get(f"#{chunk_id}/@stored_replicas")

    @authors("danilalexeev")
    def test_pending_restart_lease_expired(self):
        set("//sys/@config/node_tracker/pending_restart_lease_timeout", 3000)

        create("table", "//tmp/t", attributes={"replication_factor": 3})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 3)

        node = get(f"#{chunk_id}/@stored_replicas")[0]
        node_index = get("//sys/cluster_nodes/{}/@annotations/yt_env_index".format(node))

        add_maintenance("cluster_node", node, "pending_restart", "")

        self.Env.kill_service("node", indexes=[node_index])

        sleep(1.5)
        assert get("//sys/cluster_nodes/{}/@state".format(node)) == "online"

        wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
        wait(lambda: not get(f"//sys/cluster_nodes/{node}/@pending_restart"))

        self.Env.start_nodes()

    @authors("danilalexeev")
    @pytest.mark.parametrize("wait_for_removal", [True, False])
    def test_auto_remove_pending_restart1(self, wait_for_removal):
        set("//sys/@config/node_tracker/pending_restart_lease_timeout", 2000)

        node = ls("//sys/cluster_nodes")[0]
        for _ in range(4):
            add_maintenance("cluster_node", node, "pending_restart", "")[node]

        assert get(f"//sys/cluster_nodes/{node}/@pending_restart")

        if wait_for_removal:
            wait(lambda: not get(f"//sys/cluster_nodes/{node}/@pending_restart"), sleep_backoff=1.0)
            assert remove_maintenance("cluster_node", node, type_="pending_restart") == {node: {}}
        else:
            assert remove_maintenance("cluster_node", node, type_="pending_restart") == {node: {"pending_restart": 4}}

    @authors("danilalexeev")
    def test_auto_remove_pending_restart2(self):
        set("//sys/@config/node_tracker/pending_restart_lease_timeout", 2000)

        nodes = ls("//sys/cluster_nodes")

        maintenance_ids = {}
        for node in nodes:
            maintenance_ids.update(add_maintenance("cluster_node", node, "pending_restart", ""))

        for _ in range(4):
            for node in nodes:
                remove_maintenance("cluster_node", node, id=maintenance_ids[node])
                maintenance_ids.update(add_maintenance("cluster_node", node, "pending_restart", ""))

        for node in nodes:
            assert get(f"//sys/cluster_nodes/{node}/@pending_restart")

        for node in nodes:
            wait(lambda: not get(f"//sys/cluster_nodes/{node}/@pending_restart"), sleep_backoff=1.0)


##################################################################


class TestPendingRestartNodeDisposal(TestNodePendingRestartBase):
    ENABLE_MULTIDAEMON = False  # There are specific component kills.
    DELTA_NODE_CONFIG = {
        "data_node": {
            "master_connector": {
                "delay_before_full_heartbeat_report": 4000,
            },
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
        },
    }

    @authors("danilalexeev")
    def test_no_missing_replicas_erasure(self):
        set("//sys/@config/chunk_manager/disposed_pending_restart_node_chunk_refresh_delay", 10000)

        create("table", "//tmp/t", attributes={"erasure_codec": "reed_solomon_3_3"})
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 6)

        nodes = get(f"#{chunk_id}/@stored_replicas")[:2]
        node_indexes = [get("//sys/cluster_nodes/{}/@annotations/yt_env_index".format(node)) for node in nodes]

        maintenance_ids = {}
        for node in nodes:
            maintenance_ids.update(add_maintenance("cluster_node", node, "pending_restart", ""))

        sleep(0.5)

        status = get("#" + chunk_id + "/@replication_status/default")
        assert status["temporarily_unavailable"]
        assert not status["data_missing"]
        assert not status["parity_missing"]

        self.Env.kill_service("node", indexes=node_indexes)
        self.Env.start_nodes()

        # explicit statistics
        def check1():
            status = get("#" + chunk_id + "/@replication_status/default")
            return (
                not status["temporarily_unavailable"] and
                (status["data_missing"] or status["parity_missing"])
            )
        wait(check1)

        # general statistics
        assert chunk_id in ls("//sys/replica_temporarily_unavailable_chunks")
        assert len(ls("//sys/data_missing_chunks")) == 0
        assert len(ls("//sys/parity_missing_chunks")) == 0

        for node in nodes:
            wait(lambda: get(f"//sys/cluster_nodes/{node}/@state") == "online")
            remove_maintenance("cluster_node", node, id=maintenance_ids[str(node)])

        # explicit statistics
        status = get("#" + chunk_id + "/@replication_status/default")
        assert not status["temporarily_unavailable"]
        assert not status["data_missing"]
        assert not status["parity_missing"]
        assert not status["overreplicated"]

        # general statistics
        assert chunk_id in ls("//sys/replica_temporarily_unavailable_chunks")
        assert len(ls("//sys/data_missing_chunks")) == 0
        assert len(ls("//sys/parity_missing_chunks")) == 0

        wait(lambda: chunk_id not in ls("//sys/replica_temporarily_unavailable_chunks"))


##################################################################


class TestChunkServerMulticell(TestChunkServer):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_validate_chunk_host_cell_role1(self):
        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["cypress_node_host"]}})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external": True, "external_cell_tag": 11})

    @authors("aleksandra-zh")
    def test_validate_chunk_host_cell_role2(self):
        set("//sys/@config/multicell_manager/remove_secondary_cell_default_roles", True)
        set("//sys/@config/multicell_manager/cell_descriptors", {})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"external": True, "external_cell_tag": 11})

        set("//sys/@config/multicell_manager/remove_secondary_cell_default_roles", False)
        create("table", "//tmp/t", attributes={"external": True, "external_cell_tag": 11})

    @authors("aleksandra-zh")
    def test_multicell_with_primary_chunk_host(self):
        set("//sys/@config/multicell_manager/remove_secondary_cell_default_roles", True)
        set("//sys/@config/multicell_manager/cell_descriptors", {"10": {"roles": ["cypress_node_host", "chunk_host"]}})

        create("table", "//tmp/t")

    @authors("babenko")
    def test_owning_nodes3(self):
        create("table", "//tmp/t0", attributes={"external": False})
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})

        write_table("//tmp/t1", {"a": "b"})

        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t0")
        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t2")

        chunk_ids0 = get("//tmp/t0/@chunk_ids")
        chunk_ids1 = get("//tmp/t1/@chunk_ids")
        chunk_ids2 = get("//tmp/t2/@chunk_ids")

        assert chunk_ids0 == chunk_ids1
        assert chunk_ids1 == chunk_ids2
        chunk_ids = chunk_ids0
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert_items_equal(get("#" + chunk_id + "/@owning_nodes"), ["//tmp/t0", "//tmp/t1", "//tmp/t2"])

    @authors("babenko")
    def test_chunk_requisition_registry_orchid(self):
        pass

    @authors("h0pless")
    def test_dedicated_chunk_host_role_alert(self):
        def has_alert(msg):
            for alert in get("//sys/@master_alerts"):
                if msg in str(alert):
                    return True
            return False

        set("//sys/@config/multicell_manager/cell_descriptors",
            {"11": {"roles": ["chunk_host", "dedicated_chunk_host"]}})
        wait(lambda: has_alert('Cell received conflicting "chunk_host" and "dedicated_chunk_host" roles'))

        set("//sys/@config/multicell_manager/cell_descriptors",
            {"11": {"roles": ["chunk_host"]}})
        wait(lambda: not has_alert('Cell received conflicting "chunk_host" and "dedicated_chunk_host" roles'))

    @authors("h0pless")
    def test_dedicated_chunk_host_roles_only(self):
        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["dedicated_chunk_host"]},
            "12": {"roles": ["dedicated_chunk_host"]}})

        with pytest.raises(YtError, match="No secondary masters with a chunk host role were found"):
            create("table", "//tmp/t", attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                    {"name": "c", "type": "int64"}
                ]})

    @authors("h0pless")
    def test_dedicated_chunk_host_role(self):
        dedicated_host_cell_tag = 11
        chunk_host_cell_tag = 12

        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["dedicated_chunk_host"]},
            "12": {"roles": ["chunk_host"]}})

        create("table", "//tmp/t1", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ],
            "external_cell_tag": dedicated_host_cell_tag})

        create("table", "//tmp/t2", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ],
            "external_cell_tag": chunk_host_cell_tag})

        create("table", "//tmp/t3", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ]})

        create_tablet_cell_bundle("b1")
        create_tablet_cell_bundle("b2")

        set("//sys/tablet_cell_bundles/b1/@options/changelog_external_cell_tag", dedicated_host_cell_tag)
        set("//sys/tablet_cell_bundles/b2/@options/changelog_external_cell_tag", chunk_host_cell_tag)

        sync_create_cells(2, tablet_cell_bundle="b1")
        sync_create_cells(2, tablet_cell_bundle="b2")

        multicell_sleep()

        tablet_cells = ls("//sys/tablet_cells")
        for cell in tablet_cells:
            if (get("//sys/tablet_cells/{0}/@tablet_cell_bundle".format(cell)) == "b1"):
                expected_cell_tag = dedicated_host_cell_tag
            else:
                expected_cell_tag = chunk_host_cell_tag
            changelogs = ls("//sys/tablet_cells/{0}/changelogs".format(cell))
            assert changelogs
            for changelog in changelogs:
                changelog_path = "//sys/tablet_cells/{0}/changelogs/{1}".format(cell, changelog)
                is_external = get("{0}/@external".format(changelog_path))
                if is_external:
                    assert get("{0}/@external_cell_tag".format(changelog_path)) == expected_cell_tag
                else:
                    assert get("{0}/@native_cell_tag".format(changelog_path)) == expected_cell_tag
                for chunk_id in get("{0}/@chunk_ids".format(changelog_path)):
                    assert get("#{}/@native_cell_tag".format(chunk_id)) == expected_cell_tag

    @authors("koloshmet")
    def test_lost_vital_chunks_sample(self):
        pytest.skip()

        set("//sys/@config/chunk_manager/lost_vital_chunks_sample_update_period", 1000)

        create("table", "//tmp/t0", attributes={"external": False})
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        create("table", "//tmp/t3", attributes={"external_cell_tag": 11})

        write_table("//tmp/t0", {"a": "b"})
        write_table("//tmp/t1", {"c": "d"})
        write_table("//tmp/t2", {"e": "f"})
        write_table("//tmp/t3", {"g": "h"})

        chunk_id0 = get_singular_chunk_id("//tmp/t0")
        chunk_id1 = get_singular_chunk_id("//tmp/t1")
        chunk_id2 = get_singular_chunk_id("//tmp/t2")
        chunk_id3 = get_singular_chunk_id("//tmp/t3")

        nodes = list(get("//sys/cluster_nodes"))
        for node in nodes:
            set_node_banned(node, True)

        wait(lambda: get("//sys/@lost_vital_chunk_count") == 4)
        wait(lambda: get("//sys/lost_vital_chunks_sample/@count") == 4)
        lost_vital_chunks_sample = get("//sys/lost_vital_chunks_sample")
        assert chunk_id0 in lost_vital_chunks_sample
        assert chunk_id1 in lost_vital_chunks_sample
        assert chunk_id2 in lost_vital_chunks_sample
        assert chunk_id3 in lost_vital_chunks_sample

        set("//sys/@config/chunk_manager/max_lost_vital_chunks_sample_size_per_cell", 1)

        wait(lambda: get("//sys/lost_vital_chunks_sample/@count") == 3)
        assert get("//sys/@lost_vital_chunk_count") == 4
        lost_vital_chunks_sample = get("//sys/lost_vital_chunks_sample")
        assert chunk_id0 in lost_vital_chunks_sample and chunk_id2 in lost_vital_chunks_sample
        assert chunk_id1 in lost_vital_chunks_sample or chunk_id3 in lost_vital_chunks_sample

        for node in nodes:
            set_node_banned(node, False)

        wait(lambda: get("//sys/@lost_vital_chunk_count") == 0)
        wait(lambda: get("//sys/lost_vital_chunks_sample/@count") == 0)


##################################################################


class TestChunkServerReplicaRemoval(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 3
    NUM_NODES = 8

    DELTA_NODE_CONFIG = {
        "data_node": {
            "disk_health_checker": {
                "check_period": 1000,
            },
        },
    }

    def _wait_for_replicas_removal(self, path, service_to_restart):
        chunk_id = get_singular_chunk_id(path)
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) > 0)
        node = get("#{0}/@stored_replicas".format(chunk_id))[0]

        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

        set(
            "//sys/cluster_nodes/{0}/@resource_limits_overrides/removal_slots".format(node),
            0,
        )

        remove(path)
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) > 0)

        with Restarter(self.Env, service_to_restart):
            pass

        remove("//sys/cluster_nodes/{0}/@resource_limits_overrides/removal_slots".format(node))
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

    @authors("aleksandra-zh", "danilalexeev")
    @pytest.mark.parametrize("service_to_restart", [NODES_SERVICE, MASTERS_SERVICE])
    @pytest.mark.parametrize("wait_for_barrier", [True, False])
    def test_chunk_replica_removal(self, service_to_restart, wait_for_barrier):
        update_nodes_dynamic_config({
            "data_node": {
                "remove_chunk_job" : {
                    "wait_for_incremental_heartbeat_barrier": wait_for_barrier,
                }
            }
        })
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        self._wait_for_replicas_removal("//tmp/t", service_to_restart)

    @authors("aleksandra-zh")
    def test_nonexistent_chunk_replica_removal(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) > 0)
        node = get("#{0}/@stored_replicas".format(chunk_id))[0]

        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

        set("//sys/@config/chunk_manager/testing/disable_removing_replicas_from_destroyed_queue", True)

        remove("//tmp/t")
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) > 0)

        sleep(1)

        set("//sys/@config/chunk_manager/testing/disable_removing_replicas_from_destroyed_queue", False)
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

    @authors("aleksandra-zh", "danilalexeev")
    @pytest.mark.parametrize("service_to_restart", [NODES_SERVICE, MASTERS_SERVICE])
    @pytest.mark.parametrize("wait_for_barrier", [True, False])
    def test_journal_chunk_replica_removal(self, service_to_restart, wait_for_barrier):
        update_nodes_dynamic_config({
            "data_node": {
                "remove_chunk_job" : {
                    "wait_for_incremental_heartbeat_barrier": wait_for_barrier,
                }
            }
        })
        create("journal", "//tmp/j")
        write_journal("//tmp/j", [{"payload": "payload" + str(i)} for i in range(0, 10)])

        self._wait_for_replicas_removal("//tmp/j", service_to_restart)


class TestChunkServerReplicaRemovalMulticell(TestChunkServerReplicaRemoval):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1


##################################################################


@pytest.mark.enabled_multidaemon
class TestLastFinishedJobStoreLimit(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 4

    @authors("kvk1920")
    @pytest.mark.timeout(150)
    @pytest.mark.parametrize("limit", [0, 1, 10])
    def test_last_finished_job_limit(self, limit):
        set("//sys/@config/chunk_manager/finished_jobs_queue_size", limit)

        finished_jobs = []

        for table_num in range(limit + 1):
            table = "//tmp/t{}".format(table_num)
            create("table", table)
            write_table(table, {"a": "b"})
            chunk_id = get_singular_chunk_id(table)

            def check_if_job_finished():
                if len(get("#{}/@stored_replicas".format(chunk_id))) != 3:
                    return False
                jobs = get("//sys/chunks/{}/@jobs".format(chunk_id))
                if (0 == limit and 0 == len(jobs)) or (1 == len(jobs) and "completed" == jobs[0]["state"]):
                    if jobs:
                        finished_jobs.append(jobs[0])
                    return True
                else:
                    return False

            wait(check_if_job_finished)

        if limit == 0:
            assert len(finished_jobs) == 0
        else:
            chunk_id = get_singular_chunk_id("//tmp/t0")
            assert finished_jobs[0] not in get("//sys/chunks/{}/@jobs".format(chunk_id))


##################################################################


@pytest.mark.enabled_multidaemon
class TestMultipleErasurePartsPerNode(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("babenko")
    def test_allow_multiple_erasure_parts_per_node(self):
        create("table", "//tmp/t", attributes={"erasure_codec": "reed_solomon_3_3"})
        rows = [{"a": "b"}]
        write_table("//tmp/t", rows)
        assert read_table("//tmp/t") == rows

        chunk_id = get_singular_chunk_id("//tmp/t")

        status = get("#" + chunk_id + "/@replication_status/default")
        assert not status["data_missing"]
        assert not status["parity_missing"]
        assert not status["overreplicated"]
        assert not status["underreplicated"]


##################################################################


@pytest.mark.enabled_multidaemon
class TestConsistentChunkReplicaPlacementBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 10
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "consistent_replica_placement": {
                "enable": True,
                "token_redistribution_period": 500,
            },
        },
    }

    def _mount_insert_unmount(self, table_path, row):
        sync_mount_table(table_path)
        insert_rows(table_path, [row])
        sync_unmount_table(table_path)

    def _create_table_with_two_consistently_placed_chunks(self, table_path, attributes=None):
        if attributes is None:
            attributes = {}

        schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ]

        attributes["dynamic"] = True
        attributes["enable_consistent_chunk_replica_placement"] = True
        attributes["schema"] = schema

        sync_create_cells(1)
        create("table", table_path, attributes=attributes)

        row1 = {"key": "k1", "value": "v1"}
        self._mount_insert_unmount(table_path, row1)

        row2 = {"key": "k2", "value": "v2"}
        self._mount_insert_unmount(table_path, row2)

        chunk_ids = get("{}/@chunk_ids".format(table_path))
        assert len(chunk_ids) == 2

        assert get("#{}/@consistent_replica_placement_hash".format(chunk_ids[0])) == \
            get("#{}/@consistent_replica_placement_hash".format(chunk_ids[1]))

    def _disable_token_redistribution(self):
        old_period = get("//sys/@config/chunk_manager/consistent_replica_placement/token_redistribution_period")
        remove("//sys/@config/chunk_manager/consistent_replica_placement/token_redistribution_period")
        sleep(old_period / 1000.0)

    def _are_chunk_replicas_collocated(self, *stored_replicas):
        def by_index_then_node_address(replica):
            # NB: -1 is for regular chunks.
            # NB: media are ignored.
            return (replica.attributes.get("index", -1), str(replica))

        def to_addresses(replicas):
            return [str(replica) for replica in replicas]

        stored_replicas = [to_addresses(sorted(replicas, key=by_index_then_node_address)) for replicas in stored_replicas]

        return all(str(replicas) == str(stored_replicas[0]) for replicas in stored_replicas[1:])

    def _are_chunks_collocated(self, chunk_ids):
        stored_replicas = [get("#{}/@stored_replicas".format(chunk_id)) for chunk_id in chunk_ids]
        return self._are_chunk_replicas_collocated(stored_replicas)


##################################################################


class TestConsistentChunkReplicaPlacement(TestConsistentChunkReplicaPlacementBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("shakurov")
    def test_token_count_attribute(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement", {
            "token_distribution_bucket_count": 10,
            "tokens_per_node": 7,
            "token_redistribution_period": 500
        })
        sleep(1.5)

        for node in ls("//sys/cluster_nodes", attributes=["consistent_replica_placement_token_count"]):
            token_count = node.attributes["consistent_replica_placement_token_count"]["default"]
            assert token_count >= 7
            assert token_count <= 70

    @authors("shakurov")
    def test_regular(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        assert get("//tmp/t1/@enable_consistent_chunk_replica_placement")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    def test_chunk_destruction(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t4")
        chunk_ids = get("//tmp/t4/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        remove("//tmp/t4")

        # Must not crash here.
        gc_collect()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t4")
        chunk_ids = get("//tmp/t4/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    @pytest.mark.parametrize("trouble_mode", ["ban", "decommission", "disable_write_sessions"])
    def test_node_trouble(self, trouble_mode):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t5")
        chunk_ids = get("//tmp/t5/@chunk_ids")

        troubled_node = get("#{}/@stored_replicas".format(chunk_ids[0]))[1]
        add_maintenance("cluster_node", troubled_node, trouble_mode, "test node trouble")

        def are_chunks_collocated():
            chunk0_replicas = get("#{}/@stored_replicas".format(chunk_ids[0]))
            if len(chunk0_replicas) != 3:
                return False

            if troubled_node in chunk0_replicas:
                return False  # Wait for banning/decommissioning to take effect.

            chunk1_replicas = get("#{}/@stored_replicas".format(chunk_ids[1]))

            if len(chunk1_replicas) != 3:
                return False

            if troubled_node in chunk1_replicas:
                return False  # Wait for banning/decommissioning to take effect.

            return self._are_chunk_replicas_collocated(chunk0_replicas, chunk1_replicas)

        # Decommissioning may take some time.
        wait(are_chunks_collocated, iter=30, sleep_backoff=1.0)

    @authors("shakurov")
    @pytest.mark.parametrize("trouble_mode", ["ban", "decommission", "disable_write_sessions"])
    def test_troubled_node_restart(self, trouble_mode):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t5")
        chunk_ids = get("//tmp/t5/@chunk_ids")

        troubled_node = str(get("#{}/@stored_replicas".format(chunk_ids[0]))[1])
        maintenance_id = add_maintenance("cluster_node", troubled_node, trouble_mode, "test roubled node restart")[troubled_node]

        def are_chunks_collocated(troubled_node_ok):
            chunk0_replicas = get("#{}/@stored_replicas".format(chunk_ids[0]))

            if len(chunk0_replicas) != 3:
                return False

            if not troubled_node_ok and (troubled_node in chunk0_replicas):
                return False  # Wait for banning/decommissioning to take effect.

            chunk1_replicas = get("#{}/@stored_replicas".format(chunk_ids[1]))

            if len(chunk1_replicas) != 3:
                return False

            if not troubled_node_ok and (troubled_node in chunk1_replicas):
                return False  # Wait for banning/decommissioning to take effect.

            return self._are_chunk_replicas_collocated(chunk0_replicas, chunk1_replicas)

        wait(lambda: are_chunks_collocated(False), iter=120, sleep_backoff=1.0)

        with Restarter(self.Env, NODES_SERVICE):
            if trouble_mode == "ban":
                # Otherwise the node won't restart.
                remove_maintenance("cluster_node", troubled_node, id=maintenance_id)

        wait(lambda: are_chunks_collocated(True), iter=120, sleep_backoff=1.0)

    @authors("shakurov")
    def test_rf_change(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")
        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    def _pull_replication_queues_empty(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            if get("//sys/cluster_nodes/{}/@pull_replication_queue_size".format(node)) > 0:
                return False
            if get("//sys/cluster_nodes/{}/@pull_replication_chunk_count".format(node)) > 0:
                return False
        return True

    @authors("aleksandra-zh")
    def test_crp_pull_replication_simple(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable_pull_replication", True)
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")

        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)

        wait(self._pull_replication_queues_empty)

    @authors("aleksandra-zh")
    def test_crp_pull_replication_dead_chunk(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable_pull_replication", True)
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")

        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)

        set("//tmp/t6/@replication_factor", 7)
        remove("//tmp/t6")

        wait(self._pull_replication_queues_empty)

    @authors("shakurov")
    def test_rf_change_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t7")
        chunk_ids = get("//tmp/t7/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        wait(lambda: self._are_chunk_replicas_collocated(
            get("#{}/@stored_replicas".format(chunk_ids[0])),
            get("#{}/@consistent_replica_placement".format(chunk_ids[0]))))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        set("//tmp/t7/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t7/@replication_factor", 3)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        wait(lambda: self._are_chunk_replicas_collocated(
            get("#{}/@stored_replicas".format(chunk_ids[0])),
            get("#{}/@consistent_replica_placement".format(chunk_ids[0]))))

        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)

    @authors("shakurov")
    def test_disable_enable(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", False)
        assert len(ls("//sys/inconsistently_placed_chunks")) == 0

        self._create_table_with_two_consistently_placed_chunks("//tmp/t8")
        chunk_ids = get("//tmp/t8/@chunk_ids")

        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", True)

        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    def test_enable_disable(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", True)

        self._create_table_with_two_consistently_placed_chunks("//tmp/t9")
        chunk_ids = get("//tmp/t9/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", False)
        assert len(ls("//sys/inconsistently_placed_chunks")) == 0

    @authors("shakurov")
    def test_replica_count_change(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t10")
        chunk_ids_1 = get("//tmp/t10/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids_1[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids_1))

        self._create_table_with_two_consistently_placed_chunks("//tmp/t11", {"replication_factor": 6})
        chunk_ids_2 = get("//tmp/t11/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids_2[0]))) == 6)
        wait(lambda: self._are_chunks_collocated(chunk_ids_2))

        set("//sys/@config/chunk_manager/consistent_replica_placement/replicas_per_chunk", 50)
        wait(lambda: self._are_chunks_collocated(chunk_ids_1))
        wait(lambda: self._are_chunks_collocated(chunk_ids_2))


##################################################################


class TestConsistentChunkReplicaPlacementSnapshotLoading(TestConsistentChunkReplicaPlacementBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    @authors("shakurov")
    def test_after_snapshot_loading_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(lambda: self._are_chunks_collocated(chunk_ids))
        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)


##################################################################


class TestConsistentChunkReplicaPlacementLeaderSwitch(TestConsistentChunkReplicaPlacementBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    @authors("shakurov")
    def test_leader_switch_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        old_leader_rpc_address = get_active_primary_master_leader_address(self)
        new_leader_rpc_address = get_active_primary_master_follower_address(self)
        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_rpc_address)
        wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))

        wait(lambda: self._are_chunks_collocated(chunk_ids))
        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)


##################################################################


class TestChunkWeightStatisticsHistogram(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3

    def _validate_chunk_weight_statistic_histograms(self, histogram_name, attribute_name):
        chunk_ids = get("//tmp/t/@chunk_ids")

        bounds = [1, 2, 4, 8, 16, 32, 64, 125, 250, 500]
        for i in range(0, 28):
            bounds.append(bounds[i] * 1000)

        def find_correct_bucket(bounds, value):
            if value <= bounds[0]:
                return 0

            for i in range(1, len(bounds)):
                if bounds[i - 1] < value <= bounds[i]:
                    return i

            return len(bounds)

        def validate_histogram(histogram_name, attribute_name, bounds, chunk_ids):
            result = []
            for i in range(0, 39):
                result.append(0)

            for chunk_id in chunk_ids:
                value = get("#{0}/@{1}".format(chunk_id, attribute_name))
                result[find_correct_bucket(bounds, value)] += 1

            master_address = ls("//sys/primary_masters")[0]
            profiler = profiler_factory().at_primary_master(master_address)
            histogram = profiler.histogram("chunk_server/histograms/" + histogram_name)

            histogram_bins = histogram.get_bins()
            for i in range(0, 39):
                assert int(histogram_bins[i]['count']) == result[i]

        validate_histogram(histogram_name, attribute_name, bounds, chunk_ids)

    def _check_chunk_weight_statistics_snapshot(self):
        create("table", "//tmp/t")
        value1 = ""
        for i in range(0, 1000):
            value1 += "a"
        value2 = ""
        for i in range(0, 500):
            value2 += "a"
        value3 = ""
        for i in range(0, 50):
            value3 += "a"

        write_table("<append=%true>//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"a": value1})
        write_table("<append=%true>//tmp/t", {"a": value2})
        write_table("<append=%true>//tmp/t", {"a": value3})
        write_table("<append=%true>//tmp/t", {"a": "c"})
        write_table("<append=%true>//tmp/t", {"a": "d"})
        write_table("<append=%true>//tmp/t", {"a": "e"})

        yield

        sleep(10)

        self._validate_chunk_weight_statistic_histograms("chunk_row_count_histogram", "row_count")
        self._validate_chunk_weight_statistic_histograms("chunk_compressed_data_size_histogram", "compressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_uncompressed_data_size_histogram", "uncompressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_data_weight_histogram", "data_weight")

    @authors("h0pless")
    def test_chunk_weight_statistics_histogram(self):
        create("table", "//tmp/t")
        value1 = ""
        for i in range(0, 1000):
            value1 += "a"
        value2 = ""
        for i in range(0, 500):
            value2 += "a"
        value3 = ""
        for i in range(0, 50):
            value3 += "a"

        write_table("<append=%true>//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"a": value1})
        write_table("<append=%true>//tmp/t", {"a": value2})
        write_table("<append=%true>//tmp/t", {"a": value3})
        write_table("<append=%true>//tmp/t", {"a": "c"})
        write_table("<append=%true>//tmp/t", {"a": "d"})
        write_table("<append=%true>//tmp/t", {"a": "e"})

        sleep(10)

        self._validate_chunk_weight_statistic_histograms("chunk_row_count_histogram", "row_count")
        self._validate_chunk_weight_statistic_histograms("chunk_compressed_data_size_histogram", "compressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_uncompressed_data_size_histogram", "uncompressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_data_weight_histogram", "data_weight")

    @authors("h0pless")
    def test_chunk_weight_statistics_histogram_snapshots(self):

        checker_state = iter(self._check_chunk_weight_statistics_snapshot())

        next(checker_state)

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        with pytest.raises(StopIteration):
            next(checker_state)


##################################################################


@pytest.mark.enabled_multidaemon
class TestChunkCreationThrottler(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("h0pless")
    def test_per_user_bytes_throttler_root(self):
        with raises_yt_error("Cannot set \"chunk_service_request_bytes_throttler\" for \"root\""):
            set("//sys/users/root/@chunk_service_request_bytes_throttler", {"limit": 1337})

    @authors("h0pless")
    @with_additional_threads
    def test_per_user_bytes_throttler_profiling(self):
        user_name = "GregorzBrzeczyszczykiewicz"
        create_user(user_name)

        create("table", "//tmp/t")

        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", True)
        set(f"//sys/users/{user_name}/@chunk_service_request_bytes_throttler", {"limit": 300})
        sleep(1)

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)
        value_counter = profiler.counter("chunk_service/bytes_throttler/value", tags={"user": user_name, "method": "create_chunk"})

        def wait_for_counter_to_change():
            wait(lambda: value_counter.get() > 0, ignore_exceptions=True)

        checker = self.spawn_additional_thread(name="wait for profiling counter to change",
                                               target=wait_for_counter_to_change)

        write_table("//tmp/t", {"place": "gmina Grzmiszczoslawice"}, timeout=20, authenticated_user=user_name)
        write_table("//tmp/t", {"place": "powiat lekolody"}, timeout=20, authenticated_user=user_name)

        checker.join()


##################################################################


@pytest.mark.enabled_multidaemon
class TestChunkServerCypressIntegration(YTEnvSetup):
    ENABLE_MULTIDAEMON = True

    @authors("danilalexeev")
    def test_filtered_virtual_map_mutating_attribute_request(self):
        # Create lost chunk on purpose
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 8})
        chunk_id = get_singular_chunk_id("//tmp/t")
        node = get(f"#{chunk_id}/@stored_replicas")[0]
        set_node_banned(node, True)
        wait(lambda: chunk_id in get("//sys/lost_chunks"))

        with pytest.raises(YtError, match="Mutating request through virtual map is forbidden"):
            set(f"//sys/lost_chunks/{chunk_id}/@a", "a")

        # Should not raise
        set(f"//sys/chunks/{chunk_id}/@a", "a")
        assert get(f"//sys/chunks/{chunk_id}/@a") == "a"
