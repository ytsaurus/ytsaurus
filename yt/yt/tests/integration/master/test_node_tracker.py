from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, MASTERS_SERVICE

from yt_commands import (
    authors, wait, execute_command, get_driver, build_snapshot,
    exists, ls, get, set, create, remove,
    write_table, update_nodes_dynamic_config,
    create_rack, create_data_center, vanilla,
    build_master_snapshots, set_node_banned,
    add_maintenance, remove_maintenance,
    create_user, raises_yt_error, make_ace)

import yt_error_codes

from yt.common import YtError

import pytest

import builtins
from datetime import datetime
from time import sleep

import shutil
import os
import time

##################################################################


class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "tags": ["config_tag1", "config_tag2"],
        "exec_agent": {
            "slot_manager": {
                "slot_location_statistics_update_period": 100,
            },
        }
    }

    @authors("babenko")
    def test_ban(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get("//sys/cluster_nodes/%s/@state" % test_node) == "online"

        set_node_banned(test_node, True)
        set_node_banned(test_node, False)

    @authors("babenko")
    def test_resource_limits_overrides_defaults(self):
        node = ls("//sys/cluster_nodes")[0]
        assert get("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node)) == {}

    @authors("psushin")
    def test_disable_write_sessions(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        maintenances = {
            node: add_maintenance(node, "disable_write_sessions", "test")
            for node in nodes
        }

        def can_write():
            try:
                write_table("//tmp/t", {"a": "b"})
                return True
            except YtError:
                return False

        assert not can_write()

        for node, maintenance in maintenances.items():
            remove_maintenance(node, maintenance)

        wait(lambda: can_write())

    @authors("psushin", "babenko")
    def test_disable_scheduler_jobs(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(test_node)) > 0
        add_maintenance(test_node, "disable_scheduler_jobs", "test")

        wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(test_node)) == 0)

    @authors("babenko")
    def test_resource_limits_overrides_valiation(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError):
            remove("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node))

    @authors("babenko")
    def test_user_tags_validation(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError):
            set("//sys/cluster_nodes/{0}/@user_tags".format(node), 123)

    @authors("babenko")
    def test_user_tags_update(self):
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["user_tag"])
        assert get("//sys/cluster_nodes/{0}/@user_tags".format(node)) == ["user_tag"]
        assert "user_tag" in get("//sys/cluster_nodes/{0}/@tags".format(node))

    @authors("babenko")
    def test_config_tags(self):
        for node in ls("//sys/cluster_nodes"):
            tags = get("//sys/cluster_nodes/{0}/@tags".format(node))
            assert "config_tag1" in tags
            assert "config_tag2" in tags

    @authors("babenko")
    def test_rack_tags(self):
        create_rack("r")
        node = ls("//sys/cluster_nodes")[0]
        assert "r" not in get("//sys/cluster_nodes/{0}/@tags".format(node))
        set("//sys/cluster_nodes/{0}/@rack".format(node), "r")
        assert "r" in get("//sys/cluster_nodes/{0}/@tags".format(node))
        remove("//sys/cluster_nodes/{0}/@rack".format(node))
        assert "r" not in get("//sys/cluster_nodes/{0}/@tags".format(node))

    @authors("babenko", "shakurov")
    def test_create_cluster_node(self):
        kwargs = {"type": "cluster_node"}
        with pytest.raises(YtError):
            execute_command("create", kwargs)

    @authors("gritukan")
    def test_node_decommissioned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        create("table", "//tmp/t")

        def can_write():
            try:
                write_table("//tmp/t", {"a": "b"})
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        maintenances = {
            node: add_maintenance(node, "decommission", "test")
            for node in nodes
        }

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) == 0)
        wait(lambda: not can_write())

        for node, maintenance in maintenances.items():
            remove_maintenance(node, maintenance)

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

    @authors("gritukan")
    @pytest.mark.skip("Per-slot location statistics can be heavy; enable after switching to new heartbeats")
    def test_slot_location_statistics(self):
        def get_max_slot_space_usage():
            max_slot_space_usage = -1
            for node in ls("//sys/cluster_nodes"):
                slot_locations_path = "//sys/cluster_nodes/{}/@statistics/slot_locations".format(node)
                statistics = get(slot_locations_path)
                for index in range(len(statistics)):
                    slot_space_usages = get(slot_locations_path + "/{}/slot_space_usages".format(index))
                    for slot_space_usage in slot_space_usages:
                        max_slot_space_usage = max(max_slot_space_usage, slot_space_usage)
            return max_slot_space_usage

        assert get_max_slot_space_usage() == 0
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "my_task": {
                        "job_count": 1,
                        "command": "fallocate -l 10M my_file; sleep 3600",
                    }
                }
            }
        )

        wait(lambda: 10 * 1024 * 1024 <= get_max_slot_space_usage() <= 12 * 1024 * 1024)
        op.abort()

    @authors("akozhikhov")
    def test_memory_category_limits(self):
        node = ls("//sys/cluster_nodes")[0]

        memory_statistics = get("//sys/cluster_nodes/{}/@statistics/memory".format(node))
        assert "limit" not in memory_statistics["alloc_fragmentation"]
        assert memory_statistics["block_cache"]["limit"] == 2000000

    @authors("ignat")
    def test_cpu_statistics(self):
        node = ls("//sys/cluster_nodes")[0]

        cpu_statistics = get("//sys/cluster_nodes/{}/@statistics/cpu".format(node))
        assert cpu_statistics["tablet_slots"] == 0.0
        assert cpu_statistics["dedicated"] == 2.0
        assert cpu_statistics["jobs"] > 0.0

    @authors("capone212")
    def test_io_statistics(self):
        create("table", "//tmp/t")

        for parts in range(10):
            row_count = 1024
            write_table("<append=%true>//tmp/t", [{"a": i} for i in range(row_count)])

        update_nodes_dynamic_config({
            "data_node": {
                "io_throughput_meter": {
                    "enabled": True,
                    "time_between_tests": 1000,
                    "estimate_time_limit": 10000,
                    "mediums": [
                        {
                            "medium_name": "default",
                            "enabled": True,
                            "packet_size": 4096,
                            "read_to_write_ratio": 75,
                            "writer_count": 2,
                            "max_write_file_size": 1024 * 1024,
                            "wait_after_congested": 100,
                            "segment_size": 5,
                            "simulated_request_latency": 200,
                            "use_direct_io": False,
                            "verification_window_period" : 100,
                            "congestion_detector": {
                                "probes_interval": 50,
                            }
                        }
                    ]
                },
            }
        })

        wait(lambda: get("//sys/cluster_nodes/@io_statistics")["filesystem_write_rate"] > 0)
        per_medium_io = get("//sys/cluster_nodes/@io_statistics_per_medium")
        assert "default" in per_medium_io
        assert per_medium_io["default"]["filesystem_write_rate"] > 0

        wait(lambda: get("//sys/cluster_nodes/@io_statistics")["disk_read_capacity"] > 0, timeout=60)

    @authors("gritukan")
    def test_do_not_unregister_on_master_update(self):
        lease_txs = {}
        for node in ls("//sys/cluster_nodes"):
            lease_tx = get("//sys/cluster_nodes/{}/@lease_transaction_id".format(node))
            lease_txs[node] = lease_tx

        # Build snapshots with read only and wait a bit.
        build_master_snapshots(set_read_only=True)
        time.sleep(3)

        # Shutdown masters and wait a bit.
        with Restarter(self.Env, MASTERS_SERVICE):
            time.sleep(3)

        # Wait a bit after "update".
        time.sleep(3)

        # Nodes should not reregister.
        for node in ls("//sys/cluster_nodes"):
            lease_tx = get("//sys/cluster_nodes/{}/@lease_transaction_id".format(node))
            assert lease_txs[node] == lease_tx


##################################################################


class TestNodeTrackerMulticell(TestNodeTracker):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("shakurov")
    def test_tag_replication(self):
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags/end".format(node), "cool_tag")

        create_rack("r0")
        create_data_center("d0")
        set("//sys/racks/r0/@data_center", "d0")
        set("//sys/cluster_nodes/{0}/@rack".format(node), "r0")

        tags = {tag for tag in get("//sys/cluster_nodes/{0}/@tags".format(node))}
        assert "cool_tag" in tags
        assert "r0" in tags
        assert "d0" in tags

        tags1 = {tag for tag in get("//sys/cluster_nodes/{0}/@tags".format(node), driver=get_driver(1))}
        assert tags1 == tags

        tags2 = {tag for tag in get("//sys/cluster_nodes/{0}/@tags".format(node), driver=get_driver(2))}
        assert tags2 == tags2


################################################################################


class TestRemoveClusterNodes(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
            "register_timeout": 1000,
            "incremental_heartbeat_timeout": 1000,
            "full_heartbeat_timeout": 1000,
            "job_heartbeat_timeout": 1000,
        }
    }

    @authors("babenko")
    def test_remove_nodes(self):
        for _ in range(10):
            with Restarter(self.Env, NODES_SERVICE):
                for node in ls("//sys/cluster_nodes"):
                    wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
                    id = get("//sys/cluster_nodes/{}/@id".format(node))
                    remove("//sys/cluster_nodes/" + node)
                    wait(lambda: not exists("#" + id))

            build_snapshot(cell_id=None)

            with Restarter(self.Env, MASTERS_SERVICE):
                pass


################################################################################


class TestNodeUnrecognizedOptionsAlert(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "enable_unrecognized_options_alert": True,
        "some_nonexistent_option": 42,
    }

    @authors("gritukan")
    def test_node_unrecognized_options_alert(self):
        nodes = ls("//sys/cluster_nodes")
        alerts = get("//sys/cluster_nodes/{}/@alerts".format(nodes[0]))
        assert alerts[0]["code"] == yt_error_codes.UnrecognizedConfigOption


################################################################################


class TestReregisterNode(YTEnvSetup):
    NUM_NODES = 2
    FIRST_CONFIG = None
    SECOND_CONFIG = None

    @classmethod
    def _change_node_address(cls):
        path1 = os.path.join(cls.FIRST_CONFIG["data_node"]["store_locations"][0]["path"], "uuid")
        path2 = os.path.join(cls.SECOND_CONFIG["data_node"]["store_locations"][0]["path"], "uuid")

        with Restarter(cls.Env, NODES_SERVICE, sync=False):
            shutil.copy(path1, path2)

    @classmethod
    def modify_node_config(cls, config):
        if (cls.FIRST_CONFIG is None):
            cls.FIRST_CONFIG = config
        else:
            cls.SECOND_CONFIG = config

    @authors("aleksandra-zh")
    def test_reregister_node_with_different_address(self):
        TestReregisterNode._change_node_address()

        def get_online_node_count():
            online_count = 0
            for node in ls("//sys/cluster_nodes", attributes=["state"]):
                if node.attributes["state"] == "online":
                    online_count += 1
            return online_count

        wait(lambda: get_online_node_count() == 1)
        sleep(5)
        assert get_online_node_count() == 1


################################################################################


class TestNodeMaintenance(YTEnvSetup):
    TEST_MAINTENANCE_FLAGS = True

    _KIND_TO_FLAG = {
        "ban": "banned",
        "decommission": "decommissioned",
        "disable_write_sessions": "disable_write_sessions",
        "disable_scheduler_jobs": "disable_scheduler_jobs",
        "disable_tablet_cells": "disable_tablet_cells",
    }

    @authors("kvk1920")
    def test_direct_flag_set(self):
        create_user("u1")
        create_user("u2")
        node = ls("//sys/cluster_nodes")[0]

        for kind, flag in self._KIND_TO_FLAG.items():
            for user in ["u1", "u2"]:
                add_maintenance(node, kind, f"maintenance by {user}", authenticated_user=user)
            set(f"//sys/cluster_nodes/{node}/@{flag}", True, authenticated_user="u1")
            maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")
            # Setting @{flag} %true removes all existing requests.
            assert len(maintenances) == 1
            ((maintenance_id, maintenance),) = maintenances.items()
            assert maintenance["maintenance_type"] == kind
            assert maintenance["user_name"] == "u1"
            ts = datetime.strptime(maintenance["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            assert (datetime.utcnow() - ts).seconds / 60 <= 30

            add_maintenance(node, kind, "another maintenance by u2", authenticated_user="u2")
            maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert len(maintenances) == 2
            m1, m2 = maintenances.values()
            if m1["user_name"] != "u1":
                m1, m2 = m2, m1
            assert m2["user_name"] == "u2"
            assert m2["comment"] == "another maintenance by u2"
            set(f"//sys/cluster_nodes/{node}/@{flag}", False)
            assert not get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert not get(f"//sys/cluster_nodes/{node}/@{flag}")

    @authors("kvk1920")
    def test_deprecation_message(self):
        set("//sys/@config/node_tracker/forbid_maintenance_attribute_writes", True)
        try:
            node = ls("//sys/cluster_nodes")[0]
            for flag in self._KIND_TO_FLAG.values():
                with raises_yt_error("deprecated"):
                    set(f"//sys/cluster_nodes/{node}/@{flag}", True)
        finally:
            set("//sys/@config/node_tracker/forbid_maintenance_attribute_writes", False)

    @authors("kvk1920")
    def test_add_remove(self):
        create_user("u1")
        create_user("u2")
        node = ls("//sys/cluster_nodes")[0]
        for kind, flag in self._KIND_TO_FLAG.items():
            m1 = add_maintenance(node, kind, "comment1", authenticated_user="u1")
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")
            m2 = add_maintenance(node, kind, "comment2", authenticated_user="u2")
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

            remove_maintenance(node, m1)
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

            ((m_id, m),) = get(f"//sys/cluster_nodes/{node}/@maintenance_requests").items()
            assert m_id == m2

            assert m["maintenance_type"] == kind
            assert m["comment"] == "comment2"
            assert m["user_name"] == "u2"
            ts = datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            assert (datetime.utcnow() - ts).seconds / 60 <= 30

            remove_maintenance(node, m2)
            assert not get(f"//sys/cluster_nodes/{node}/@{flag}")

    @authors("kvk1920")
    def test_mixing_types(self):
        node = ls("//sys/cluster_nodes")[0]
        for kind in self._KIND_TO_FLAG:
            add_maintenance(node, kind, kind)

        for flag in self._KIND_TO_FLAG.values():
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

        maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
        assert len(maintenances) == len(self._KIND_TO_FLAG)
        kinds = builtins.set(self._KIND_TO_FLAG)
        assert kinds == {req["maintenance_type"] for req in maintenances.values()}
        assert kinds == {req["comment"] for req in maintenances.values()}
        maintenance_ids = {req["maintenance_type"]: req_id for req_id, req in maintenances.items()}

        cleared_flags = builtins.set()

        def check_flags():
            for flag in self._KIND_TO_FLAG.values():
                assert get(f"//sys/cluster_nodes/{node}/@{flag}") == (flag not in cleared_flags)

        for kind, flag in self._KIND_TO_FLAG.items():
            check_flags()
            remove_maintenance(node, maintenance_ids[kind])
            cleared_flags.add(flag)
            check_flags()

    @authors("kvk1920")
    def test_access(self):
        create_user("u")

        node = ls("//sys/cluster_nodes")[0]

        old_acl = get("//sys/schemas/cluster_node/@acl")
        set("//sys/schemas/cluster_node/@acl", [make_ace("deny", "u", "write")])
        try:
            maintenance_id = add_maintenance(node, "ban", "ban by root")
            with raises_yt_error("Access denied"):
                add_maintenance(node, "ban", "ban by u", authenticated_user="u")
            with raises_yt_error("Access denied"):
                remove_maintenance(node, maintenance_id, authenticated_user="u")
        finally:
            set("//sys/schemas/cluster_node/@acl", old_acl)


################################################################################


class TestNodeMaintenanceMulticell(TestNodeMaintenance):
    NUM_SECONDARY_MASTER_CELLS = 2
