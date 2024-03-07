from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create_user, ls, get, add_maintenance, remove_maintenance,
    raises_yt_error, make_ace, set,
    create_host, remove_host,
    externalize,
    wait)

from yt.common import YtError

import pytest

import builtins
from contextlib import suppress
from datetime import datetime

################################################################################


class TestMaintenanceTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    TEST_MAINTENANCE_FLAGS = True
    NUM_RPC_PROXIES = 2
    NUM_HTTP_PROXIES = 2
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    DELTA_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
            "death_age": 500,
            "cypress_timeout": 50,
        },
    }

    DELTA_RPC_PROXY_CONFIG = {
        "discovery_service": {
            "proxy_update_period": 100
        },
    }

    _KIND_TO_FLAG = {
        "ban": "banned",
        "decommission": "decommissioned",
        "disable_write_sessions": "disable_write_sessions",
        "disable_scheduler_jobs": "disable_scheduler_jobs",
        "disable_tablet_cells": "disable_tablet_cells",
    }

    def teardown_method(self, method):
        for node in ls("//sys/cluster_nodes"):
            self._set_host(node, node)

        for host in ls("//sys/hosts"):
            with suppress(YtError):
                remove_host(host)
        super(TestMaintenanceTracker, self).teardown_method(method)

    def _set_host(self, node, host):
        set(f"//sys/cluster_nodes/{node}/@host", host)

    @authors("kvk1920")
    def test_direct_flag_set(self):
        create_user("u1")
        create_user("u2")
        node = ls("//sys/cluster_nodes")[0]

        for kind, flag in self._KIND_TO_FLAG.items():
            for user in ["u1", "u2"]:
                add_maintenance("cluster_node", node, kind, f"maintenance by {user}", authenticated_user=user)
            set(f"//sys/cluster_nodes/{node}/@{flag}", True, authenticated_user="u1")
            maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")
            # Setting @{flag} %true removes all existing requests.
            assert len(maintenances) == 1
            ((maintenance_id, maintenance),) = maintenances.items()
            assert maintenance["type"] == kind
            assert maintenance["user"] == "u1"
            ts = datetime.strptime(maintenance["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            assert (datetime.utcnow() - ts).seconds / 60 <= 30

            add_maintenance("cluster_node", node, kind, "another maintenance by u2", authenticated_user="u2")
            maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert len(maintenances) == 2
            m1, m2 = maintenances.values()
            if m1["user"] != "u1":
                m1, m2 = m2, m1
            assert m2["user"] == "u2"
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
            m1 = add_maintenance("cluster_node", node, kind, comment="comment1", authenticated_user="u1")[node]
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")
            m2 = add_maintenance("cluster_node", node, kind, comment="comment2", authenticated_user="u2")[node]
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

            assert remove_maintenance("cluster_node", node, id=m1) == {node: {kind: 1}}
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

            ((m_id, m),) = get(f"//sys/cluster_nodes/{node}/@maintenance_requests").items()
            assert m_id == m2

            assert m["type"] == kind
            assert m["comment"] == "comment2"
            assert m["user"] == "u2"
            ts = datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            assert (datetime.utcnow() - ts).seconds / 60 <= 30

            assert remove_maintenance("cluster_node", node, id=m2) == {node: {kind: 1}}
            assert not get(f"//sys/cluster_nodes/{node}/@{flag}")

    @authors("kvk1920")
    def test_mixing_types(self):
        node = ls("//sys/cluster_nodes")[0]
        for kind in self._KIND_TO_FLAG:
            add_maintenance("cluster_node", node, kind, comment=kind)

        for flag in self._KIND_TO_FLAG.values():
            assert get(f"//sys/cluster_nodes/{node}/@{flag}")

        maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
        assert len(maintenances) == len(self._KIND_TO_FLAG)
        kinds = builtins.set(self._KIND_TO_FLAG)
        assert kinds == {req["type"] for req in maintenances.values()}
        assert kinds == {req["comment"] for req in maintenances.values()}
        maintenance_ids = {req["type"]: req_id for req_id, req in maintenances.items()}

        cleared_flags = builtins.set()

        def check_flags():
            for flag in self._KIND_TO_FLAG.values():
                assert get(f"//sys/cluster_nodes/{node}/@{flag}") == (flag not in cleared_flags)

        for kind, flag in self._KIND_TO_FLAG.items():
            check_flags()
            assert remove_maintenance("cluster_node", node, id=maintenance_ids[kind]) == {
                node: {kind: 1}
            }
            cleared_flags.add(flag)
            check_flags()

    @authors("kvk1920")
    def test_access(self):
        create_user("u")

        node = ls("//sys/cluster_nodes")[0]

        old_acl = get("//sys/schemas/cluster_node/@acl")
        set("//sys/schemas/cluster_node/@acl", [make_ace("deny", "u", "write")])
        try:
            maintenance_id = add_maintenance("cluster_node", node, "ban", comment="ban by root")[node]
            with raises_yt_error("Access denied"):
                add_maintenance("cluster_node", node, "ban", comment="ban by u", authenticated_user="u")
            with raises_yt_error("Access denied"):
                remove_maintenance("cluster_node", node, id=maintenance_id, authenticated_user="u")
        finally:
            set("//sys/schemas/cluster_node/@acl", old_acl)

    @authors("kvk1920")
    def test_cannot_abort_lease_transaction(self):
        create_user("non_aborter")

        set("//sys/schemas/system_transaction/@acl/-1", {
            "action": "deny",
            "subjects": ["non_aborter"],
            "permissions": ["write"],
            "inheritance_mode": "object_and_descendants",
        })

        node = ls("//sys/cluster_nodes")[0]
        # Should not fail.
        set(f"//sys/cluster_nodes/{node}/@banned", True, authenticated_user="non_aborter")
        wait(lambda: get(f"//sys/cluster_nodes/{node}/@state") == "offline")

    @authors("kvk1920")
    def test_remove_all(self):
        node = ls("//sys/cluster_nodes")[0]
        path = f"//sys/cluster_nodes/{node}"

        add_maintenance("cluster_node", node, "disable_write_sessions", comment="comment1")
        add_maintenance("cluster_node", node, "decommission", comment="comment2")

        assert len(get(f"{path}/@maintenance_requests")) == 2
        assert get(f"{path}/@decommissioned")
        assert get(f"{path}/@disable_write_sessions")

        assert remove_maintenance("cluster_node", node, all=True) == {node: {
            "disable_write_sessions": 1,
            "decommission": 1,
        }}

        assert len(get(f"{path}/@maintenance_requests")) == 0
        assert not get(f"{path}/@decommissioned")
        assert not get(f"{path}/@disable_write_sessions")

    @authors("kvk1920")
    def test_remove_mine(self):
        create_user("u1")
        create_user("u2")

        node = ls("//sys/cluster_nodes")[0]
        path = f"//sys/cluster_nodes/{node}"

        add_maintenance("cluster_node", node, "disable_write_sessions", "comment1", authenticated_user="u1")
        add_maintenance("cluster_node", node, "disable_scheduler_jobs", "comment2", authenticated_user="u2")

        assert len(get(f"{path}/@maintenance_requests")) == 2

        assert remove_maintenance("cluster_node", node, mine=True, authenticated_user="u1") == {
            node: {"disable_write_sessions": 1}
        }
        assert [request["user"] for request in get(f"{path}/@maintenance_requests").values()] == ["u2"]

    @authors("kvk1920")
    def test_remove_many(self):
        create_user("u1")
        create_user("u2")

        node = ls("//sys/cluster_nodes")[0]
        path = "//sys/cluster_nodes/" + node

        ids_to_remove = [
            add_maintenance("cluster_node", node, "disable_write_sessions", "", authenticated_user="u1")[node],
            add_maintenance("cluster_node", node, "disable_write_sessions", "", authenticated_user="u1")[node],
            add_maintenance("cluster_node", node, "disable_scheduler_jobs", "", authenticated_user="u2")[node],
        ]

        reminded_requests = sorted([
            ids_to_remove[2],
            add_maintenance("cluster_node", node, "disable_write_sessions", "", authenticated_user="u1")[node]
        ])

        assert remove_maintenance("cluster_node", node, mine=True, ids=ids_to_remove, authenticated_user="u1") == {
            node: {"disable_write_sessions": 2}
        }

        # Only listed in `ids_to_remove` requests and with user "u1" must be removed.
        assert sorted(get(path + "/@maintenance_requests")) == reminded_requests

    @authors("kvk1920")
    def test_host_maintenance(self):
        create_host("h1")
        create_host("h2")
        create_user("u")

        nodes = ls("//sys/cluster_nodes")
        self._set_host(nodes[0], "h1")
        self._set_host(nodes[1], "h1")
        self._set_host(nodes[2], "h2")

        maintenances = add_maintenance("host", "h1", "ban", comment="because I want", authenticated_user="u")
        assert builtins.set(maintenances.keys()) == {nodes[0], nodes[1]}
        assert [maintenances[nodes[0]]] == list(get(f"//sys/cluster_nodes/{nodes[0]}/@maintenance_requests").keys())
        assert [maintenances[nodes[1]]] == list(get(f"//sys/cluster_nodes/{nodes[1]}/@maintenance_requests").keys())

        for node in nodes[:2]:
            maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
            assert len(maintenances) == 1
            maintenance = list(maintenances.values())[0]
            assert maintenance["type"] == "ban"
            assert maintenance["user"] == "u"
            assert maintenance["comment"] == "because I want"

        assert not get(f"//sys/cluster_nodes/{nodes[2]}/@maintenance_requests")

        not_mine_maintenance = add_maintenance("cluster_node", nodes[0], "ban", comment="")[nodes[0]]
        add_maintenance("cluster_node", nodes[1], "ban", comment="", authenticated_user="u")
        h2_maintenances = add_maintenance("cluster_node", nodes[2], "ban", comment="", authenticated_user="u")
        assert list(h2_maintenances.keys()) == [nodes[2]]
        h2_maintenance = h2_maintenances[nodes[2]]

        assert remove_maintenance("host", "h1", mine=True, authenticated_user="u") == {
            nodes[0]: {"ban": 1},
            nodes[1]: {"ban": 2},
        }

        # All requests of user "u" must be removed from every node on "h" host.
        assert not get(f"//sys/cluster_nodes/{nodes[1]}/@maintenance_requests")
        assert list(get(f"//sys/cluster_nodes/{nodes[0]}/@maintenance_requests")) == [not_mine_maintenance]
        # Another hosts must not be affected.
        assert list(get(f"//sys/cluster_nodes/{nodes[2]}/@maintenance_requests")) == [h2_maintenance]

    @authors("kvk1920")
    @pytest.mark.parametrize("proxy_type", ["http", "rpc"])
    def test_proxy_maintenance(self, proxy_type):
        if self.DRIVER_BACKEND == proxy_type:
            pytest.skip("Rpc proxies cannot be used if they are banned")
        proxy = ls(f"//sys/{proxy_type}_proxies")[1]
        proxy_path = f"//sys/{proxy_type}_proxies/{proxy}"
        maintenance_id = add_maintenance(f"{proxy_type}_proxy", proxy, "ban", comment="ABCDEF")[proxy]
        assert get(f"{proxy_path}/@banned")
        maintenances = get(f"{proxy_path}/@maintenance_requests")
        assert len(maintenances) == 1
        assert maintenances[maintenance_id]["type"] == "ban"
        assert maintenances[maintenance_id]["comment"] == "ABCDEF"

        remove_maintenance(f"{proxy_type}_proxy", proxy, id=maintenance_id)
        assert not get(f"{proxy_path}/@banned")


################################################################################


class TestMaintenanceTrackerMulticell(TestMaintenanceTracker):
    NUM_SECONDARY_MASTER_CELLS = 2

    @classmethod
    def setup_class(cls):
        super(TestMaintenanceTrackerMulticell, cls).setup_class()
        externalize("//sys/rpc_proxies", 11)
        externalize("//sys/http_proxies", 12)

    @authors("kvk1920")
    def test_maintenance_ids_sorting(self):
        node = ls("//sys/cluster_nodes")[0]
        for _ in range(3):
            for i in range(5):
                add_maintenance("cluster_node", node, "ban", comment=f"{i}")
            maintenances = list(get(f"//sys/cluster_nodes/{node}/@maintenance_requests"))
            assert len(maintenances) == 5
            maintenances.sort(reverse=True)
            assert remove_maintenance("cluster_node", node, ids=maintenances) == {
                node: {"ban": 5}
            }


################################################################################


class TestMaintenanceTrackerWithRpc(TestMaintenanceTracker):
    DRIVER_BACKEND = "rpc"
