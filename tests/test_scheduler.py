from .conftest import ZERO_RESOURCE_REQUESTS

from yp.common import YtResponseError, wait, WaitFailed

from yt.packages.six.moves import xrange

from yt.yson import YsonEntity, YsonUint64
import yt.common

import pytest

from collections import defaultdict


DEFAULT_POD_SET_SPEC = {
        "account_id": "tmp",
        "node_segment_id": "default"
    }

@pytest.mark.usefixtures("yp_env")
class TestScheduler(object):
    def _create_pod_with_boilerplate(self, yp_client, pod_set_id, spec):
        merged_spec = yt.common.update(
            {
                "resource_requests": ZERO_RESOURCE_REQUESTS
            },
            spec)
        return yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": merged_spec
        })

    def _create_nodes(
            self,
            yp_env,
            node_count,
            rack_count=1,
            hfsm_state="up",
            cpu_total_capacity=100,
            memory_total_capacity=1000000000,
            disk_total_capacity=100000000000,
            disk_total_volume_slots=10):
        yp_client = yp_env.yp_client

        node_ids = []
        for i in xrange(node_count):
            node_id = yp_client.create_object("node", attributes={
                    "spec": {
                        "ip6_subnets": [
                            {"vlan_id": "backbone", "subnet": "1:2:3:4::/64"}
                        ]
                    },
                    "labels" : {
                        "topology": {
                            "node": "node-{}".format(i),
                            "rack": "rack-{}".format(i // (node_count // rack_count)),
                            "dc": "butovo"
                        }
                    }
                })
            yp_client.update_hfsm_state(node_id, hfsm_state, "Test")
            node_ids.append(node_id)
            yp_client.create_object("resource", attributes={
                    "meta": {
                        "node_id": node_id
                    },
                    "spec": {
                        "cpu": {
                            "total_capacity": cpu_total_capacity,
                        }
                    }
                })
            yp_client.create_object("resource", attributes={
                    "meta": {
                        "node_id": node_id
                    },
                    "spec": {
                        "memory": {
                            "total_capacity": memory_total_capacity,
                        }
                    }
                })
            yp_client.create_object("resource", attributes={
                    "meta": {
                        "node_id": node_id
                    },
                    "spec": {
                        "disk": {
                            "total_capacity": disk_total_capacity,
                            "total_volume_slots": disk_total_volume_slots,
                            "storage_class": "hdd",
                            "supported_policies": ["quota", "exclusive"],
                        }
                    }
                })
        return node_ids

    def _get_scheduled_allocations(self, yp_env, node_id, kind):
        yp_client = yp_env.yp_client
        results = yp_client.select_objects("resource", filter="[/meta/kind] = \"{}\" and [/meta/node_id] = \"{}\"".format(kind, node_id),
                                           selectors=["/status/scheduled_allocations"])
        assert len(results) == 1
        return results[0][0]

    def test_create_with_enabled_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        node_id = yp_client.create_object("node")
        with pytest.raises(YtResponseError):
            self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "enable_scheduling": True,
                    "node_id": node_id
                })

    def test_force_assign(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 1)
        node_id = node_ids[0]

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": False,
                "node_id": node_id
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["assigned"]

        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == []
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

        yp_client.update_object("pod", pod_id,
            set_updates=[
                {"path": "/spec/node_id", "value": ""}
            ])

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["disabled"]

        yp_client.update_object("pod", pod_id,
            set_updates=[
                {"path": "/spec/node_id", "value": node_id}
            ])

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["assigned"]

    def test_enable_disable_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": False,
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["disabled"]

        node_ids = self._create_nodes(yp_env, 10)

        with pytest.raises(YtResponseError):
            yp_client.update_object("pod", pod_id,
                set_updates=[
                    {"path": "/spec/enable_scheduling", "value": True},
                    {"path": "/spec/node_id", "value": node_ids[0]}
                ])

        yp_client.update_object("pod", pod_id,
            set_updates=[
                {"path": "/spec/enable_scheduling", "value": True}
            ])

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] != "" and
                     yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "resource_requests": {
                    "vcpu_guarantee": 100
                },
                "enable_scheduling": True
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "pending"

        self._create_nodes(yp_env, 10)

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] != "" and
                     yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == [{"pod_id": pod_id, "cpu": {"capacity": 100}}]
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/resource_requests/vcpu_guarantee", "value": YsonUint64(50)}])
        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == [{"pod_id": pod_id, "cpu": {"capacity": 50}}]
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

    def test_cpu_limit(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        for i in xrange(10):
            pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "resource_requests": {
                        "vcpu_guarantee": 30
                    },
                    "enable_scheduling": True
                })
        self._create_nodes(yp_env, 1)

        wait(lambda: len(yp_client.select_objects("pod", filter="[/status/scheduling/state] = \"assigned\"", selectors=["/meta/id"])) > 0)
        assert len(yp_client.select_objects("pod", filter="[/status/scheduling/state] = \"assigned\"", selectors=["/meta/id"])) == 3

        node_id = yp_client.select_objects("node", selectors=["/meta/id"])[0][0]
        unassigned_pod_id = yp_client.select_objects("pod", filter="[/status/scheduling/state] != \"assigned\"", selectors=["/meta/id"])[0][0]
        with pytest.raises(YtResponseError):
            yp_client.update_object("pod", unassigned_pod_id, set_updates=[{"path": "/spec/node_id", "value": node_id}])

    def test_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 10)
        good_node_id = node_ids[5]
        yp_client.update_object("node", good_node_id, set_updates=[
            {"path": "/labels/status", "value": "good"}
        ])

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "resource_requests": {
                    "vcpu_guarantee": 100
                },
                "enable_scheduling": True,
                "node_filter" : '[/labels/status] = "good"'
            })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == good_node_id)

    def test_malformed_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 10)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": True,
                "node_filter" : '[/some/nonexisting] = "value"'
            })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == YsonEntity())

    def _wait_for_pod_assignment(self, yp_env):
        yp_client = yp_env.yp_client
        wait(lambda: all(x[0] == "assigned"
                         for x in yp_client.select_objects("pod", selectors=["/status/scheduling/state"])))

    def test_antiaffinity_per_node(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 10)
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "spec": dict(DEFAULT_POD_SET_SPEC, **{
                    "antiaffinity_constraints": [
                        {"key": "node", "max_pods": 1}
                    ]
                })
            })
        for i in xrange(10):
            self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "enable_scheduling": True,
                })

        self._wait_for_pod_assignment(yp_env)
        node_ids = set(x[0] for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))
        assert len(node_ids) == 10

    def test_antiaffinity_per_node_and_rack(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 10, 2)
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "spec": dict(DEFAULT_POD_SET_SPEC, **{
                    "antiaffinity_constraints": [
                        {"key": "node", "max_pods": 1},
                        {"key": "rack", "max_pods": 3}
                    ],
                })
            })
        for i in xrange(6):
            self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "enable_scheduling": True,
                })

        self._wait_for_pod_assignment(yp_env)
        node_ids = set(x[0] for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))
        assert len(node_ids) == 6

        rack_to_counter = defaultdict(int)
        for node_id in node_ids:
            rack = yp_client.get_object("node", node_id, selectors=["/labels/topology/rack"])[0]
            rack_to_counter[rack] += 1
        assert all(rack_to_counter[rack] == 3 for rack in rack_to_counter)

    def test_assign_to_up_nodes_only(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 10, hfsm_state="down")

        up_node_id = node_ids[5]
        yp_client.update_hfsm_state(up_node_id, "up", "Test")

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        for _ in xrange(10):
            self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "enable_scheduling": True,
                })

        self._wait_for_pod_assignment(yp_env)
        assert all(x[0] == up_node_id for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))

    def test_node_maintenance(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 1)
        node_id = node_ids[0]

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_ids = []
        for _ in xrange(10):
            pod_ids.append(self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                    "enable_scheduling": True
                }))

        self._wait_for_pod_assignment(yp_env)
        assert all(x[0] == node_id for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))
        assert all(x[0] == "none" for x in yp_client.select_objects("pod", selectors=["/status/eviction/state"]))

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "requested"

        wait(lambda: all(x[0] == "requested" for x in yp_client.select_objects("pod", selectors=["/status/eviction/state"])))

        for pod_id in pod_ids:
            yp_client.acknowledge_pod_eviction(pod_id, "Test")

        wait(lambda: all(x[0] == YsonEntity() for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"])))
        assert all(x[0] == "none" for x in yp_client.select_objects("pod", selectors=["/status/eviction/state"]))

        wait(lambda: yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "acknowledged")

        yp_client.update_hfsm_state(node_id, "maintenance", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "in_progress"

        yp_client.update_hfsm_state(node_id, "down", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "none"

    def test_node_segments(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 10)
        for id in node_ids:
            yp_client.create_object("node_segment", attributes={
                "meta": {
                    "id": "segment-" + id
                },
                "spec": {
                    "node_filter": "[/labels/cool] = \"stuff\""
                }
            })

        good_node_id = node_ids[0]
        good_segment_id = "segment-" + good_node_id

        yp_client.update_object("node", good_node_id, set_updates=[{"path": "/labels/cool", "value": "stuff"}])

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": good_segment_id}})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": True
            })

        self._wait_for_pod_assignment(yp_env)
        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == good_node_id

    def test_invalid_node_filter_at_pod(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 1)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": True,
                "node_filter": "invalid!!!"
            })

        wait(lambda: "error" in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0])

    def test_invalid_network_project_in_pod_spec(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 1)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})
        pod_id = self._create_pod_with_boilerplate(yp_client, pod_set_id, {
                "ip6_address_requests": [
                    {"vlan_id": "backbone", "network_id": "nonexisting"}
                ],
                "enable_scheduling": True
            })

        wait(lambda: "error" in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0])

    def _test_schedule_pod_with_exclusive_disk_usage(self, yp_env, exclusive_first):
        yp_client = yp_env.yp_client

        disk_total_capacity = 2 * (10 ** 10)
        self._create_nodes(yp_env, node_count=1, disk_total_capacity=disk_total_capacity)

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": DEFAULT_POD_SET_SPEC})

        exclusive_pod_attributes = {
            "meta": {
                "pod_set_id": pod_set_id,
            },
            "spec": {
                "enable_scheduling": True,
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "disk_volume_requests": [
                    {
                        "id": "hdd1",
                        "storage_class": "hdd",
                        "exclusive_policy": {
                            "min_capacity": disk_total_capacity // 2,
                        },
                    },
                ],
            },
        }

        nonexclusive_pod_attributes = {
            "meta": {
                "pod_set_id": pod_set_id,
            },
            "spec": {
                "enable_scheduling": True,
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "disk_volume_requests": [
                    {
                        "id": "hdd1",
                        "storage_class": "hdd",
                        "quota_policy": {
                            "capacity": disk_total_capacity // 2,
                        },
                    },
                ],
            },
        }

        def get_scheduling_status(pod_id):
            return yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0]

        def wait_for_scheduling_status(pod_id, check_status):
            try:
                wait(lambda: check_status(get_scheduling_status(pod_id)))
            except WaitFailed as exception:
                raise WaitFailed("Wait for pod scheduling failed: /status/scheduling = '{}')".format(
                    get_scheduling_status(pod_id)))

        if exclusive_first:
            exclusive_pod_id = yp_client.create_object("pod", attributes=exclusive_pod_attributes)
            wait_for_scheduling_status(exclusive_pod_id, lambda status: status["state"] == "assigned" and "error" not in status)
            nonexclusive_pod_id = yp_client.create_object("pod", attributes=nonexclusive_pod_attributes)
            wait_for_scheduling_status(nonexclusive_pod_id, lambda status: status["state"] == "pending" and "error" in status)
        else:
            nonexclusive_pod_id = yp_client.create_object("pod", attributes=nonexclusive_pod_attributes)
            wait_for_scheduling_status(nonexclusive_pod_id, lambda status: status["state"] == "assigned" and "error" not in status)
            exclusive_pod_id = yp_client.create_object("pod", attributes=exclusive_pod_attributes)
            wait_for_scheduling_status(exclusive_pod_id, lambda status: status["state"] == "pending" and "error" in status)

    def test_schedule_pod_with_exclusive_disk_usage_before_nonexclusive(self, yp_env):
        self._test_schedule_pod_with_exclusive_disk_usage(yp_env, exclusive_first=True)

    def test_schedule_pod_with_exclusive_disk_usage_after_nonexclusive(self, yp_env):
        self._test_schedule_pod_with_exclusive_disk_usage(yp_env, exclusive_first=False)
