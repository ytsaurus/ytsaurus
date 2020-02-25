from .conftest import (
    DEFAULT_ACCOUNT_ID,
    ZERO_RESOURCE_REQUESTS,
    are_pods_assigned,
    are_pods_touched_by_scheduler,
    assert_over_time,
    create_nodes,
    create_pod_set,
    create_pod_set_with_quota,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
    is_error_pod_scheduling_status,
    is_pod_assigned,
    update_node_id,
    wait_pod_is_assigned,
    wait_pod_is_assigned_to,
)

from yp.local import set_account_infinite_resource_limits

from yp.common import YtResponseError, wait, WaitFailed

from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries

from yt.yson import YsonEntity, YsonUint64

import yt.common

from yt.packages.six import itervalues
from yt.packages.six.moves import xrange

import pytest

from collections import Counter
from functools import partial
import random
import time


@pytest.mark.usefixtures("yp_env")
class TestScheduler(object):
    def _get_scheduled_allocations(self, yp_env, node_id, kind):
        yp_client = yp_env.yp_client
        results = yp_client.select_objects(
            "resource",
            filter='[/meta/kind] = "{}" and [/meta/node_id] = "{}"'.format(kind, node_id),
            selectors=["/status/scheduled_allocations"],
        )
        assert len(results) == 1
        return results[0][0]

    def test_create_with_enabled_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        node_id = yp_client.create_object("node")
        with pytest.raises(YtResponseError):
            create_pod_with_boilerplate(
                yp_client, pod_set_id, {"enable_scheduling": True, "node_id": node_id}
            )

    def test_force_assign(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 1)
        node_id = node_ids[0]

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client, pod_set_id, {"enable_scheduling": False, "node_id": node_id}
        )

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == [
            "assigned"
        ]

        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == []
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

        update_node_id(yp_client, pod_id, "")

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == [
            "disabled"
        ]

        update_node_id(yp_client, pod_id, node_id)

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == [
            "assigned"
        ]

    def test_enable_disable_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": False,})

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == [
            "disabled"
        ]

        node_ids = create_nodes(yp_client, 10)

        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    {"path": "/spec/enable_scheduling", "value": True},
                    {"path": "/spec/node_id", "value": node_ids[0]},
                ],
            )

        yp_client.update_object(
            "pod", pod_id, set_updates=[{"path": "/spec/enable_scheduling", "value": True}]
        )

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            != ""
            and yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0]
            == "assigned"
        )

    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"resource_requests": {"vcpu_guarantee": 100}, "enable_scheduling": True},
        )

        assert (
            yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0]
            == "pending"
        )

        create_nodes(yp_client, 10)

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            != ""
            and yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0]
            == "assigned"
        )

        node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]

        allocations = self._get_scheduled_allocations(yp_env, node_id, "cpu")
        assert len(allocations) == 1
        assert "pod_uuid" in allocations[0] and allocations[0]["pod_uuid"]
        assert allocations[0]["pod_id"] == pod_id
        assert allocations[0]["cpu"] == {"capacity": 100}

        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                {"path": "/spec/resource_requests/vcpu_guarantee", "value": YsonUint64(50)}
            ],
        )

        allocations = self._get_scheduled_allocations(yp_env, node_id, "cpu")
        assert len(allocations) == 1
        assert "pod_uuid" in allocations[0] and allocations[0]["pod_uuid"]
        assert allocations[0]["pod_id"] == pod_id
        assert allocations[0]["cpu"] == {"capacity": 50}

        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == []

    def test_remove_allocations_without_lock_conflict(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]
        pod_set_id = create_pod_set(yp_client)

        def create_pod(pod_id, transaction_id=None):
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                dict(enable_scheduling=True, resource_requests=dict(vcpu_guarantee=1)),
                pod_id=pod_id,
                transaction_id=transaction_id,
            )

        create_pod("first")
        wait_pod_is_assigned_to(yp_client, "first", node_id)

        # Assign and revoke concurrently without conflicts.
        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("pod", "first", transaction_id=transaction_id)
        create_pod("second")
        wait_pod_is_assigned_to(yp_client, "second", node_id)
        yp_client.commit_transaction(transaction_id)
        wait(lambda: len(self._get_scheduled_allocations(yp_env, node_id, "cpu")) == 1)

        create_pod("third")
        wait_pod_is_assigned_to(yp_client, "third", node_id)
        wait(lambda: len(self._get_scheduled_allocations(yp_env, node_id, "cpu")) == 2)

        # Recreate pod in transaction to change pod uuid on node.
        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("pod", "third", transaction_id=transaction_id)
        create_pod("third", transaction_id=transaction_id)
        create_pod("fourth")
        wait_pod_is_assigned_to(yp_client, "fourth", node_id)
        yp_client.commit_transaction(transaction_id)

        wait_pod_is_assigned_to(yp_client, "third", node_id)
        wait(lambda: len(self._get_scheduled_allocations(yp_env, node_id, "cpu")) == 3)

    def test_cpu_limit(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        for i in xrange(10):
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {"resource_requests": {"vcpu_guarantee": 30}, "enable_scheduling": True},
            )
        create_nodes(yp_client, 1)

        wait(
            lambda: len(
                yp_client.select_objects(
                    "pod", filter='[/status/scheduling/state] = "assigned"', selectors=["/meta/id"]
                )
            )
            > 0
        )
        assert (
            len(
                yp_client.select_objects(
                    "pod", filter='[/status/scheduling/state] = "assigned"', selectors=["/meta/id"]
                )
            )
            == 3
        )

        node_id = yp_client.select_objects("node", selectors=["/meta/id"])[0][0]
        unassigned_pod_id = yp_client.select_objects(
            "pod", filter='[/status/scheduling/state] != "assigned"', selectors=["/meta/id"]
        )[0][0]
        with pytest.raises(YtResponseError):
            update_node_id(yp_client, unassigned_pod_id, node_id, with_retries=False)

    def _wait_for_pod_assignment(self, yp_env):
        yp_client = yp_env.yp_client
        wait(
            lambda: all(
                x[0] == "assigned"
                for x in yp_client.select_objects("pod", selectors=["/status/scheduling/state"])
            )
        )

    def test_assign_to_up_nodes_only(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10, hfsm_state="down")

        up_node_id = node_ids[5]
        yp_client.update_hfsm_state(up_node_id, "up", "Test")

        pod_set_id = create_pod_set(yp_client)
        for _ in xrange(10):
            create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": True,})

        self._wait_for_pod_assignment(yp_env)
        assert all(
            x[0] == up_node_id
            for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"])
        )

    def test_node_segments(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10)
        for id in node_ids:
            yp_client.create_object(
                "node_segment",
                attributes={
                    "meta": {"id": "segment-" + id},
                    "spec": {"node_filter": '[/labels/cool] = "stuff"'},
                },
            )

        good_node_id = node_ids[0]
        good_segment_id = "segment-" + good_node_id

        yp_client.update_object(
            "node", good_node_id, set_updates=[{"path": "/labels/cool", "value": "stuff"}]
        )

        pod_set_id = yp_client.create_object(
            "pod_set", attributes={"spec": {"node_segment_id": good_segment_id}}
        )
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": True})

        self._wait_for_pod_assignment(yp_env)
        assert (
            yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == good_node_id
        )

    def test_invalid_network_project_in_pod_spec(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "ip6_address_requests": [{"vlan_id": "backbone", "network_id": "nonexisting"}],
                "enable_scheduling": True,
            },
        )

        wait(
            lambda: "error"
            in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0]
        )

    def test_allocations_clearance(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(
            yp_client,
            1,
            cpu_total_capacity=1000,
            network_bandwidth=500,
            disk_specs=[
                dict(
                    total_capacity=300,
                    total_volume_slots=10,
                    storage_class="hdd",
                    supported_policies=["quota"],
                )
            ],
            gpu_specs=[dict(model="v100", total_memory=2 ** 10)],
        )[0]

        pod_set_id = create_pod_set_with_quota(
            yp_client,
            bandwidth_quota=500,
            gpu_quota=dict(v100=1),
            disk_quota=dict(hdd=dict(capacity=300)),
        )[0]

        pod_id = yp_client.create_object(
            "pod",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "enable_scheduling": True,
                    "disk_volume_requests": [
                        {"id": "disk0", "storage_class": "hdd", "quota_policy": {"capacity": 300,},}
                    ],
                    "gpu_requests": [{"id": "gpu0", "model": "v100", "min_memory": 2 ** 10,}],
                },
            },
        )

        get_allocations = lambda: yp_client.get_object(
            "pod",
            pod_id,
            selectors=[
                "/status/scheduled_resource_allocations",
                "/status/disk_volume_allocations",
                "/status/gpu_allocations",
            ],
        )

        wait_pod_is_assigned(yp_client, pod_id)
        scheduled_allocations, disk_allocations, gpu_allocations = get_allocations()
        assert len(scheduled_allocations) > 0
        assert len(disk_allocations) == 1
        assert len(gpu_allocations) == 1
        assert disk_allocations[0]["id"] == "disk0"
        assert gpu_allocations[0]["request_id"] == "gpu0"

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "test")
        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]
            == "requested"
        )
        # Conflict with pod maintenance controller (/pod/status/maintenance shares lock with /pod/status/eviction) is possible.
        run_with_retries(
            lambda: yp_client.acknowledge_pod_eviction(pod_id, "test"),
            exceptions=(YtTabletTransactionLockConflict,),
        )
        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == ""
        )

        scheduled_allocations, disk_allocations, gpu_allocations = get_allocations()
        assert scheduled_allocations == YsonEntity()
        assert disk_allocations == YsonEntity()
        assert gpu_allocations == YsonEntity()

    def test_update_ip6_address_network_id_without_rescheduling(self, yp_env):
        yp_client = yp_env.yp_client

        network_project_ids = []
        for index in xrange(2):
            network_project_ids.append(
                yp_client.create_object(
                    "network_project",
                    attributes=dict(
                        meta=dict(id="somenet{}".format(index)), spec=dict(project_id=42 + index),
                    ),
                )
            )
        vlan_id = "backbone"
        create_nodes(yp_client, 10, vlan_id=vlan_id)

        pod_set_id = create_pod_set(yp_client)
        pod_spec = dict(
            enable_scheduling=True,
            ip6_address_requests=[
                dict(
                    vlan_id=vlan_id,
                    network_id=network_project_ids[0],
                    labels=dict(key1="value1",),
                    enable_dns=True,
                )
            ],
        )
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        wait(
            lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
        )

        def get_the_only_ip6_address_allocation():
            allocations = yp_client.get_object(
                "pod", pod_id, selectors=["/status/ip6_address_allocations"],
            )[0]
            assert len(allocations) == 1
            return allocations[0]

        def get_the_only_address_by_fqdn(fqdn):
            records = yp_client.get_object("dns_record_set", fqdn, selectors=["/spec/records"],)[0]
            assert len(records) == 1
            return records[0]["data"]

        scheduling_status = get_pod_scheduling_status(yp_client, pod_id)
        allocation = get_the_only_ip6_address_allocation()
        assert len(allocation["persistent_fqdn"]) > 0
        assert len(allocation["transient_fqdn"]) > 0
        assert get_the_only_address_by_fqdn(allocation["persistent_fqdn"]) == allocation["address"]

        def update_network_id(network_id):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(path="/spec/ip6_address_requests/0/network_id", value=network_id,)
                ],
            )

        update_network_id(network_project_ids[1])

        assert get_pod_scheduling_status(yp_client, pod_id) == scheduling_status
        new_allocation = get_the_only_ip6_address_allocation()
        assert allocation["address"] != new_allocation["address"]
        assert (
            get_the_only_address_by_fqdn(new_allocation["persistent_fqdn"])
            == new_allocation["address"]
        )
        for field_name in ("vlan_id", "labels", "persistent_fqdn", "transient_fqdn"):
            assert allocation[field_name] == new_allocation[field_name]

    def test_remove_orphaned_allocations_profiling(self, yp_env):
        orchid = yp_env.create_orchid_client()
        assert len(orchid.get_instances()) == 1
        instance = orchid.get_instances()[0]

        def get_profiling():
            samples = orchid.get(
                instance, "/profiling/scheduler/loop/allocation_plan/remove_orphaned_allocations",
            )
            return dict((sample["time"], sample["value"]) for sample in samples)

        base_profiling = get_profiling()

        def get_delta_profiling_values():
            profiling = get_profiling()
            for t in base_profiling:
                if t in profiling:
                    profiling.pop(t)
            return list(itervalues(profiling))

        yp_client = yp_env.yp_client

        resource_requests = dict(vcpu_guarantee=100)

        node_ids = create_nodes(yp_client, node_count=2)
        pod_set_id = create_pod_set(yp_client)

        pod_id1 = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(enable_scheduling=True, resource_requests=resource_requests),
        )
        wait_pod_is_assigned(yp_client, pod_id1)

        pod_node_id1 = yp_client.get_object(
            "pod", pod_id1, selectors=["/status/scheduling/node_id"]
        )[0]

        pod_node_id2 = None
        for node_id in node_ids:
            if node_id != pod_node_id1:
                pod_node_id2 = node_id
                break
        assert pod_node_id2 is not None

        pod_id2 = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(node_id=pod_node_id2, resource_requests=resource_requests),
        )

        def all_zeros(values):
            return all(map(lambda value: value == 0, values))

        assert_over_time(lambda: all_zeros(get_delta_profiling_values()))

        yp_client.remove_object("pod", pod_id1)
        yp_client.remove_object("pod", pod_id2)

        wait(lambda: sum(get_delta_profiling_values()) == 2)
        assert_over_time(lambda: sum(get_delta_profiling_values()) == 2)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerSlots(object):
    def test_slots_resource(self, yp_env):
        yp_client = yp_env.yp_client

        no_slot_node_id = create_nodes(yp_client, 1, slot_capacity=None)[0]
        create_nodes(yp_client, 1, slot_capacity=0)

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"resource_requests": {"vcpu_guarantee": 100, "slot": 1,}, "enable_scheduling": True,},
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        yp_client.remove_object("node", no_slot_node_id)

        no_slot_pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"resource_requests": {"vcpu_guarantee": 100,}, "enable_scheduling": True,},
        )
        wait(
            lambda: is_assigned_pod_scheduling_status(
                get_pod_scheduling_status(yp_client, no_slot_pod_id)
            )
        )

        create_nodes(yp_client, 1, slot_capacity=1)

        wait(
            lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
        )

    def test_slots_overcommit(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1, cpu_total_capacity=1000, slot_capacity=300)[0]
        pod_set_id = create_pod_set(yp_client)

        pod_ids = []
        for _ in range(3):
            pod_id = create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {
                    "resource_requests": {"vcpu_guarantee": 100, "slot": 1,},
                    "enable_scheduling": True,
                },
            )
            pod_ids.append(pod_id)

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        slot_id = yp_client.select_objects(
            "resource",
            selectors=["/meta/id"],
            filter='[/meta/node_id]="{}" and [/meta/kind]="slot"'.format(node_id),
        )[0][0]
        yp_client.update_object(
            "resource", slot_id, set_updates=[{"path": "/spec/slot/total_capacity", "value": 1}]
        )

        new_pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"resource_requests": {"vcpu_guarantee": 100, "slot": 1,}, "enable_scheduling": True,},
        )

        wait(
            lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, new_pod_id))
        )

        new_node_id = create_nodes(yp_client, 1, cpu_total_capacity=1000, slot_capacity=300)[0]

        wait(
            lambda: is_assigned_pod_scheduling_status(
                get_pod_scheduling_status(yp_client, new_pod_id)
            )
        )

        for pod_id in pod_ids + [new_pod_id]:
            scheduled_node_id = yp_client.get_object(
                "pod", pod_id, selectors=["/status/scheduling/node_id"]
            )[0]
            if pod_id in pod_ids:
                expected_node_id = node_id
            else:
                expected_node_id = new_node_id
            assert scheduled_node_id == expected_node_id

    @pytest.mark.parametrize("slot_demand_before,slot_demand_after", [(None, 1), (1, 2), (2, 0)])
    def test_update_slots_demand_without_rescheduling(
        self, yp_env, slot_demand_before, slot_demand_after
    ):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10, cpu_total_capacity=1000, slot_capacity=300)
        pod_set_id = create_pod_set(yp_client)

        pod_ids = []
        for _ in range(3):
            pod_spec = {
                "resource_requests": {"vcpu_guarantee": 100,},
                "enable_scheduling": True,
            }
            if slot_demand_before is not None:
                pod_spec["resource_requests"]["slot"] = slot_demand_before
            pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
            pod_ids.append(pod_id)

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        pod_to_node = {
            pod_id: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            for pod_id in pod_ids
        }

        for pod_id in pod_ids:
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[{"path": "/spec/resource_requests/slot", "value": slot_demand_after}],
            )

        new_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": True})
        wait(
            lambda: is_assigned_pod_scheduling_status(
                get_pod_scheduling_status(yp_client, new_pod_id)
            )
        )

        has_slot_allocation = lambda allocations: any(
            yp_client.get_object("resource", alloc["resource_id"], selectors=["/meta/kind"])[0]
            == "slot"
            for alloc in allocations
        )
        wait(
            lambda: all(
                has_slot_allocation(
                    yp_client.get_object(
                        "pod", pod_id, selectors=["/status/scheduled_resource_allocations"]
                    )[0]
                )
                is (slot_demand_after != 0)
                for pod_id in pod_ids
            )
        )

        node_pods_count = Counter(pod_to_node.values())
        for node_id in node_ids:
            slot_used = yp_client.select_objects(
                "resource",
                selectors=["/status/used/slot/capacity"],
                filter='[/meta/node_id]="{}" and [/meta/kind]="slot"'.format(node_id),
            )[0][0]
            assert slot_used == node_pods_count.get(node_id, 0) * slot_demand_after

        for pod_id in pod_ids:
            pod_node = yp_client.get_object(
                "pod", pod_id, selectors=["/status/scheduling/node_id"]
            )[0]
            assert pod_node == pod_to_node[pod_id]


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerExclusiveDiskUsage(object):
    def _test_schedule_pod_with_exclusive_disk_usage(self, yp_env, exclusive_first):
        yp_client = yp_env.yp_client

        disk_total_capacity = 2 * (10 ** 10)
        create_nodes(yp_client, node_count=1, disk_specs=[dict(total_capacity=disk_total_capacity)])

        pod_set_id = create_pod_set(yp_client)

        exclusive_pod_attributes = {
            "meta": {"pod_set_id": pod_set_id,},
            "spec": {
                "enable_scheduling": True,
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "disk_volume_requests": [
                    {
                        "id": "hdd1",
                        "storage_class": "hdd",
                        "exclusive_policy": {"min_capacity": disk_total_capacity // 2,},
                    },
                ],
            },
        }

        nonexclusive_pod_attributes = {
            "meta": {"pod_set_id": pod_set_id,},
            "spec": {
                "enable_scheduling": True,
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "disk_volume_requests": [
                    {
                        "id": "hdd1",
                        "storage_class": "hdd",
                        "quota_policy": {"capacity": disk_total_capacity // 2,},
                    },
                ],
            },
        }

        def wait_for_scheduling_status(pod_id, check_status):
            try:
                wait(lambda: check_status(get_pod_scheduling_status(yp_client, pod_id)))
            except WaitFailed:
                raise WaitFailed(
                    "Wait for pod scheduling failed: /status/scheduling = '{}')".format(
                        get_pod_scheduling_status(yp_client, pod_id)
                    )
                )

        if exclusive_first:
            exclusive_pod_id = yp_client.create_object("pod", attributes=exclusive_pod_attributes)
            wait_for_scheduling_status(
                exclusive_pod_id,
                lambda status: status["state"] == "assigned" and "error" not in status,
            )
            nonexclusive_pod_id = yp_client.create_object(
                "pod", attributes=nonexclusive_pod_attributes
            )
            wait_for_scheduling_status(
                nonexclusive_pod_id,
                lambda status: status["state"] == "pending" and "error" in status,
            )
        else:
            nonexclusive_pod_id = yp_client.create_object(
                "pod", attributes=nonexclusive_pod_attributes
            )
            wait_for_scheduling_status(
                nonexclusive_pod_id,
                lambda status: status["state"] == "assigned" and "error" not in status,
            )
            exclusive_pod_id = yp_client.create_object("pod", attributes=exclusive_pod_attributes)
            wait_for_scheduling_status(
                exclusive_pod_id, lambda status: status["state"] == "pending" and "error" in status
            )

    def test_schedule_pod_with_exclusive_disk_usage_before_nonexclusive(self, yp_env):
        self._test_schedule_pod_with_exclusive_disk_usage(yp_env, exclusive_first=True)

    def test_schedule_pod_with_exclusive_disk_usage_after_nonexclusive(self, yp_env):
        self._test_schedule_pod_with_exclusive_disk_usage(yp_env, exclusive_first=False)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerNodeFilter(object):
    def test_pod_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10)
        good_node_id = node_ids[5]
        yp_client.update_object(
            "node", good_node_id, set_updates=[{"path": "/labels/status", "value": "good"}]
        )

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"vcpu_guarantee": 100},
                "enable_scheduling": True,
                "node_filter": '[/labels/status] = "good"',
            },
        )

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == good_node_id
        )

    def test_pod_set_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10)
        good_node_id = node_ids[5]
        yp_client.update_object(
            "node", good_node_id, set_updates=[{"path": "/labels/status", "value": "good"}]
        )

        pod_set_id = create_pod_set(yp_client, spec=dict(node_filter='[/labels/status] = "good"'))
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"resource_requests": {"vcpu_guarantee": 100}, "enable_scheduling": True,},
        )

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == good_node_id
        )

    def test_node_filter_priority(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 10)
        good_node_id = node_ids[5]
        bad_node_id = node_ids[8]
        yp_client.update_object(
            "node", good_node_id, set_updates=[{"path": "/labels/status", "value": "good"}]
        )
        yp_client.update_object(
            "node", bad_node_id, set_updates=[{"path": "/labels/status", "value": "bad"}]
        )

        pod_set_id = create_pod_set(yp_client, spec=dict(node_filter='[/labels/status] = "bad"'))
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"vcpu_guarantee": 100},
                "enable_scheduling": True,
                "node_filter": '[/labels/status] = "good"',
            },
        )

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == good_node_id
        )

    def test_ignore_pod_set_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]
        yp_client.update_object(
            "node", node_id, set_updates=[{"path": "/labels/status", "value": "good"}]
        )

        pod_set_id = create_pod_set(yp_client, spec=dict(node_filter='[/labels/status] = "bad"'))
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"vcpu_guarantee": 100},
                "enable_scheduling": True,
                "node_filter": "true",
            },
        )

        wait(
            lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )

    def test_malformed_pod_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 10)
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"enable_scheduling": True, "node_filter": '[/some/nonexisting] = "value"'},
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

    def test_malformed_pod_set_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 10)
        pod_set_id = create_pod_set(
            yp_client, spec=dict(node_filter='[/some/nonexisting] = "value"')
        )
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": True,})

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

    def test_invalid_node_filter_at_pod(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client, pod_set_id, {"enable_scheduling": True, "node_filter": "invalid!!!"}
        )

        wait(
            lambda: "error"
            in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0]
        )


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerPartialAssignment:
    # Need backoff time on failed allocation greater than loop period.
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 1000,
            "failed_allocation_backoff": {"start": 15 * 1000, "max": 15 * 1000,},
        },
    }

    def test(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set(yp_client)

        erroneous_pod_id = yp_client.create_object(
            "pod",
            {
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "enable_scheduling": True,
                    "ip6_address_requests": [{"network_id": "nonexisting", "vlan_id": "backbone"}],
                    "resource_requests": {
                        "vcpu_guarantee": 100,
                        "vcpu_limit": 100,
                        "memory_guarantee": 128 * 1024 * 1024,
                        "memory_limit": 128 * 1024 * 1024,
                    },
                },
            },
        )

        initial_pod_dump = yp_client.get_object("pod", erroneous_pod_id, [""])[0]

        pod_ids = yp_client.create_objects(
            [
                (
                    "pod",
                    {
                        "meta": {"pod_set_id": pod_set_id},
                        "spec": {
                            "enable_scheduling": True,
                            "resource_requests": ZERO_RESOURCE_REQUESTS,
                        },
                    },
                )
                for _ in xrange(2)
            ]
        )

        def _test_got_error_and_not_partially_assigned(old_pod_dump, pod_id):
            new_pod_dump = yp_client.get_object("pod", pod_id, [""])[0]

            assert (
                old_pod_dump["status"]["scheduling"]["last_updated"]
                == new_pod_dump["status"]["scheduling"]["last_updated"]
            )

            assert "error" in new_pod_dump["status"]["scheduling"]
            assert new_pod_dump["status"].get("state", None) != "assigned"
            assert new_pod_dump["status"].get("node_id", "") == ""

            assert not new_pod_dump["status"].get("scheduled_resource_allocations", None)
            assert not new_pod_dump["status"].get("ip6_address_allocations", None)
            assert not new_pod_dump["status"]["dns"].get("transient_fqdn", None)

            return True

        # Pod with request of ip address with invalid network_id
        # must not be assigned, its resources must not be allocated
        # and this pod must get field /status/scheduling/error after assign.
        wait(lambda: are_pods_touched_by_scheduler(yp_client, [erroneous_pod_id]))
        assert_over_time(
            lambda: _test_got_error_and_not_partially_assigned(initial_pod_dump, erroneous_pod_id)
        )

        wait(lambda: are_pods_assigned(yp_client, pod_ids))


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerFailedAllocationBackoff:
    # Need start backoff time smaller than loop period and max backoff time greater than it.
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 1000,
            "failed_allocation_backoff": {"start": 200, "max": 15 * 1000, "base": 2.0,},
        },
    }

    def test(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set(yp_client)

        pod_ids = []
        for _ in range(3):
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {
                    "enable_scheduling": True,
                    "ip6_address_requests": [{"network_id": "nonexisting", "vlan_id": "backbone"}],
                },
            )
            pod_ids.append(
                create_pod_with_boilerplate(yp_client, pod_set_id, {"enable_scheduling": True})
            )
            wait(lambda: are_pods_assigned(yp_client, pod_ids))


@pytest.mark.usefixtures("yp_env")
class TestSchedulerGpu(object):
    def test_gpu_resource_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 10)
        for gpu_model in ("v10{}".format(i) for i in range(10)):
            gpu_spec = dict(model=gpu_model, total_memory=2 ** 16)
            create_nodes(yp_client, 1, gpu_specs=[gpu_spec])
        for gpu_memory in (2 ** (15 - i) for i in range(10)):
            gpu_spec = dict(model="k100", total_memory=gpu_memory)
            node_id = create_nodes(yp_client, 1, gpu_specs=[gpu_spec])[0]
            if gpu_memory == 2 ** 15:
                matching_node_id = node_id

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(k100=1),)[0]
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"vcpu_guarantee": 100,},
                "gpu_requests": [{"id": "gpu1", "model": "k100", "min_memory": 2 ** 15,}],
                "enable_scheduling": True,
            },
        )

        wait_pod_is_assigned_to(yp_client, pod_id, matching_node_id)

    def test_gpu_no_rescheduling(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100, v200=100),)[0]

        make_nodes = lambda count: create_nodes(
            yp_client,
            count,
            cpu_total_capacity=1000,
            gpu_specs=[dict(model="v100", total_memory=2 ** 16)] * 2,
        )

        make_pod = lambda: create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "gpu_requests": [{"id": "gpu1", "model": "v100", "min_memory": 2 ** 16,}],
                "enable_scheduling": True,
            },
        )

        get_node_id = lambda pod_id: yp_client.get_object(
            "pod", pod_id, selectors=["/status/scheduling/node_id"]
        )[0]

        node_id = make_nodes(1)[0]
        pod_ids = [make_pod() for _ in range(2)]
        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        gpu_resources = [
            response[0]
            for response in yp_client.select_objects(
                "resource",
                filter='[/meta/node_id] = "{}" and [/meta/kind] = "gpu"'.format(node_id),
                selectors=["/meta/id"],
            )
        ]

        make_nodes(10)

        yp_client.update_object(
            "resource", gpu_resources[0], set_updates=[dict(path="/spec/gpu/total_memory", value=0)]
        )
        yp_client.update_object(
            "pod",
            pod_ids[1],
            set_updates=[dict(path="/spec/gpu_requests/0/min_memory", value=2 ** 40),],
        )
        yp_client.update_object(
            "pod", pod_ids[1], set_updates=[dict(path="/spec/gpu_requests/0/model", value="v200"),]
        )

        new_pod_id = make_pod()
        wait(lambda: is_pod_assigned(yp_client, new_pod_id))
        assert all(get_node_id(pod_id) == node_id for pod_id in pod_ids)

        with pytest.raises(YtResponseError):
            yp_client.remove_object("resource", gpu_resources[1])

    def test_multiple_gpu_resource_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        gpu_spec = dict(model="v100", total_memory=2 ** 16)
        create_nodes(yp_client, 21, gpu_specs=[gpu_spec] * 2)
        nodes = create_nodes(yp_client, 11, gpu_specs=[gpu_spec] * 3)
        matching_node_id = nodes[5]
        occupied_node_ids = nodes[:5] + nodes[6:]

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100),)[0]
        for node_id in occupied_node_ids:
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {
                    "node_id": node_id,
                    "gpu_requests": [{"id": "gpu1", "model": "v100", "min_memory": 2 ** 10,}],
                },
            )

        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "gpu_requests": [
                    {"id": "gpu{}".format(i), "model": "v100", "min_memory": 2 ** 10,}
                    for i in range(3)
                ],
                "enable_scheduling": True,
            },
        )

        wait_pod_is_assigned_to(yp_client, pod_id, matching_node_id)

    def test_gpu_resource_exact_memory_bound(self, yp_env):
        yp_client = yp_env.yp_client

        memory_capacities = [2 ** i for i in range(30)]
        for i in range(10):
            node_memory_capacities = memory_capacities[3 * i : 3 * i + 3]
            gpu_specs = [
                dict(model="v100", total_memory=memory) for memory in node_memory_capacities
            ]
            create_nodes(yp_client, 1, gpu_specs=gpu_specs)

        pod_set_id = create_pod_set_with_quota(
            yp_client, gpu_quota=dict(v100=len(memory_capacities)),
        )[0]
        pod_ids = []
        random.seed(42)
        random.shuffle(memory_capacities)
        for memory in memory_capacities:
            pod_id = create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {
                    "gpu_requests": [
                        {"id": "gpu1", "model": "v100", "min_memory": memory, "max_memory": memory,}
                    ],
                    "enable_scheduling": True,
                },
            )
            pod_ids.append(pod_id)

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        for memory, pod_id in zip(memory_capacities, pod_ids):
            allocations = yp_client.get_object(
                "pod", pod_id, selectors=["/status/scheduled_resource_allocations"]
            )[0]
            for alloc in allocations:
                resource_spec = yp_client.get_object(
                    "resource", alloc["resource_id"], selectors=["/spec"]
                )[0]
                if "gpu" in resource_spec:
                    assert resource_spec["gpu"]["total_memory"] == memory

    def test_gpu_resource_no_overcommit(self, yp_env):
        yp_client = yp_env.yp_client

        gpu_spec = dict(model="v100", total_memory=2 ** 16)
        create_nodes(yp_client, 1, gpu_specs=[gpu_spec] * 2)

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100),)[0]

        make_pod = lambda: create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "gpu_requests": [{"id": "gpu1", "model": "v100", "min_memory": 2 ** 16,}],
                "enable_scheduling": True,
            },
        )

        pod_ids = [make_pod() for _ in range(2)]
        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        new_pod_id = make_pod()
        wait(
            lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, new_pod_id))
        )

    def test_gpu_resource_no_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100),)[0]

        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "gpu_requests": [{"id": "gpu1", "model": "v100", "min_memory": 2 ** 16,}],
                "enable_scheduling": True,
            },
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

    def test_gpu_resource_filters(self, yp_env):
        yp_client = yp_env.yp_client

        memory_capacities = [2 ** i for i in range(30)]
        v100_nodes = [
            create_nodes(yp_client, 1, gpu_specs=[dict(model="v100", total_memory=capacity)])[0]
            for capacity in memory_capacities
        ]
        k100_node = create_nodes(
            yp_client,
            1,
            gpu_specs=[dict(model="k100", total_memory=capacity) for capacity in memory_capacities],
        )[0]
        pod_set_id = create_pod_set_with_quota(
            yp_client, gpu_quota=dict(v100=len(memory_capacities), k100=1),
        )[0]

        make_pod = lambda **params: create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {"gpu_requests": [dict(id="gpu1", **params)], "enable_scheduling": True},
        )

        small_pod = make_pod(model="v100", max_memory=1)
        big_pod = make_pod(model="v100", min_memory=2 ** 29)
        k100_pod = make_pod(model="k100")

        wait(lambda: are_pods_assigned(yp_client, [small_pod, big_pod, k100_pod]))

        assert get_pod_scheduling_status(yp_client, small_pod)["node_id"] == v100_nodes[0]
        assert get_pod_scheduling_status(yp_client, big_pod)["node_id"] == v100_nodes[-1]
        assert get_pod_scheduling_status(yp_client, k100_pod)["node_id"] == k100_node


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerNetwork(object):
    def test_network_bandwidth(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set_with_quota(yp_client, bandwidth_quota=2 ** 60)[0]

        create_pod = lambda guarantee, limit: create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {
                    "network_bandwidth_guarantee": guarantee,
                    "network_bandwidth_limit": limit,
                },
                "enable_scheduling": True,
            },
        )

        wrong_node_id = create_nodes(yp_client, 1, network_bandwidth=None)[0]
        pod_ids = [create_pod(2 ** 29, 2 ** 32)]
        wait(
            lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_ids[0]))
        )

        yp_client.remove_object("node", wrong_node_id)

        pod_ids.append(create_pod(2 ** 29, 2 ** 32))
        pod_ids.append(
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                {"resource_requests": {"vcpu_guarantee": 10,}, "enable_scheduling": True},
            )
        )

        node_id = create_nodes(yp_client, 1, network_bandwidth=2 ** 30)[0]

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        network_allocations = yp_client.select_objects(
            "resource",
            filter='[/meta/node_id] = "{}" and [/meta/kind] = "network"'.format(node_id),
            selectors=["/status/scheduled_allocations"],
        )[0]

        assert len(network_allocations) == 1
        for alloc in network_allocations:
            assert alloc[0]["network"]["bandwidth"] == 2 ** 29

        zero_pod_id = create_pod(0, 2 ** 32)
        dummy_pod_id = create_pod_with_boilerplate(
            yp_client, pod_set_id, {"enable_scheduling": True}
        )

        wait(lambda: are_pods_assigned(yp_client, [zero_pod_id, dummy_pod_id]))

        new_pod_id = create_pod(1, 2 ** 32)
        wait(
            lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, new_pod_id))
        )

    def test_network_bandwidth_update_without_rescheduling(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 10, network_bandwidth=2 ** 30)
        pod_set_id = create_pod_set_with_quota(yp_client, bandwidth_quota=2 ** 60)[0]

        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"network_bandwidth_guarantee": 2 ** 20,},
                "enable_scheduling": True,
            },
        )

        wait(lambda: are_pods_assigned(yp_client, [pod_id]))

        node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
        network_resource = yp_client.select_objects(
            "resource",
            filter='[/meta/node_id] = "{}" and [/meta/kind] = "network"'.format(node_id),
            selectors=["/meta/id"],
        )[0][0]
        yp_client.update_object(
            "resource",
            network_resource,
            set_updates=[dict(path="/spec/network/total_bandwidth", value=0)],
        )

        new_pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            {
                "resource_requests": {"network_bandwidth_guarantee": 2 ** 20,},
                "enable_scheduling": True,
            },
        )

        wait(lambda: are_pods_assigned(yp_client, [new_pod_id]))

        assert (
            yp_client.get_object("pod", new_pod_id, selectors=["/status/scheduling/node_id"])[0]
            != node_id
        )
        assert (
            yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )


class TestSchedulerEveryNodeSelectionStrategyBase(object):
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 100,
            "failed_allocation_backoff": {"start": 150, "max": 150,},
            "global_resource_allocator": {
                "every_node_selection_strategy": {"iteration_period": 5, "iteration_splay": 2,},
            },
        },
    }

    _SCHEDULER_SAMPLE_SIZE = 10
    _NODE_COUNT = 20

    def _get_sufficient_wait_time(self):
        scheduler_config = self.YP_MASTER_CONFIG["scheduler"]
        strategy_config = scheduler_config["global_resource_allocator"][
            "every_node_selection_strategy"
        ]
        max_possible_iteration_period = (
            strategy_config["iteration_period"] + strategy_config["iteration_splay"]
        )
        seconds_per_iteration = (
            scheduler_config["loop_period"] + scheduler_config["failed_allocation_backoff"]["max"]
        ) / 1000.0
        return seconds_per_iteration * (max_possible_iteration_period + 5)

    def _get_default_node_configuration(self):
        return dict(
            cpu_total_capacity=10 ** 3,
            memory_total_capacity=10 ** 10,
            network_bandwidth=2 ** 30,
            disk_specs=[dict(total_capacity=10 ** 12)],
            vlan_id="backbone",
        )

    def _get_nonexistent_vlan_id(self):
        candidate = "somevlan"
        assert candidate != self._get_default_node_configuration()["vlan_id"]
        return candidate

    def _create_default_nodes(self, yp_env_configurable, config):
        yp_client = yp_env_configurable.yp_client
        default_config = self._get_default_node_configuration()
        default_config.update(config)
        create_nodes(yp_client, node_count=self._NODE_COUNT, **default_config)

    def _create_default_pod_set(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        return create_pod_set_with_quota(
            yp_client,
            cpu_quota=2 ** 60,
            memory_quota=2 ** 60,
            bandwidth_quota=2 ** 60,
            disk_quota=dict(hdd=dict(capacity=2 ** 60)),
        )[0]

    def _validate_scheduling_error(self, yp_env_configurable, pod_id, status_check):
        yp_client = yp_env_configurable.yp_client
        wait(lambda: status_check(get_pod_scheduling_status(yp_client, pod_id)))

    def _test_scheduling_error(self, yp_env_configurable, pod_spec, status_check):
        yp_client = yp_env_configurable.yp_client
        self._create_default_nodes(yp_env_configurable, dict())
        pod_set_id = self._create_default_pod_set(yp_env_configurable)
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        self._validate_scheduling_error(yp_env_configurable, pod_id, status_check)

    def _error_status_check(self, allocation_error_string, status):
        return (
            is_error_pod_scheduling_status(status)
            and "{}: {}".format(allocation_error_string, self._SCHEDULER_SAMPLE_SIZE)
            in str(status["error"])
            and "{}: {}".format(allocation_error_string, self._NODE_COUNT) in str(status["error"])
        )

    def _multiple_errors_status_check(self, allocation_error_strings, status):
        for allocation_error_string in allocation_error_strings:
            if not self._error_status_check(allocation_error_string, status):
                return False
        return True


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerEveryNodeSelectionStrategyNetworkErrors(
    TestSchedulerEveryNodeSelectionStrategyBase
):
    def test_ip6_address_internet_scheduling_unknown_module_error(self, yp_env_configurable):
        pod_spec = dict(
            enable_scheduling=True,
            ip6_address_requests=[
                dict(
                    vlan_id=self._get_default_node_configuration()["vlan_id"],
                    network_id="somenet",
                    enable_internet=True,
                ),
            ],
        )
        status_check = partial(self._error_status_check, "IP6AddressIP4TunnelUnknownNetworkModule")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_ip6_address_internet_scheduling_capacity_error(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        user_name = "root"
        network_module_id = "EKB-4.3.2"  # New empty network module.
        vlan_id = self._get_default_node_configuration()["vlan_id"]

        network_id = yp_client.create_object(
            "network_project",
            attributes={
                "spec": {"project_id": 123,},
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["use",], "subjects": [user_name,]}],
                },
            },
        )

        ip4_address_pool_id = yp_client.create_object(
            "ip4_address_pool",
            attributes={
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["use",], "subjects": [user_name,]}],
                },
            },
        )

        # Populating the module with exactly one address.
        yp_client.create_object(
            "internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": ip4_address_pool_id,},
                "spec": {"ip4_address": "1.3.5.7", "network_module_id": network_module_id,},
            },
        )

        pod_spec = dict(
            enable_scheduling=True,
            ip6_address_requests=[
                dict(
                    vlan_id=vlan_id, network_id=network_id, ip4_address_pool_id=ip4_address_pool_id,
                ),
            ],
        )

        self._create_default_nodes(
            yp_env_configurable, config=dict(network_module_id=network_module_id)
        )
        pod_set_id = self._create_default_pod_set(yp_env_configurable)

        # Occupying the only ip4 address in network module.
        first_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        wait(lambda: is_pod_assigned(yp_client, first_pod_id))

        # Scheduling of the second pod should fail due to the lack of ip4 addresses in network module.
        second_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        status_check = partial(self._error_status_check, "IP6AddressIP4TunnelCapacity")
        self._validate_scheduling_error(yp_env_configurable, second_pod_id, status_check)

    def test_ip6_address_vlan_scheduling_error(self, yp_env_configurable):
        pod_spec = dict(
            enable_scheduling=True,
            ip6_address_requests=[
                dict(
                    vlan_id=self._get_nonexistent_vlan_id(),
                    network_id="somenet",
                    enable_internet=False,
                ),
            ],
        )
        status_check = partial(self._error_status_check, "IP6AddressVlan")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_ip6_subnet_vlan_scheduling_error(self, yp_env_configurable):
        pod_spec = dict(
            enable_scheduling=True,
            ip6_subnet_requests=[
                dict(vlan_id=self._get_nonexistent_vlan_id(), network_id="somenet",),
            ],
        )
        status_check = partial(self._error_status_check, "IP6Subnet")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_network_scheduling_error(self, yp_env_configurable):
        bandwidth = self._get_default_node_configuration()["network_bandwidth"]
        pod_spec = dict(
            enable_scheduling=True,
            resource_requests=dict(network_bandwidth_guarantee=bandwidth + 1),
        )
        status_check = partial(self._error_status_check, "Network")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerEveryNodeSelectionStrategyResourceErrors(
    TestSchedulerEveryNodeSelectionStrategyBase
):
    def test_antiaffinity_vacancy_scheduling_error(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        create_nodes(
            yp_client, node_count=self._NODE_COUNT, **self._get_default_node_configuration()
        )

        pod_set_id = create_pod_set(
            yp_client, spec=dict(antiaffinity_constraints=[dict(key="node", max_pods=1,),],),
        )

        pod_spec = dict(enable_scheduling=True)

        preallocated_pod_ids = []
        for _ in range(self._NODE_COUNT):
            preallocated_pod_ids.append(
                create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
            )

        def preallocated_status_check():
            statuses = map(partial(get_pod_scheduling_status, yp_client), preallocated_pod_ids)
            return all(map(is_assigned_pod_scheduling_status, statuses))

        wait(preallocated_status_check)

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        wait(
            lambda: self._error_status_check(
                "Antiaffinity", get_pod_scheduling_status(yp_client, pod_id)
            )
        )

    def test_cpu_scheduling_error(self, yp_env_configurable):
        cpu_total_capacity = self._get_default_node_configuration()["cpu_total_capacity"]
        pod_spec = dict(
            enable_scheduling=True, resource_requests=dict(vcpu_guarantee=cpu_total_capacity + 1)
        )
        status_check = partial(self._error_status_check, "Cpu")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_memory_scheduling_error(self, yp_env_configurable):
        memory_total_capacity = self._get_default_node_configuration()["memory_total_capacity"]
        pod_spec = dict(
            enable_scheduling=True, resource_requests=dict(memory_limit=memory_total_capacity + 1),
        )
        status_check = partial(self._error_status_check, "Memory")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_slot_scheduling_error(self, yp_env_configurable):
        pod_spec = dict(enable_scheduling=True, resource_requests=dict(slot=100500),)
        status_check = partial(self._error_status_check, "Slot")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_disk_scheduling_error(self, yp_env_configurable):
        disk_total_capacity = self._get_default_node_configuration()["disk_specs"][0][
            "total_capacity"
        ]
        pod_spec = dict(
            enable_scheduling=True,
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class="hdd",
                    quota_policy=dict(capacity=disk_total_capacity + 1),
                )
            ],
        )
        status_check = partial(self._error_status_check, "Disk")
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_cpu_memory_disk_scheduling_errors(self, yp_env_configurable):
        cpu_total_capacity = self._get_default_node_configuration()["cpu_total_capacity"]
        memory_total_capacity = self._get_default_node_configuration()["memory_total_capacity"]
        disk_total_capacity = self._get_default_node_configuration()["disk_specs"][0][
            "total_capacity"
        ]
        pod_spec = dict(
            enable_scheduling=True,
            resource_requests=dict(
                vcpu_guarantee=cpu_total_capacity + 1, memory_limit=memory_total_capacity + 1,
            ),
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class="hdd",
                    quota_policy=dict(capacity=disk_total_capacity + 1),
                )
            ],
        )
        status_check = partial(self._multiple_errors_status_check, ("Cpu", "Memory", "Disk"))
        self._test_scheduling_error(yp_env_configurable, pod_spec, status_check)

    def test_pods_collision(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        create_nodes(
            yp_client, node_count=self._NODE_COUNT, **self._get_default_node_configuration()
        )

        pod_set_id = create_pod_set(yp_client)

        cpu_total_capacity = self._get_default_node_configuration()["cpu_total_capacity"]
        pod_spec = dict(
            enable_scheduling=True, resource_requests=dict(vcpu_guarantee=cpu_total_capacity + 1),
        )

        # Generate non-schedulable pods.
        POD_COUNT = 10
        pod_ids = []
        for i in range(POD_COUNT):
            pod_ids.append(create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec))

        # Make sure scheduler has enough time for changing internal state.
        time.sleep(self._get_sufficient_wait_time())

        # Recreate previous pods with schedulable guarantees.
        for pod_id in pod_ids:
            yp_client.remove_object("pod", pod_id)

        pod_spec["resource_requests"]["vcpu_guarantee"] = cpu_total_capacity
        for pod_id in pod_ids:
            create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec, pod_id=pod_id)

        # Expect scheduler to correctly identify different objects with equal ids.
        wait(
            lambda: all(
                is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
                for pod_id in pod_ids
            )
        )

    def test_scheduling_errors_brevity(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        create_nodes(yp_client, node_count=1, **self._get_default_node_configuration())
        pod_set_id = create_pod_set(yp_client)

        cpu_total_capacity = self._get_default_node_configuration()["cpu_total_capacity"]
        pod_spec = dict(
            enable_scheduling=True, resource_requests=dict(vcpu_guarantee=cpu_total_capacity + 1),
        )
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        error = get_pod_scheduling_status(yp_client, pod_id)["error"]["inner_errors"][0]["message"]
        resources = [
            pair.split(": ") for pair in error[error.find("{") + 1 : error.find("}")].split(", ")
        ]
        assert len(resources) == 1
        assert resources[0] == ["Cpu", "1"]


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulePodsStageLimits(object):
    SCHEDULE_PODS_STAGE_POD_LIMIT = 5
    LOOP_PERIOD_SECONDS = 20

    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": LOOP_PERIOD_SECONDS * 1000,
            "failed_allocation_backoff": {"start": 1000, "max": 1000,},
            "schedule_pods_stage": {"pod_limit": SCHEDULE_PODS_STAGE_POD_LIMIT,},
        },
    }

    def test_pod_limit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        def get_assigned_pod_count_over_time(pod_ids, iterations, sleep_backoff):
            assigned_pod_counts = []

            for _ in xrange(iterations):
                assigned_pod_counts.append(
                    sum(map(lambda pod_id: is_pod_assigned(yp_client, pod_id), pod_ids))
                )
                time.sleep(sleep_backoff)

            return assigned_pod_counts

        node_count = 1
        pod_count = 20
        create_nodes(yp_client, node_count)

        transaction_id = yp_client.start_transaction()

        pod_set_id = create_pod_set(yp_client, transaction_id=transaction_id)
        pod_ids = [
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(enable_scheduling=True),
                transaction_id=transaction_id,
            )
            for _ in xrange(pod_count)
        ]

        yp_client.commit_transaction(transaction_id)

        sleep_backoff_seconds = 5
        assert 2 * sleep_backoff_seconds < TestSchedulePodsStageLimits.LOOP_PERIOD_SECONDS

        minimal_scheduler_iterations = (
            pod_count - 1
        ) // TestSchedulePodsStageLimits.SCHEDULE_PODS_STAGE_POD_LIMIT + 1
        minimal_scheduling_time = (
            minimal_scheduler_iterations - 1
        ) * TestSchedulePodsStageLimits.LOOP_PERIOD_SECONDS
        check_iterations = minimal_scheduling_time // sleep_backoff_seconds + 1

        assigned_pod_counts = get_assigned_pod_count_over_time(
            pod_ids, check_iterations, sleep_backoff_seconds
        )

        assert assigned_pod_counts[0] <= TestSchedulePodsStageLimits.SCHEDULE_PODS_STAGE_POD_LIMIT
        for index in xrange(len(assigned_pod_counts) - 1):
            assert (
                assigned_pod_counts[index + 1]
                <= assigned_pod_counts[index]
                + TestSchedulePodsStageLimits.SCHEDULE_PODS_STAGE_POD_LIMIT
            )

        wait(lambda: are_pods_assigned(yp_client, pod_ids))


def _stress_test_scheduler(yp_client, create_pods_in_one_transaction=False):
    NODE_COUNT = 10
    POD_PER_NODE_COUNT = 10
    POD_CPU_CAPACITY = 3 * 1000
    POD_MEMORY_CAPACITY = 16 * (1024 ** 3)
    POD_DISK_CAPACITY = 100 * (1024 ** 3)

    set_account_infinite_resource_limits(yp_client, DEFAULT_ACCOUNT_ID)

    create_nodes(
        yp_client,
        NODE_COUNT,
        rack_count=1,
        hfsm_state="up",
        cpu_total_capacity=POD_CPU_CAPACITY * POD_PER_NODE_COUNT,
        memory_total_capacity=POD_MEMORY_CAPACITY * POD_PER_NODE_COUNT,
        disk_specs=[
            dict(
                total_capacity=POD_DISK_CAPACITY * POD_PER_NODE_COUNT,
                total_volume_slots=POD_PER_NODE_COUNT,
            )
        ],
    )

    transaction_id = None
    if create_pods_in_one_transaction:
        transaction_id = yp_client.start_transaction()

    pod_set_id = create_pod_set(yp_client, transaction_id=transaction_id)

    pod_spec = dict(
        enable_scheduling=True,
        resource_requests=dict(vcpu_guarantee=POD_CPU_CAPACITY, memory_limit=POD_MEMORY_CAPACITY,),
        disk_volume_requests=[
            dict(id="disk1", storage_class="hdd", quota_policy=dict(capacity=POD_DISK_CAPACITY),),
        ],
    )

    pod_ids = []
    for pod_index in range(NODE_COUNT * POD_PER_NODE_COUNT):
        pod_ids.append(
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                pod_spec,
                pod_id=str(pod_index),
                transaction_id=transaction_id,
            )
        )

    if create_pods_in_one_transaction:
        yp_client.commit_transaction(transaction_id)

    wait(
        lambda: all(
            is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
            for pod_id in pod_ids
        )
    )

    failed_pod_id = create_pod_with_boilerplate(
        yp_client, pod_set_id, pod_spec, pod_id="failed-pod"
    )

    wait(
        lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, failed_pod_id))
    )


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerNodeRandomHashPodNodeScore(object):
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 100,
            "failed_allocation_backoff": {"start": 150, "max": 150,},
            "global_resource_allocator": {
                "pod_node_score": {"type": "node_random_hash", "parameters": {"seed": 100500,},},
            },
        },
    }

    def test(self, yp_env_configurable):
        _stress_test_scheduler(yp_env_configurable.yp_client)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerFreeCpuMemoryShareVariancePodNodeScore(object):
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 100,
            "failed_allocation_backoff": {"start": 150, "max": 150,},
            "global_resource_allocator": {
                "pod_node_score": {"type": "free_cpu_memory_share_variance",},
            },
        },
    }

    def test(self, yp_env_configurable):
        _stress_test_scheduler(yp_env_configurable.yp_client)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerFreeCpuMemoryShareSquaredMinDeltaPodNodeScore(object):
    YP_MASTER_CONFIG = {
        "scheduler": {
            "loop_period": 100,
            "failed_allocation_backoff": {"start": 150, "max": 150,},
            "global_resource_allocator": {
                "pod_node_score": {"type": "free_cpu_memory_share_squared_min_delta",},
            },
        },
    }

    def test(self, yp_env_configurable):
        _stress_test_scheduler(yp_env_configurable.yp_client)


@pytest.mark.usefixtures("yp_env_configurable")
class TestTransactionLookupSession(object):
    YP_MASTER_CONFIG = dict(transaction_manager=dict(max_keys_per_lookup_request=2))

    def test(self, yp_env_configurable):
        # Assuming that a good way to stress test lookups is to schedule many pods within one transaction.
        _stress_test_scheduler(yp_env_configurable.yp_client, create_pods_in_one_transaction=True)
