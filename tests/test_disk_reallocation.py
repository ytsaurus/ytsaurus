from .conftest import (
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    update_node_id,
    wait_pod_is_assigned,
    wait_pods_are_assigned,
)

from yp.common import YtResponseError

from yt.yson import YsonUint64

from yt.packages.six import itervalues

import collections
import pytest


class TestDiskReallocation(object):
    def _get_pod_disk_volume_only_allocation(self, yp_client, pod_id):
        allocations = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/status/disk_volume_allocations"]
        )[0]
        assert 1 == len(allocations)
        return allocations[0]

    def _get_disk_only_id(self, yp_client, storage_class):
        response = yp_client.select_objects(
            "resource",
            filter='[/meta/kind] = "disk" and [/spec/disk/storage_class] = "hdd"',
            selectors=["/meta/id"],
        )
        assert 1 == len(response)
        return response[0][0]

    def test_update_bandwidth_limit(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(
            yp_client,
            node_count=1,
            disk_specs=[
                dict(
                    read_bandwidth_factor=1.0,
                    write_bandwidth_factor=1.0,
                    read_operation_rate_divisor=1.0,
                    write_operation_rate_divisor=1.0,
                )
            ],
        )

        pod_set_id = create_pod_set(yp_client)

        base_quota_policy = dict(capacity=10 ** 9)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True,
                disk_volume_requests=[
                    dict(id="disk1", storage_class="hdd", quota_policy=base_quota_policy),
                ],
            ),
        )
        wait_pod_is_assigned(yp_client, pod_id)

        def validate_disk_volume_allocation_limits(bandwidth_limit):
            disk_volume_allocation = self._get_pod_disk_volume_only_allocation(yp_client, pod_id)
            field_names = (
                "read_bandwidth_limit",
                "write_bandwidth_limit",
                "read_operation_rate_limit",
                "write_operation_rate_limit",
            )
            for field_name in field_names:
                assert disk_volume_allocation.get(field_name, None) == bandwidth_limit

        validate_disk_volume_allocation_limits(None)
        last_bandwidth_limit = 300
        for bandwidth_limit in (100, 200, None, last_bandwidth_limit):
            quota_policy = base_quota_policy.copy()
            if bandwidth_limit is not None:
                quota_policy["bandwidth_limit"] = bandwidth_limit
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(path="/spec/disk_volume_requests/0/quota_policy", value=quota_policy,),
                ],
            )
            validate_disk_volume_allocation_limits(bandwidth_limit)

        # Update of bandwidth limit is possible even in case of overcommit.
        resource_id = self._get_disk_only_id(yp_client, "hdd")
        yp_client.update_object(
            "resource",
            resource_id,
            set_updates=[
                dict(
                    path="/spec/disk/total_capacity",
                    value=base_quota_policy["capacity"] - 1,
                ),
            ],
        )
        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                dict(
                    path="/spec/disk_volume_requests/0/quota_policy/bandwidth_limit",
                    value=last_bandwidth_limit + 1,
                ),
            ],
        )
        validate_disk_volume_allocation_limits(last_bandwidth_limit + 1)

    def test_add_nonexclusive_to_exclusive(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set(yp_client)
        node_id = create_nodes(
            yp_client,
            node_count=1,
            disk_specs=[
                dict(
                    total_capacity=1000,
                    total_volume_slots=10,
                    storage_class="hdd",
                ),
                dict(
                    total_capacity=2000,
                    total_volume_slots=10,
                    storage_class="ssd",
                ),
            ],
        )[0]

        hdd_resource_id = yp_client.select_objects(
            "resource",
            filter='[/meta/kind] = "disk" and [/spec/disk/storage_class] = "hdd"',
            selectors=["/meta/id"],
        )[0][0]
        ssd_resource_id = yp_client.select_objects(
            "resource",
            filter='[/meta/kind] = "disk" and [/spec/disk/storage_class] = "ssd"',
            selectors=["/meta/id"],
        )[0][0]

        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                disk_volume_requests=[
                    dict(id="hdd", storage_class="hdd", exclusive_policy=dict(min_capacity=500)),
                ],
                node_id=node_id,
            ),
        )

        assert sorted([hdd_resource_id]) == sorted(
            [
                x["resource_id"]
                for x in yp_client.get_object(
                    "pod", pod_id, selectors=["/status/scheduled_resource_allocations"]
                )[0]
            ]
        )

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                {
                    "path": "/spec/disk_volume_requests/end",
                    "value": {
                        "id": "ssd",
                        "storage_class": "ssd",
                        "quota_policy": {"capacity": 600},
                    },
                }
            ],
        )

        assert sorted([hdd_resource_id, ssd_resource_id]) == sorted(
            [
                x["resource_id"]
                for x in yp_client.get_object(
                    "pod", pod_id, selectors=["/status/scheduled_resource_allocations"]
                )[0]
            ]
        )

    def test_update_capacity(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(
            yp_client,
            node_count=1,
            disk_specs=[
                dict(
                    total_capacity=1000,
                    total_volume_slots=10,
                    storage_class="hdd",
                ),
            ],
        )[0]
        hdd_resource_id = yp_client.select_objects(
            "resource",
            filter='[/meta/kind] = "disk" and [/spec/disk/storage_class] = "hdd"',
            selectors=["/meta/id"],
        )[0][0]

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                disk_volume_requests=[
                    dict(id="hdd1", storage_class="hdd", quota_policy=dict(capacity=500)),
                ],
                node_id=node_id,
            ),
        )

        allocations1 = yp_client.get_object(
            "resource", hdd_resource_id, selectors=["/status/scheduled_allocations"]
        )[0]

        # Add new request.
        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                {
                    "path": "/spec/disk_volume_requests/end",
                    "value": {
                        "id": "hdd2",
                        "storage_class": "hdd",
                        "quota_policy": {"capacity": 100},
                    },
                }
            ],
        )

        allocations2 = yp_client.get_object(
            "resource", hdd_resource_id, selectors=["/status/scheduled_allocations"]
        )[0]

        # Change capacity of the request.
        def update_capacity(value):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    {
                        "path": "/spec/disk_volume_requests/0/quota_policy/capacity",
                        "value": YsonUint64(value),
                    }
                ],
            )

        with pytest.raises(YtResponseError):
            update_capacity(555)

        update_node_id(yp_client, pod_id, "")

        update_capacity(555)

        update_node_id(yp_client, pod_id, node_id)

        allocations3 = yp_client.get_object(
            "resource", hdd_resource_id, selectors=["/status/scheduled_allocations"]
        )[0]

        assert len(allocations1) == 1
        assert len(allocations2) == 2
        assert len(allocations3) == 2

        for a in [allocations1, allocations2, allocations3]:
            for b in a:
                assert b["pod_id"] == pod_id

        volume_id1 = allocations1[0]["disk"]["volume_id"]
        volume_id2 = allocations2[1]["disk"]["volume_id"]
        volume_id3 = allocations3[0]["disk"]["volume_id"]
        volume_id4 = allocations3[1]["disk"]["volume_id"]

        assert allocations1[0]["disk"] == {
            "exclusive": False,
            "volume_id": volume_id1,
            "capacity": YsonUint64(500),
            "bandwidth": YsonUint64(0),
        }

        assert allocations2[0]["disk"] == {
            "exclusive": False,
            "volume_id": volume_id1,
            "capacity": YsonUint64(500),
            "bandwidth": YsonUint64(0),
        }
        assert allocations2[1]["disk"] == {
            "exclusive": False,
            "volume_id": volume_id2,
            "capacity": YsonUint64(100),
            "bandwidth": YsonUint64(0),
        }

        assert allocations3[0]["disk"] == {
            "exclusive": False,
            "volume_id": volume_id3,
            "capacity": YsonUint64(555),
            "bandwidth": YsonUint64(0),
        }
        assert allocations3[1]["disk"] == {
            "exclusive": False,
            "volume_id": volume_id4,
            "capacity": YsonUint64(100),
            "bandwidth": YsonUint64(0),
        }

    def test_update_bandwidth_guarantee(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(
            yp_client,
            node_count=1,
            disk_specs=[
                dict(
                    total_bandwidth=1000,
                    read_bandwidth_factor=1.0,
                    write_bandwidth_factor=1.0,
                    read_operation_rate_divisor=1.0,
                    write_operation_rate_divisor=1.0,
                ),
            ],
        )

        capacity = 100

        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            dict(
                enable_scheduling=True,
                disk_volume_requests=[
                    dict(
                        id="disk1",
                        storage_class="hdd",
                        quota_policy=dict(capacity=capacity, bandwidth_guarantee=100),
                    ),
                ],
            ),
        )
        wait_pod_is_assigned(yp_client, pod_id)

        def update(value):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(
                        path="/spec/disk_volume_requests/0/quota_policy/bandwidth_guarantee",
                        value=value,
                    ),
                ],
            )

        def get_resource_disk_only_allocation():
            response = yp_client.select_objects(
                "resource",
                filter='[/meta/kind] = "disk"',
                selectors=["/status/scheduled_allocations"],
            )
            assert 1 == len(response)
            allocations = response[0][0]
            assert 1 == len(allocations)
            return allocations[0]["disk"]

        assert 100 == self._get_pod_disk_volume_only_allocation(yp_client, pod_id)["read_bandwidth_guarantee"]
        assert 100 == get_resource_disk_only_allocation()["bandwidth"]

        # Update with overcommit.
        with pytest.raises(YtResponseError):
            update(1001)
        assert 100 == self._get_pod_disk_volume_only_allocation(yp_client, pod_id)["read_bandwidth_guarantee"]
        assert 100 == get_resource_disk_only_allocation()["bandwidth"]

        # Update without overcommit.
        update(1000)
        assert 1000 == self._get_pod_disk_volume_only_allocation(yp_client, pod_id)["read_bandwidth_guarantee"]
        assert 1000 == get_resource_disk_only_allocation()["bandwidth"]

        previous_allocation_id = get_resource_disk_only_allocation()["volume_id"]
        last_bandwidth = 300
        for bandwidth_guarantee in (100, 0, 200, None, last_bandwidth):
            quota_policy = dict(capacity=capacity)
            if bandwidth_guarantee is not None:
                quota_policy["bandwidth_guarantee"] = bandwidth_guarantee
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(path="/spec/disk_volume_requests/0/quota_policy", value=quota_policy,),
                ],
            )

            pod_allocation = self._get_pod_disk_volume_only_allocation(yp_client, pod_id)
            resource_allocation = get_resource_disk_only_allocation()

            # Various fields.
            assert capacity == resource_allocation["capacity"]
            assert capacity == pod_allocation["capacity"]
            field_names = (
                "read_bandwidth_guarantee",
                "write_bandwidth_guarantee",
                "read_operation_rate_guarantee",
                "write_operation_rate_guarantee",
            )
            for field_name in field_names:
                assert pod_allocation[field_name] == (0 if bandwidth_guarantee is None else bandwidth_guarantee)

            # Relationship between pod and resource allocations.
            assert pod_allocation["volume_id"] == resource_allocation["volume_id"]

            # New allocation id is generated each time.
            assert previous_allocation_id != resource_allocation["volume_id"]
            previous_allocation_id = resource_allocation["volume_id"]

        # Update of both capacity and bandwidth is not allowed.
        def update_both_capacity_and_bandwidth(capacity, bandwidth):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(
                        path="/spec/disk_volume_requests/0/quota_policy",
                        value=dict(capacity=capacity, bandwidth_guarantee=bandwidth),
                    ),
                ],
            )

        with pytest.raises(YtResponseError):
            update_both_capacity_and_bandwidth(capacity - 1, last_bandwidth - 1)
        update_both_capacity_and_bandwidth(capacity, last_bandwidth - 1)

    def test_reallocation_object_relationships(self, yp_env):
        yp_client = yp_env.yp_client

        # One disk can serve exactly two disk volume requests.
        total_capacity = 1000

        # One extra disk.
        create_nodes(
            yp_client,
            node_count=1,
            disk_specs=[
                dict(
                    total_capacity=total_capacity,
                    total_bandwidth=1000,
                ),
                dict(
                    total_capacity=total_capacity,
                    total_bandwidth=1000,
                ),
                dict(
                    total_capacity=total_capacity,
                    total_bandwidth=1000,
                ),
            ],
        )

        # More than one disk request.
        pod_set_id = create_pod_set(yp_client)

        def create_pod():
            return create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                dict(
                    enable_scheduling=True,
                    disk_volume_requests=[
                        dict(
                            id="disk1",
                            storage_class="hdd",
                            quota_policy=dict(capacity=total_capacity // 2, bandwidth_guarantee=100),
                        ),
                        dict(
                            id="disk2",
                            storage_class="hdd",
                            quota_policy=dict(capacity=total_capacity // 2, bandwidth_guarantee=200),
                        ),
                    ],
                ),
            )

        # More than one pod.
        pod_id1 = create_pod()
        pod_id2 = create_pod()
        wait_pods_are_assigned(yp_client, [pod_id1, pod_id2])

        Allocation = collections.namedtuple("Allocation", ["id", "resource_id", "pod_id", "request_id"])

        def get_allocations():
            resource_allocations_response = yp_client.select_objects(
                "resource",
                filter='[/meta/kind] = "disk"',
                selectors=["/meta/id", "/status/scheduled_allocations"],
            )
            resource_allocations = dict()
            for resource_id, allocations in resource_allocations_response:
                for allocation in allocations:
                    id = allocation["disk"]["volume_id"]
                    pod_id = allocation["pod_id"]
                    assert id not in resource_allocations
                    resource_allocations[id] = Allocation(id, resource_id, pod_id, request_id=None)

            pod_allocations_response = yp_client.select_objects(
                "pod",
                selectors=["/meta/id", "/status/disk_volume_allocations"],
            )
            pod_allocations = dict()
            for pod_id, allocations in pod_allocations_response:
                for allocation in allocations:
                    id = allocation["volume_id"]
                    resource_id = allocation["resource_id"]
                    assert id not in pod_allocations
                    pod_allocations[id] = Allocation(id, resource_id, pod_id, request_id=None)

            assert resource_allocations == pod_allocations

            for _, allocations in pod_allocations_response:
                for allocation in allocations:
                    id = allocation["volume_id"]
                    request_id = allocation["id"]
                    pod_allocations[id] = pod_allocations[id]._replace(request_id=request_id)

            return pod_allocations

        def validate_allocations(allocations):
            assert 4 == len(allocations)
            count_per_pod = collections.Counter()
            count_per_resource = collections.Counter()
            for id in allocations:
                allocation = allocations[id]
                count_per_pod[allocation.pod_id] += 1
                count_per_resource[allocation.resource_id] += 1
            assert [2, 2] == list(itervalues(count_per_pod))
            for resource_id in count_per_resource:
                assert 2 >= count_per_resource[resource_id]

        def get_allocation(allocations, pod_id, request_id):
            for id in allocations:
                allocation = allocations[id]
                if allocation.pod_id == pod_id and allocation.request_id == request_id:
                    return allocation
            assert False, 'Could not find the allocation for pod "{}" and request "{}"'.format(pod_id, request_id)

        def remove_allocation(allocations, pod_id, request_id):
            new_allocations = dict()
            for id in allocations:
                allocation = allocations[id]
                if allocation.pod_id != pod_id or allocation.request_id != request_id:
                    assert id not in new_allocations
                    new_allocations[id] = allocation
            return new_allocations

        allocations1 = get_allocations()
        validate_allocations(allocations1)

        pod1_disk1_resource_id = get_allocation(allocations1, pod_id1, "disk1").resource_id
        allocations1 = remove_allocation(allocations1, pod_id1, "disk1")

        for bandwidth in range(1, 10):
            yp_client.update_object(
                "pod",
                pod_id1,
                set_updates=[
                    dict(
                        path="/spec/disk_volume_requests/0/quota_policy/bandwidth_guarantee",
                        value=bandwidth,
                    ),
                ],
            )
            allocations2 = get_allocations()
            validate_allocations(allocations2)

            # New allocation must be created at the same resource.
            new_pod1_disk1_resource_id = get_allocation(allocations2, pod_id1, "disk1").resource_id
            assert pod1_disk1_resource_id == new_pod1_disk1_resource_id

            # Other allocations must match exactly.
            allocations2 = remove_allocation(allocations2, pod_id1, "disk1")
            assert allocations1 == allocations2
