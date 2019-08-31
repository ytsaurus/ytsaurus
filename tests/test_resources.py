from .conftest import create_pod_set_with_quota, create_nodes

from yp.common import YtResponseError, wait

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

import pytest

import random


@pytest.mark.usefixtures("yp_env")
class TestResources(object):
    def test_node_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("resource", attributes={"spec": {"cpu": {"total_capacity": 100}}})

    def test_kind_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        with pytest.raises(YtResponseError):
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {}})

    def test_kind_set_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        cpu_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"cpu": {"total_capacity": 1}}
        })
        memory_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"memory": {"total_capacity": 1}}
        })
        disk_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"disk": {"total_capacity": 1000, "storage_class": "hdd", "device": "/dev/null"}}
        })
        slot_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"slot": {"total_capacity": 1}}
        })
        gpu_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"gpu": {"model": "v100", "total_memory": 200000, "uuid": "abc"}}
        })
        network_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"network": {"total_bandwidth": 64}}
        })

        assert yp_client.get_object("resource", cpu_resource_id, selectors=["/meta/kind"])[0] == "cpu"
        assert yp_client.get_object("resource", memory_resource_id, selectors=["/meta/kind"])[0] == "memory"
        assert yp_client.get_object("resource", disk_resource_id, selectors=["/meta/kind"])[0] == "disk"
        assert yp_client.get_object("resource", slot_resource_id, selectors=["/meta/kind"])[0] == "slot"
        assert yp_client.get_object("resource", gpu_resource_id, selectors=["/meta/kind"])[0] == "gpu"
        assert yp_client.get_object("resource", network_resource_id, selectors=["/meta/kind"])[0] == "network"

    def test_cannot_change_kind(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 1}}})
        with pytest.raises(YtResponseError):
            yp_client.update_object("resource", resource_id, set_updates=[
                    {"path": "/spec", "value": {"memory": {"total_capacity": 1}}}
                ])

    def test_get(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"memory": {"total_capacity": 1000}}
        })
        result = yp_client.get_object("resource", resource_id, selectors=[
            "/meta/kind",
            "/spec/memory/total_capacity",
            "/meta/id",
            "/meta/node_id"
        ])
        assert result[0] == "memory"
        assert result[1] == 1000
        assert result[2] == resource_id
        assert result[3] == node_id

    def test_parent_node_must_exist(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object(object_type="resource", attributes={
                "meta": {"node_id": "nonexisting_node_id"}
            })

    def test_resource_create_destroy(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        node_id = yp_client.create_object(object_type="node")
        resource_attributes = {
            "meta": {"node_id": node_id},
            "spec": {"disk": {"total_capacity": 1000, "storage_class": "hdd", "device": "/dev/null"}}
        }
        resource_ids = [yp_client.create_object(object_type="resource", attributes=resource_attributes)
                        for i in xrange(10)]

        def get_counts():
            return (len(list(yt_client.select_rows("* from [//yp/db/nodes] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/resources] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/parents]"))))

        assert get_counts() == (1, 10, 10)

        yp_client.remove_object("resource", resource_ids[0])

        assert get_counts() == (1, 9, 9)

        yp_client.remove_object("node", node_id)

        assert get_counts() == (0, 0, 0)

    def test_node_segment_totals(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": '[/labels/status] = "good"'}})

        def create_bad_node():
            node_id = yp_client.create_object("node", attributes={"labels": {"status": "bad"}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 100}}})
            yp_client.create_object("resource", attributes={
                "meta": {"node_id": node_id},
                "spec": {
                    "gpu": {"model": "v100", "total_memory": 3000, "uuid": node_id + "gpu"}
                }
            })
            return node_id

        def create_good_node():
            node_id = yp_client.create_object("node", attributes={"labels": {"status": "good"}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 1}}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"memory": {"total_capacity": 10}}})
            yp_client.create_object(
                "resource",
                attributes={
                    "meta": {"node_id": node_id},
                    "spec": {"disk": {"total_capacity": 100, "storage_class": "hdd", "total_bandwidth": 3}}
                }
            )
            yp_client.create_object("resource", attributes={
                "meta": {"node_id": node_id},
                "spec": {
                    "gpu": {"model": "v100", "total_memory": 3000, "uuid": node_id + "gpu"}
                }
            })
            return node_id

        for _ in xrange(5):
            create_bad_node()

        for _ in xrange(5):
            node_id = create_good_node()

        for _ in xrange(5):
            node_id = create_good_node()
            yp_client.update_object("node", node_id, set_updates=[{
                "path": "/control/update_hfsm_state",
                "value": {"state": "up", "message": "test"}}])

        def _check(totals_field, n):
            totals = yp_client.get_object("node_segment", segment_id, selectors=["/status/" + totals_field])[0]
            return totals != YsonEntity() and \
                   totals["cpu"]["capacity"] == n and \
                   totals["memory"]["capacity"] == n * 10 and \
                   totals["disk_per_storage_class"] == {"hdd": {"capacity": n * 100, "bandwidth": n * 3}} and \
                   totals["gpu_per_model"] == {"v100": {"capacity": n}}

        wait(lambda: _check("total_resources", 10))
        wait(lambda: _check("schedulable_resources", 5))

    def test_gpu_allocation_optimality(self, yp_env):
        '''
            This test checks that GPUs requests are satisfied in an optimal way
            (each request is satisfied with a GPU with the smallest possible memory capacity).
        '''
        yp_client = yp_env.yp_client

        memory_capacities = [2 ** i for i in range(10)]
        random.seed(42)
        random.shuffle(memory_capacities)

        node_id = create_nodes(yp_client, 1, gpu_specs=[dict(model="v100", total_memory=capacity)
                                                        for capacity in memory_capacities])[0]

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=len(memory_capacities)))[0]
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "node_id": node_id,
                "gpu_requests": [
                    {
                        "id": "gpu{}".format(capacity),
                        "model": "v100",
                        "min_memory": capacity,
                    }
                    for capacity in memory_capacities
                ]
            }
        })

        gpu_resources = yp_client.select_objects("resource",
            filter='[/meta/node_id] = "{}" and [/meta/kind] = "gpu"'.format(node_id),
            selectors=["/meta/id", "/spec/gpu/uuid", "/spec/gpu/total_memory"])
        get_pod_id = lambda resource_id: yp_client.get_object("resource", resource_id,
                                                selectors=["/status/scheduled_allocations/0/pod_id"])[0]
        wait(lambda: all(get_pod_id(resource[0]) == pod_id for resource in gpu_resources))

        gpu_allocations = yp_client.get_object("pod", pod_id, selectors=["/status/gpu_allocations"])[0]
        for capacity, allocation in zip(memory_capacities, gpu_allocations):
            resource_id, uuid, _ = [gpu for gpu in gpu_resources if gpu[2] == capacity][0]
            assert allocation["request_id"] == "gpu{}".format(capacity)
            assert allocation["resource_id"] == resource_id
            assert allocation["device_uuid"] == uuid

    def test_gpu_allocation_no_shuffle(self, yp_env):
        yp_client = yp_env.yp_client
        node_id = create_nodes(yp_client, 1, cpu_total_capacity=1000,
                               gpu_specs=[dict(model="v100", total_memory=2 ** 10)] * 50)[0]

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100))[0]
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "node_id": node_id,
                "gpu_requests": [
                    {
                        "id": "gpu{}".format(i),
                        "model": "v100",
                    }
                    for i in range(50)
                ]
            }
        })

        def get_request_id_to_allocation_id():
            request_id_to_allocation_id = {}
            allocations = yp_client.get_object("pod", pod_id, selectors=["/status/gpu_allocations"])
            for alloc in allocations:
                request_id_to_allocation_id[alloc[0]["request_id"]] = alloc[0]["id"]
            return request_id_to_allocation_id

        old_request_id_to_allocation_id = get_request_id_to_allocation_id()
        gpu_allocations = yp_client.select_objects("resource",
                                                   filter="[/meta/node_id] = \"{}\" and [/meta/kind] = \"gpu\"",
                                                   selectors=["/status/scheduled_allocations/0/gpu/allocation_id"])
        for alloc_id in gpu_allocations:
            assert alloc_id[0] in old_request_id_to_allocation_id.values()

        yp_client.update_object("pod", pod_id, remove_updates=[dict(path="/spec/gpu_requests/15")])
        new_request_id_to_allocation_id = get_request_id_to_allocation_id()

        for key, value in new_request_id_to_allocation_id.items():
            assert old_request_id_to_allocation_id[key] == value

        yp_client.update_object("pod", pod_id, set_updates=[
            dict(path="/spec/gpu_requests/end", value=dict(id="new_gpu", model="v100", min_memory=2 ** 10))
        ])

        new_request_id_to_allocation_id = get_request_id_to_allocation_id()
        for key, value in new_request_id_to_allocation_id.items():
            if key != "new_gpu":
                assert old_request_id_to_allocation_id[key] == value

    def test_gpu_allocation_node_resource_change(self, yp_env):
        yp_client = yp_env.yp_client
        node_id = create_nodes(yp_client, 1, cpu_total_capacity=1000,
                               gpu_specs=[dict(model="v100", total_memory=2 ** 10)] * 50)[0]

        pod_set_id = create_pod_set_with_quota(yp_client, gpu_quota=dict(v100=100, k200=100))[0]
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "node_id": node_id,
                "gpu_requests": [
                    {
                        "id": "gpu0",
                        "model": "v100",
                        "min_memory": 2 ** 10,
                    }
                ]
            }
        })

        get_gpu_id = lambda: yp_client.get_object("pod", pod_id,
                                                  selectors=["/status/gpu_allocations/0/resource_id"])[0]
        old_gpu_id = get_gpu_id()
        yp_client.update_object("resource", old_gpu_id,
                                set_updates=[dict(path="/spec/gpu/total_memory", value=0)])
        assert get_gpu_id() == old_gpu_id

        yp_client.update_object("resource", old_gpu_id,
                                set_updates=[dict(path="/spec/gpu/model", value="k200")])
        assert get_gpu_id() == old_gpu_id

    def test_resource_used_free_status(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = create_pod_set_with_quota(
            yp_client,
            bandwidth_quota=500,
            gpu_quota=dict(v100=1),
            disk_quota=dict(hdd=dict(capacity=300)))[0]

        node_id = yp_client.create_object("node")

        cpu_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "cpu": {"total_capacity": 100}
            }
        })
        memory_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "memory": {"total_capacity": 200}
            }
        })
        disk_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "disk": {"total_capacity": 1000, "total_volume_slots": 10, "supported_policies": ["quota"], "storage_class": "hdd", "device": "/dev/null"}
            }
        })
        gpu_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "gpu": {"model": "v100", "total_memory": 3000, "uuid": "123"}
            }
        })
        slot_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "slot": {"total_capacity": 300}
            }
        })
        network_resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {
                "network": {"total_bandwidth": 2000}
            }
        })

        assert yp_client.get_object("resource", cpu_resource_id, selectors=["/status/free/cpu/capacity", "/status/used/cpu/capacity"]) == [100, 0]
        assert yp_client.get_object("resource", memory_resource_id, selectors=["/status/free/memory/capacity", "/status/used/memory/capacity"]) == [200, 0]
        assert yp_client.get_object("resource", disk_resource_id, selectors=["/status/free/disk/capacity", "/status/used/disk/capacity"]) == [1000, 0]
        assert yp_client.get_object("resource", slot_resource_id, selectors=["/status/free/slot/capacity", "/status/used/slot/capacity"]) == [300, 0]
        assert yp_client.get_object("resource", gpu_resource_id, selectors=["/status/free/gpu/capacity", "/status/used/gpu/capacity"]) == [1, 0]
        assert yp_client.get_object("resource", network_resource_id, selectors=["/status/free/network/bandwidth", "/status/used/network/bandwidth"]) == [2000, 0]

        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "node_id": node_id,
                "resource_requests": {
                    "vcpu_guarantee": 30,
                    "vcpu_limit": 40,
                    "memory_guarantee": 10,
                    "memory_limit": 20,
                    "slot": 1,
                    "network_bandwidth_guarantee": 500,
                },
                "disk_volume_requests": [
                    {
                        "id": "hdd",
                        "storage_class": "hdd",
                        "quota_policy": {"capacity": 300}
                    }
                ],
                "gpu_requests": [
                    {
                        "id": "mygpu",
                        "model": "v100",
                        "min_memory": 1000,
                    }
                ]
            }
        })

        assert yp_client.get_object("resource", cpu_resource_id, selectors=["/status/free/cpu/capacity", "/status/used/cpu/capacity"]) == [70, 30]
        assert yp_client.get_object("resource", memory_resource_id, selectors=["/status/free/memory/capacity", "/status/used/memory/capacity"]) == [180, 20]
        assert yp_client.get_object("resource", disk_resource_id, selectors=["/status/free/disk/capacity", "/status/used/disk/capacity"]) == [700, 300]
        assert yp_client.get_object("resource", slot_resource_id, selectors=["/status/free/slot/capacity", "/status/used/slot/capacity"]) == [299, 1]
        assert yp_client.get_object("resource", gpu_resource_id, selectors=["/status/free/gpu/capacity", "/status/used/gpu/capacity"]) == [0, 1]
        assert yp_client.get_object("resource", network_resource_id, selectors=["/status/free/network/bandwidth", "/status/used/network/bandwidth"]) == [1500, 500]

        yp_client.remove_object("pod", pod_id)

        wait(lambda: yp_client.get_object("resource", cpu_resource_id, selectors=["/status/free/cpu/capacity", "/status/used/cpu/capacity"]) == [100, 0] and \
                     yp_client.get_object("resource", memory_resource_id, selectors=["/status/free/memory/capacity", "/status/used/memory/capacity"]) == [200, 0] and \
                     yp_client.get_object("resource", disk_resource_id, selectors=["/status/free/disk/capacity", "/status/used/disk/capacity"]) == [1000, 0] and \
                     yp_client.get_object("resource", slot_resource_id, selectors=["/status/free/slot/capacity", "/status/used/slot/capacity"]) == [300, 0] and \
                     yp_client.get_object("resource", gpu_resource_id, selectors=["/status/free/gpu/capacity", "/status/used/gpu/capacity"]) == [1, 0] and \
                     yp_client.get_object("resource", network_resource_id, selectors=["/status/free/network/bandwidth", "/status/used/network/bandwidth"]) == [2000, 0])
