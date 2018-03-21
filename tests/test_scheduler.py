import pytest

from yp.client import YpResponseError
from yt.yson import YsonEntity, YsonUint64
from yt.environment.helpers import wait
from collections import defaultdict

ALL_NODES_SEGMENT = "all"

@pytest.mark.usefixtures("yp_env")
class TestScheduler(object):
    def _create_all_nodes_segment(self, yp_env):
        yp_client = yp_env.yp_client
        
        yp_client.create_object("node_segment", attributes={
            "meta": {"id": ALL_NODES_SEGMENT},
            "spec": {"node_filter": "0=0"}
        })

    def _create_nodes(self, yp_env, node_count, rack_count = 1, hfsm_state="up"):
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
                            "rack": "rack-{}".format(i / (node_count / rack_count)),
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
                        "kind": "cpu",
                        "total_capacity": 100
                    }
                })
            yp_client.create_object("resource", attributes={
                    "meta": {
                        "node_id": node_id
                    },
                    "spec": {
                        "kind": "memory",
                        "total_capacity": 1000000000
                    }
                })
            yp_client.create_object("resource", attributes={
                    "meta": {
                        "node_id": node_id
                    },
                    "spec": {
                        "kind": "disk",
                        "total_capacity": 100000000000
                    }
                })
        return node_ids

    def _get_scheduled_allocations(self, yp_env, node_id, kind):
        yp_client = yp_env.yp_client
        results = yp_client.select_objects("resource", filter="[/spec/kind] = \"{}\" and [/meta/node_id] = \"{}\"".format(kind, node_id),
                                           selectors=["/status/scheduled_allocations"])
        assert len(results) == 1
        return results[0][0]

    def test_create_with_enabled_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        node_id = yp_client.create_object("node")
        with pytest.raises(YpResponseError):
            yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "enable_scheduling": True,
                        "node_id": node_id
                    }
                })

    def test_force_assign(self, yp_env):
        yp_client = yp_env.yp_client
        
        node_ids = self._create_nodes(yp_env, 1)
        node_id = node_ids[0]

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "enable_scheduling": False,
                    "node_id": node_id
                }
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["assigned"]

        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == YsonEntity()
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == YsonEntity()

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
        
        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "enable_scheduling": False
                }
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"]) == ["disabled"]

        node_ids = self._create_nodes(yp_env, 10)

        with pytest.raises(YpResponseError):
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

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "resource_requests": {
                        "vcpu_guarantee": 100
                    },
                    "enable_scheduling": True
                }
            })

        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "pending"

        self._create_nodes(yp_env, 10)

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] != "" and
                     yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == [{"pod_id": pod_id, "capacity": 100}]
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == YsonEntity()

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/resource_requests/vcpu_guarantee", "value": YsonUint64(50)}])
        assert self._get_scheduled_allocations(yp_env, node_id, "cpu") == [{"pod_id": pod_id, "capacity": 50}]
        assert self._get_scheduled_allocations(yp_env, node_id, "memory") == YsonEntity()

    def test_cpu_limit(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        for i in xrange(10):
            pod_id = yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "resource_requests": {
                            "vcpu_guarantee": 30
                        },
                        "enable_scheduling": True
                    }
                })
        self._create_nodes(yp_env, 1)

        wait(lambda: len(yp_client.select_objects("pod", filter="[/status/scheduling/state] = \"assigned\"", selectors=["/meta/id"])) > 0)
        assert len(yp_client.select_objects("pod", filter="[/status/scheduling/state] = \"assigned\"", selectors=["/meta/id"])) == 3

        node_id = yp_client.select_objects("node", selectors=["/meta/id"])[0][0]
        unassigned_pod_id = yp_client.select_objects("pod", filter="[/status/scheduling/state] != \"assigned\"", selectors=["/meta/id"])[0][0]
        with pytest.raises(YpResponseError):
            yp_client.update_object("pod", unassigned_pod_id, set_updates=[{"path": "/spec/node_id", "value": node_id}])

    def test_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 10)
        good_node_id = node_ids[5]
        yp_client.update_object("node", good_node_id, set_updates=[
            {"path": "/labels/status", "value": "good"}
        ])

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "resource_requests": {
                        "vcpu_guarantee": 100
                    },
                    "enable_scheduling": True,
                    "node_filter" : '[/labels/status] = "good"'
                }
            })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == good_node_id)

    def test_malformed_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_all_nodes_segment(yp_env)
        self._create_nodes(yp_env, 10)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "enable_scheduling": True,
                    "node_filter" : '[/some/nonexisting] = "value"'
                }
            })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == YsonEntity())

    def _wait_for_pod_assignment(self, yp_env):
        yp_client = yp_env.yp_client
        wait(lambda: all(x[0] == "assigned"
                         for x in yp_client.select_objects("pod", selectors=["/status/scheduling/state"])))

    def test_antiaffinity_per_node(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_all_nodes_segment(yp_env)
        self._create_nodes(yp_env, 10)
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "spec": {
                    "antiaffinity_constraints": [
                        {"key": "node", "max_pods": 1}
                    ],
                    "node_segment_id": ALL_NODES_SEGMENT
                }
            })
        for i in xrange(10):
            yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "enable_scheduling": True
                    }
                })
        
        self._wait_for_pod_assignment(yp_env)
        node_ids = set(x[0] for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))
        assert len(node_ids) == 10

    def test_antiaffinity_per_node_and_rack(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_all_nodes_segment(yp_env)
        self._create_nodes(yp_env, 10, 2)
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "spec": {
                    "antiaffinity_constraints": [
                        {"key": "node", "max_pods": 1},
                        {"key": "rack", "max_pods": 3}
                    ],
                    "node_segment_id": ALL_NODES_SEGMENT
                }
            })
        for i in xrange(6):
            yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "enable_scheduling": True
                    }
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

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        for _ in xrange(10):
            yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "enable_scheduling": True
                    }
                })

        self._wait_for_pod_assignment(yp_env)
        assert all(x[0] == up_node_id for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))

    def test_node_maintenance(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = self._create_nodes(yp_env, 1)
        node_id = node_ids[0]

        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": ALL_NODES_SEGMENT}})
        pod_ids = []
        for _ in xrange(10):
            pod_ids.append(yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "enable_scheduling": True
                    }
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
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "enable_scheduling": True
            }
        })

        self._wait_for_pod_assignment(yp_env)
        assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] == good_node_id

    def test_invalid_node_filter_at_pod(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 1)
        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": "all"}})
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "enable_scheduling": True,
                "node_filter": "invalid!!!"
            }
        })

        wait(lambda: "error" in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0])

    def test_invalid_network_project_in_pod_spec(self, yp_env):
        yp_client = yp_env.yp_client

        self._create_nodes(yp_env, 1)
        self._create_all_nodes_segment(yp_env)
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": "all"}})
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "backbone", "network_id": "nonexisting"}
                ],
                "enable_scheduling": True
            }
        })

        wait(lambda: "error" in yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0])
