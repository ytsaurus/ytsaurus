import pytest

from yt.yson import YsonEntity
from yp.client import YpResponseError
from yt.environment.helpers import wait

@pytest.mark.usefixtures("yp_env")
class TestNet(object):
    def test_invalid_pod_vlan_id(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }})

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet"}
                ]
            }})

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                }
            })

        with pytest.raises(YpResponseError):
            yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": node_id}])

    def test_invalid_pod_network_id(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet"}
                ]
            }})

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                },
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}
                    ]
                }
            })

        with pytest.raises(YpResponseError):
            yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": node_id}])

    def test_assign_pod_ip6_address_manual(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }})

        pod_set_id = yp_client.create_object("pod_set")

        address_labels = {"a": "b"}
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet", "labels": address_labels},
                    {"vlan_id": "somevlan", "manual_address": "a:b:c:d:a:b:c:d", "labels": address_labels}
                ]
            }})

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                },
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}
                    ]
                }
            })

        yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": node_id}])
        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 2
        assert allocations[0]["vlan_id"] == "somevlan"
        assert allocations[0]["address"].startswith("1:2:3:4:0:7b:")
        assert allocations[0]["labels"] == address_labels
        assert allocations[0]["manual"] == False
        assert allocations[1]["vlan_id"] == "somevlan"
        assert allocations[1]["address"] == "a:b:c:d:a:b:c:d"
        assert allocations[1]["labels"] == address_labels
        assert allocations[1]["manual"] == True

        assert len(list(yt_client.select_rows("* from [//yp/db/ip6_nonces]"))) == 1

        yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": ""}])
        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert allocations == YsonEntity()

        assert len(list(yt_client.select_rows("* from [//yp/db/ip6_nonces]"))) == 0

        yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": node_id}])

        assert len(list(yt_client.select_rows("* from [//yp/db/ip6_nonces]"))) == 1

        yp_client.remove_object("pod", pod_id)

        assert len(list(yt_client.select_rows("* from [//yp/db/ip6_nonces]"))) == 0

    def test_pod_ip6_address_fqdn(self, yp_env):
        yp_client = yp_env.yp_client
        
        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }})

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": False},
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True},
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True, "dns_prefix": "abc"}
                ]
            }})

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                },
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}
                    ]
                }
            })

        yp_client.update_object("pod", pod_id,  set_updates=[{"path": "/spec/node_id", "value": node_id}])
        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 3
        assert "persistent_fqdn" not in allocations[0]
        assert "transient_fqdn" not in allocations[0]
        assert allocations[1]["persistent_fqdn"] == "{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[1]["transient_fqdn"] == "{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)
        assert allocations[2]["persistent_fqdn"] == "abc.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[2]["transient_fqdn"] == "abc.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)
                
    def test_assign_pod_ip6_address_scheduler(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }
        })

        yp_client.create_object("node_segment", attributes={
            "meta": {
                "id": "all"
            },
            "spec": {
                "node_filter": "0=0"
            }
        })

        pod_set_id = yp_client.create_object("pod_set", attributes={
            "spec": {
                "node_segment_id": "all"
            }
        })

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                },
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}
                    ],
                }
            })
        yp_client.update_hfsm_state(node_id, "up", "Test")
           
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet"},
                ],
                "enable_scheduling": True
            }})

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["vlan_id"] == "somevlan"
        assert allocations[0]["address"].startswith("1:2:3:4:0:7b:")
        assert allocations[0]["manual"] == False

