import pytest

from yt.yson import YsonEntity, YsonUint64
from yt.environment.helpers import wait

@pytest.mark.usefixtures("yp_env")
class TestInternetAddresses(object):
    def _wait_scheduled_state(self, yp_client, pod_ids, state):
        def func():
            for pod_id in pod_ids:
                if yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == state:
                    return True
            return False
        wait(func)

    def _wait_scheduled_error(self, yp_client, pod_ids):
        def func():
            for pod_id in pod_ids:
                if not isinstance(yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"]), YsonEntity):
                    return True
            return False
        wait(func)

    def _prepare_objects(self, yp_client):
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

        return pod_set_id

    def _create_inet_addr(self, yp_client, network_module_id, addr):
        inet_addr_id = yp_client.create_object("internet_address", attributes={
                "spec": {
                    "ip4_address": addr,
                    "network_module_id": network_module_id,
                }
            })
        return inet_addr_id

    def _create_node(self, yp_client, network_module_id=None):
        node_id = yp_client.create_object("node", attributes={
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}
                    ],
                    "network_module_id": network_module_id,
                }
            })
        yp_client.update_hfsm_state(node_id, "up", "Test")

        return node_id
        
    def _create_pod(self, yp_client, pod_set_id, enable_internet):
        pod_id = yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_internet": enable_internet},
                ],
                "enable_scheduling": True
            }})

        return pod_id

    def test_create_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        inet_addr_id = yp_client.create_object(
            object_type="internet_address",
            attributes={
                "meta": {"id": "inet_addr"},
                "spec": {
                    "ip4_address": "1.2.3.4",
                    "network_module_id": "VLA01,VLA02,VLA03"
                },
                "status": {
                    "pod_id": "42",
                },
            })

        result = yp_client.get_object("internet_address", inet_addr_id, selectors=["/meta", "/spec", "/status"])
        assert result[0]["id"] == "inet_addr"
        assert result[1]["ip4_address"] == "1.2.3.4"
        assert result[1]["network_module_id"] == "VLA01,VLA02,VLA03"
        assert result[2]["pod_id"] == "42"

    def test_schedule_simple(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        node_id = self._create_node(yp_client, "netmodule1")
        inet_addr_id = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")
        pod_id = self._create_pod(yp_client, pod_set_id, enable_internet=True)

        self._wait_scheduled_state(yp_client, [pod_id], "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["internet_address"] == {'ip4_address': '1.2.3.4', 'id': inet_addr_id}
        assert yp_client.get_object("internet_address", inet_addr_id, selectors=["/status"])[0]["pod_id"] == pod_id

    def test_miss_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        inet_addr_id1 = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")

        node_id1 = self._create_node(yp_client, "netmodule1")
        node_id2 = self._create_node(yp_client, "netmodule1")

        pod_id1 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        pod_id2 = self._create_pod(yp_client, pod_set_id, enable_internet=True)

        self._wait_scheduled_state(yp_client, [pod_id1, pod_id2], "assigned")
        self._wait_scheduled_error(yp_client, [pod_id1, pod_id2])

        assert not isinstance(yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/error"])[0], YsonEntity) or \
               not isinstance(yp_client.get_object("pod", pod_id2, selectors=["/status/scheduling/error"])[0], YsonEntity)
        assert isinstance(yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/error"])[0], YsonEntity) or \
               isinstance(yp_client.get_object("pod", pod_id2, selectors=["/status/scheduling/error"])[0], YsonEntity)

        inet_addr_id2 = self._create_inet_addr(yp_client, "netmodule1", "2.3.4.5")

        self._wait_scheduled_state(yp_client, [pod_id1], "assigned")
        self._wait_scheduled_state(yp_client, [pod_id2], "assigned")

        assert set([inet_addr_id1, inet_addr_id2]) == set(map(lambda x: x[0][0]["internet_address"]["id"], yp_client.select_objects("pod", selectors=["/status/ip6_address_allocations"])))

    def test_schedule_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)

        for i in xrange(1, 256):
            self._create_inet_addr(yp_client, "netmodule1", "1.2.3.{}".format(i))
        for i in xrange(1, 20):
            self._create_node(yp_client, "netmodule2")
        
        node_ids = set([self._create_node(yp_client, "netmodule1") for i in range(2)])
        pod_ids = [self._create_pod(yp_client, pod_set_id, enable_internet=True) for i in range(10)]

        for pod_id in pod_ids:
            self._wait_scheduled_state(yp_client, [pod_id], "assigned")
            assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0] in node_ids

    def test_reassign_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        node_id  = self._create_node(yp_client, "netmodule1")
        inet_addr_id = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")

        pod_id1 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id1], "assigned")

        pod_id2 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_error(yp_client, [pod_id2])

        assert yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/node_id"])[0] == node_id
        assert yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0] == pod_id1

        yp_client.remove_object("pod", pod_id1)
        self._wait_scheduled_state(yp_client, [pod_id2], "assigned")

        assert yp_client.get_object("pod", pod_id2, selectors=["/status/scheduling/node_id"])[0] == node_id
        assert yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0] == pod_id2

        yp_client.remove_object("pod", pod_id2)
        assert isinstance(yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0], YsonEntity)
