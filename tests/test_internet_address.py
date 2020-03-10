from .conftest import ZERO_RESOURCE_REQUESTS

from yp.local import DEFAULT_IP4_ADDRESS_POOL_ID

from yp.common import wait, YtResponseError

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env")
class TestInternetAddresses(object):
    def _wait_scheduled_state(self, yp_client, pod_ids, state):
        def func():
            for pod_id in pod_ids:
                if (
                    yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0]
                    == state
                ):
                    return True
            return False

        wait(func)

    def _wait_scheduled_error(self, yp_client, pod_ids):
        def func():
            for pod_id in pod_ids:
                if not isinstance(
                    yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"])[0],
                    YsonEntity,
                ):
                    return True
            return False

        wait(func)

    def _prepare_objects(self, yp_client, account_id=None):
        yp_client.create_object(
            "network_project", attributes={"meta": {"id": "somenet"}, "spec": {"project_id": 123}}
        )

        pod_set_spec = {"node_segment_id": "default"}
        if account_id is not None:
            pod_set_spec["account_id"] = account_id

        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": pod_set_spec})

        return pod_set_id

    def _create_inet_addr(
        self, yp_client, network_module_id, addr, ip4_address_pool_id=DEFAULT_IP4_ADDRESS_POOL_ID
    ):
        inet_addr_id = yp_client.create_object(
            "internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": ip4_address_pool_id},
                "spec": {"ip4_address": addr, "network_module_id": network_module_id},
            },
        )
        return inet_addr_id

    def _create_node(self, yp_client, network_module_id=None):
        node_id = yp_client.create_object(
            "node",
            attributes={
                "spec": {
                    "ip6_subnets": [{"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"}],
                    "network_module_id": network_module_id,
                }
            },
        )
        yp_client.update_hfsm_state(node_id, "up", "Test")

        yp_client.create_object(
            "resource",
            attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 100}}},
        )

        yp_client.create_object(
            "resource",
            attributes={"meta": {"node_id": node_id}, "spec": {"slot": {"total_capacity": 300}}},
        )

        return node_id

    def _create_pod(self, yp_client, pod_set_id, enable_internet, resource_request=None):
        if resource_request is None:
            resource_request = ZERO_RESOURCE_REQUESTS

        pod_id = yp_client.create_object(
            "pod",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "resource_requests": resource_request,
                    "ip6_address_requests": [
                        {
                            "vlan_id": "somevlan",
                            "network_id": "somenet",
                            "enable_internet": enable_internet,
                        },
                    ],
                    "enable_scheduling": True,
                },
            },
        )

        return pod_id

    def test_create_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        inet_addr_id = yp_client.create_object(
            object_type="internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": DEFAULT_IP4_ADDRESS_POOL_ID, "id": "inet_addr"},
                "spec": {"ip4_address": "1.2.3.4", "network_module_id": "VLA01,VLA02,VLA03"},
                "status": {"pod_id": "42"},
            },
        )

        result = yp_client.get_object(
            "internet_address", inet_addr_id, selectors=["/meta", "/spec", "/status"]
        )
        assert result[0]["id"] == "inet_addr"
        assert result[1]["ip4_address"] == "1.2.3.4"
        assert result[1]["network_module_id"] == "VLA01,VLA02,VLA03"
        assert result[2]["pod_id"] == "42"

    def test_schedule_simple(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        self._create_node(yp_client, "netmodule1")
        inet_addr_id = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")
        pod_id = self._create_pod(yp_client, pod_set_id, enable_internet=True)

        self._wait_scheduled_state(yp_client, [pod_id], "assigned")

        allocations = yp_client.get_object(
            "pod", pod_id, selectors=["/status/ip6_address_allocations"]
        )[0]
        assert len(allocations) == 1
        assert allocations[0]["internet_address"] == {"ip4_address": "1.2.3.4", "id": inet_addr_id}
        assert (
            yp_client.get_object("internet_address", inet_addr_id, selectors=["/status"])[0][
                "pod_id"
            ]
            == pod_id
        )

    def test_miss_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        inet_addr_id1 = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")

        self._create_node(yp_client, "netmodule1")
        self._create_node(yp_client, "netmodule1")

        pod_id1 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        pod_id2 = self._create_pod(yp_client, pod_set_id, enable_internet=True)

        self._wait_scheduled_state(yp_client, [pod_id1, pod_id2], "assigned")
        self._wait_scheduled_error(yp_client, [pod_id1, pod_id2])

        assert isinstance(
            yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/error"])[0],
            YsonEntity,
        ) != isinstance(
            yp_client.get_object("pod", pod_id2, selectors=["/status/scheduling/error"])[0],
            YsonEntity,
        )

        inet_addr_id2 = self._create_inet_addr(yp_client, "netmodule1", "2.3.4.5")

        self._wait_scheduled_state(yp_client, [pod_id1], "assigned")
        self._wait_scheduled_state(yp_client, [pod_id2], "assigned")

        assert set([inet_addr_id1, inet_addr_id2]) == set(
            map(
                lambda x: x[0][0]["internet_address"]["id"],
                yp_client.select_objects("pod", selectors=["/status/ip6_address_allocations"]),
            )
        )

    def test_schedule_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)

        for i in xrange(1, 256):
            self._create_inet_addr(yp_client, "some.module", "1.2.3.{}".format(i))

        node_ids = set([self._create_node(yp_client, "other.module") for i in range(2)])
        pod_ids = [self._create_pod(yp_client, pod_set_id, enable_internet=True) for i in range(10)]

        for pod_id in pod_ids:
            self._wait_scheduled_state(yp_client, [pod_id], "assigned")
            assert (
                yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
                in node_ids
            )

    def test_internet_address_resource_limit(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object(
            "account",
            attributes={
                "spec": {
                    "resource_limits": {"per_segment": {"default": {"cpu": {"capacity": 100}}}}
                }
            },
        )

        pod_set_id = self._prepare_objects(yp_client, account_id=account_id)
        node_id = self._create_node(yp_client, "netmodule1")

        for i in xrange(3):
            self._create_inet_addr(yp_client, "netmodule1", "1.2.3.{}".format(i))

        pod_id1 = self._create_pod(
            yp_client,
            pod_set_id,
            enable_internet=True,
            resource_request={"vcpu_guarantee": 10, "memory_limit": 0},
        )
        self._wait_scheduled_state(yp_client, [pod_id1], "assigned")
        assert (
            yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )
        assert (
            yp_client.get_object(
                "account",
                account_id,
                selectors=["/status/resource_usage/per_segment/default/internet_address/capacity"],
            )[0]
            == 1
        )

        yp_client.update_object(
            "account",
            account_id,
            set_updates=[
                {
                    "path": "/spec/resource_limits/per_segment/default/internet_address",
                    "value": {"capacity": 0},
                }
            ],
        )

        with pytest.raises(YtResponseError):
            self._create_pod(yp_client, pod_set_id, enable_internet=True)

        pod_id_without_address1 = self._create_pod(
            yp_client,
            pod_set_id,
            enable_internet=False,
            resource_request={"vcpu_guarantee": 10, "memory_limit": 0},
        )
        self._wait_scheduled_state(yp_client, [pod_id_without_address1], "assigned")

        with pytest.raises(YtResponseError):
            self._create_pod(
                yp_client,
                pod_set_id,
                enable_internet=False,
                resource_request={"vcpu_guarantee": 81, "memory_limit": 0},
            )

        yp_client.remove_object("pod", pod_id1)
        wait(
            lambda: yp_client.get_object(
                "account",
                account_id,
                selectors=["/status/resource_usage/per_segment/default/internet_address/capacity"],
            )[0]
            != 1
        )

        with pytest.raises(YtResponseError):
            self._create_pod(yp_client, pod_set_id, enable_internet=True)

        yp_client.update_object(
            "account",
            account_id,
            set_updates=[
                {
                    "path": "/spec/resource_limits/per_segment/default/internet_address",
                    "value": {"capacity": 1},
                }
            ],
        )

        pod_id3 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id3], "assigned")
        assert (
            yp_client.get_object("pod", pod_id3, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )

        yp_client.update_object(
            "account",
            account_id,
            set_updates=[
                {
                    "path": "/spec/resource_limits/per_segment/default/internet_address",
                    "value": {"capacity": 3},
                }
            ],
        )

        pod_id4 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id4], "assigned")

        pod_id5 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id5], "assigned")

        wait(
            lambda: yp_client.get_object(
                "account",
                account_id,
                selectors=["/status/resource_usage/per_segment/default/internet_address/capacity"],
            )[0]
            == 3
        )

        yp_client.update_object(
            "account",
            account_id,
            set_updates=[
                {
                    "path": "/spec/resource_limits/per_segment/default/internet_address",
                    "value": {"capacity": 1},
                }
            ],
        )
        yp_client.remove_object("pod", pod_id4)

        wait(
            lambda: yp_client.get_object(
                "account",
                account_id,
                selectors=["/status/resource_usage/per_segment/default/internet_address/capacity"],
            )[0]
            == 2
        )

    def test_reassign_internet_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        node_id = self._create_node(yp_client, "netmodule1")
        inet_addr_id = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")

        pod_id1 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id1], "assigned")

        pod_id2 = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_error(yp_client, [pod_id2])

        assert (
            yp_client.get_object("pod", pod_id1, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )
        assert (
            yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0]
            == pod_id1
        )

        yp_client.remove_object("pod", pod_id1)
        self._wait_scheduled_state(yp_client, [pod_id2], "assigned")

        assert (
            yp_client.get_object("pod", pod_id2, selectors=["/status/scheduling/node_id"])[0]
            == node_id
        )
        assert (
            yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0]
            == pod_id2
        )

        yp_client.remove_object("pod", pod_id2)
        assert isinstance(
            yp_client.get_object("internet_address", inet_addr_id, selectors=["/status/pod_id"])[0],
            YsonEntity,
        )

    def test_internet_address_leak(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        self._create_node(yp_client, "netmodule1")
        inet_addr_id = self._create_inet_addr(yp_client, "netmodule1", "1.2.3.4")

        pod_id = self._create_pod(yp_client, pod_set_id, enable_internet=True)
        self._wait_scheduled_state(yp_client, [pod_id], "assigned")

        assert (
            "pod_id"
            in yp_client.get_object("internet_address", inet_addr_id, selectors=["/status"])[0]
        )

        addr_requests = yp_client.get_object(
            "pod", pod_id, selectors=["/spec/ip6_address_requests"]
        )[0]
        assert len(addr_requests) == 1

        addr_requests[0].pop("enable_internet", None)

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[{"path": "/spec/ip6_address_requests", "value": addr_requests}],
        )
        assert (
            "pod_id"
            not in yp_client.get_object("internet_address", inet_addr_id, selectors=["/status"])[0]
        )

        addr_requests[0]["enable_internet"] = True
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[{"path": "/spec/ip6_address_requests", "value": addr_requests}],
            )

    def test_empty_pool_id(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = self._prepare_objects(yp_client)
        self._create_node(yp_client, "netmodule")
        self._create_inet_addr(yp_client, "netmodule", "1.2.3.4")

        pod_id = yp_client.create_object(
            "pod",
            attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "resource_requests": ZERO_RESOURCE_REQUESTS,
                    "ip6_address_requests": [
                        {
                            "vlan_id": "somevlan",
                            "network_id": "somenet",
                            "ip4_address_pool_id": "",
                        },
                    ],
                    "enable_scheduling": True,
                },
            },
        )

        self._wait_scheduled_state(yp_client, [pod_id], "assigned")

    def test_empty_network_module_id(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            self._create_inet_addr(yp_client, "", "1.2.3.4")

        id = self._create_inet_addr(yp_client, "netmodule", "1.2.3.4")

        def update(network_module_id):
            yp_client.update_object(
                "internet_address",
                id,
                set_updates=[dict(path="/spec/network_module_id", value=network_module_id,)],
            )

        with pytest.raises(YtResponseError):
            update("")
        update("netmodule2")
