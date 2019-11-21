from .conftest import (
    ZERO_RESOURCE_REQUESTS,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    update_node_id,
)

from yp.common import (
    YpInvalidObjectIdError,
    YtResponseError,
    wait,
)

from yt.yson import YsonEntity

import pytest

def ipv6_to_ptr_record(address):
    octets = []
    if address.find("::") == -1:
        octets = address.split(":")
    else:
        parts = address.split("::")
        for i in range(len(parts)):
            parts[i] = parts[i].split(":")
        octets = parts[0] + [""] * (8 - len(parts[0]) - len(parts[1])) + parts[1]
    for i in range(len(octets)):
         octets[i] = "0" * (4 - len(octets[i])) + octets[i]

    address = "".join(octets)
    address = reversed(address) # reverse address
    address = ".".join([x for x in address])
    return address + ".ip6.arpa."

def check_dns_record_set(yp_client, key, type, data):
    filtered_records = yp_client.get_object("dns_record_set", key, selectors=["/spec"])
    assert len(filtered_records) == 1
    dns_record_set = filtered_records[0]
    assert len(dns_record_set["records"]) == len(data)
    records_data = []

    for i in range(len(dns_record_set["records"])):
        assert dns_record_set["records"][i]["type"] == type
        assert dns_record_set["records"][i]["class"] == "IN"
        records_data.append(dns_record_set["records"][i]["data"])

    assert sorted(records_data) == sorted(data)

def check_dns_allocation(yp_client, persistent_fqdn, transient_fqdn, address):
    check_dns_record_set(yp_client, persistent_fqdn, "AAAA", [address])
    check_dns_record_set(yp_client, transient_fqdn, "AAAA", [address])
    check_dns_record_set(yp_client, ipv6_to_ptr_record(address), "PTR", [persistent_fqdn])

@pytest.mark.usefixtures("yp_env")
class TestNet(object):
    def _create_pod_with_boilerplate(self, yp_client, spec):
        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }
        })
        node_id = "test"
        vlan_id = "somevlan"
        subnet = "1:2:3:4::/64"
        create_nodes(yp_client, node_ids=[node_id], vlan_id=vlan_id, subnet=subnet)
        pod_set_id = create_pod_set(yp_client)
        return create_pod_with_boilerplate(yp_client, pod_set_id, spec=spec)

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

        with pytest.raises(YtResponseError):
            update_node_id(yp_client, pod_id, node_id, with_retries=False)

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

        with pytest.raises(YtResponseError):
            update_node_id(yp_client, pod_id, node_id, with_retries=False)

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
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": False},
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True},
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True, "dns_prefix": "abc"},
                    {"vlan_id": "fastbone", "network_id": "somenet", "enable_dns": True},
                ]
            }})

        node_id = yp_client.create_object("node", attributes={
                "meta": {
                    "id": "test"
                },
                "spec": {
                    "ip6_subnets": [
                        {"vlan_id": "somevlan", "subnet": "1:2:3:4::/64"},
                        {"vlan_id": "fastbone", "subnet": "1:3:3:4::/64"}
                    ]
                }
            })

        update_node_id(yp_client, pod_id, node_id)
        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 4
        assert "persistent_fqdn" not in allocations[0]
        assert "transient_fqdn" not in allocations[0]
        assert allocations[1]["persistent_fqdn"] == "{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[1]["transient_fqdn"] == "{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)
        assert allocations[2]["persistent_fqdn"] == "abc.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[2]["transient_fqdn"] == "abc.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)
        assert allocations[3]["persistent_fqdn"] == "fb-{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[3]["transient_fqdn"] == "fb-{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)

        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 9

        check_dns_allocation(yp_client, allocations[1]["persistent_fqdn"], allocations[1]["transient_fqdn"], allocations[1]["address"])
        check_dns_allocation(yp_client, allocations[2]["persistent_fqdn"], allocations[2]["transient_fqdn"], allocations[2]["address"])
        check_dns_allocation(yp_client, allocations[3]["persistent_fqdn"], allocations[3]["transient_fqdn"], allocations[3]["address"])

        yp_client.update_object("pod", pod_id, set_updates=[{
            "path": "/spec/ip6_address_requests",
            "value": [
                {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True, "dns_prefix": "xyz"}
            ]
        }])

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["persistent_fqdn"] == "xyz.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[0]["transient_fqdn"] == "xyz.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)

        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 3
        check_dns_allocation(yp_client, allocations[0]["persistent_fqdn"], allocations[0]["transient_fqdn"], allocations[0]["address"])

        yp_client.remove_object("pod", pod_id)
        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 0

    def test_multiple_dns_records_with_same_prefix(self, yp_env):
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
                "resource_requests": ZERO_RESOURCE_REQUESTS,
                "ip6_address_requests": [
                    {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True, "dns_prefix": "abc"},
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

        update_node_id(yp_client, pod_id, node_id)
        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 2
        assert allocations[0]["persistent_fqdn"] == "abc.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[0]["transient_fqdn"] == "abc.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)
        assert allocations[1]["persistent_fqdn"] == "abc.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[1]["transient_fqdn"] == "abc.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)

        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 4 # two AAAA records and two PTR records

        check_dns_record_set(yp_client, allocations[0]["persistent_fqdn"], "AAAA", [allocations[0]["address"], allocations[1]["address"]])
        check_dns_record_set(yp_client, allocations[0]["transient_fqdn"], "AAAA", [allocations[0]["address"], allocations[1]["address"]])
        check_dns_record_set(yp_client, ipv6_to_ptr_record(allocations[0]["address"]), "PTR", [allocations[0]["persistent_fqdn"]])
        check_dns_record_set(yp_client, ipv6_to_ptr_record(allocations[1]["address"]), "PTR", [allocations[1]["persistent_fqdn"]])

        yp_client.update_object("pod", pod_id, set_updates=[{
            "path": "/spec/ip6_address_requests",
            "value": [
                {"vlan_id": "somevlan", "network_id": "somenet", "enable_dns": True, "dns_prefix": "abc"}
            ]
        }])

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["persistent_fqdn"] == "abc.{}.test.yp-c.yandex.net".format(pod_id)
        assert allocations[0]["transient_fqdn"] == "abc.{}-1.{}.test.yp-c.yandex.net".format(node_id, pod_id)

        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 3
        check_dns_allocation(yp_client, allocations[0]["persistent_fqdn"], allocations[0]["transient_fqdn"], allocations[0]["address"])

        yp_client.remove_object("pod", pod_id)
        assert len(yp_client.select_objects("dns_record_set", selectors=["/meta"])) == 0

    def test_assign_pod_ip6_address(self, yp_env):
        yp_client = yp_env.yp_client

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet"},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["vlan_id"] == "somevlan"
        assert allocations[0]["address"].startswith("1:2:3:4:0:7b:")

    def test_assign_pod_ip6_subnet_with_network_project(self, yp_env):
        yp_client = yp_env.yp_client

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_subnet_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet"},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_subnet_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["vlan_id"] == "somevlan"
        assert allocations[0]["subnet"].startswith("1:2:3:4:0:7b:")
        assert allocations[0]["subnet"].endswith("/112")

    def test_assign_pod_ip6_subnet_without_network_project(self, yp_env):
        yp_client = yp_env.yp_client

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_subnet_requests": [
                {"vlan_id": "somevlan"},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_subnet_allocations"])[0]
        assert len(allocations) == 1
        assert allocations[0]["vlan_id"] == "somevlan"
        assert allocations[0]["subnet"].startswith("1:2:3:4::")
        assert allocations[0]["subnet"].endswith("/112")

    def test_update_assigned_pod_with_removed_network_project(self, yp_env):
        yp_client = yp_env.yp_client

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet"},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")
        yp_client.remove_object("network_project", "somenet")
        # Must not throw
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/iss", "value": {"instances": []}}])

    def test_virtual_service_tunnel(self, yp_env):
        yp_client = yp_env.yp_client

        vs_id = yp_client.create_object("virtual_service", attributes={
            "spec": {
                "ip4_addresses": ["100.100.100.100", "2.2.2.2"],
                "ip6_addresses": ["1:1:1:1", "2:2:2:2", "3:3:3:3"],
            }
        })

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet", "virtual_service_ids": [vs_id]},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert allocations[0]["virtual_services"][0]["ip4_addresses"] == ["100.100.100.100", "2.2.2.2"]
        assert allocations[0]["virtual_services"][0]["ip6_addresses"] == ["1:1:1:1", "2:2:2:2", "3:3:3:3"]

    def test_multiple_virtual_service_tunnels(self, yp_env):
        yp_client = yp_env.yp_client

        vs_id1 = yp_client.create_object("virtual_service", attributes={
            "spec": {
                "ip4_addresses": ["1.1.1.1"],
            }
        })
        vs_id2 = yp_client.create_object("virtual_service", attributes={
            "spec": {
                "ip4_addresses": ["2.2.2.2"],
            }
        })

        vs_ids = [
            [vs_id1],
            [],
            [vs_id1, vs_id2],
            [vs_id2],
        ]

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet", "virtual_service_ids": ids} for ids in vs_ids
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        allocations = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0]
        assert len(allocations) == len(vs_ids)

        for i in range(len(allocations)):
            assert len(allocations[i].get("virtual_services", [])) == len(vs_ids[i])
            for j in range(len(vs_ids[i])):
                vs = yp_client.get_object("virtual_service", vs_ids[i][j], selectors=["/spec"])[0]
                pod_vs = allocations[i]["virtual_services"][j]
                assert pod_vs.get("ip4_addresses", []) == vs.get("ip4_addresses", [])
                assert pod_vs.get("ip6_addresses", []) == vs.get("ip6_addresses", [])

    def test_virtual_service_options(self, yp_env):
        client = yp_env.yp_client

        pod_set_id = client.create_object("pod_set")
        pod_id = client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "virtual_service_options": {
                    "ip4_mtu": 42,
                    "ip6_mtu": 36,
                    "decapsulator_anycast_address": "13:13:13:13",
                }
            },
        })

        options = client.get_object("pod", pod_id, selectors=["/spec/virtual_service_options"])
        assert options[0]["ip4_mtu"] == 42
        assert options[0]["ip6_mtu"] == 36
        assert options[0]["decapsulator_anycast_address"] == "13:13:13:13"

    def test_invalid_virtual_service_tunnel_in_pod_spec(self, yp_env):
        yp_client = yp_env.yp_client

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet", "virtual_service_ids": ["incorrect_id"]}
            ],
            "enable_scheduling": True,
        })

        wait(lambda: not isinstance(yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"])[0], YsonEntity))

    def test_invalid_virtual_service_id_in_pod_spec(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("network_project", attributes={
            "meta": {
                "id": "somenet"
            },
            "spec": {
                "project_id": 123
            }
        })

        node_id = "test"
        vlan_id = "somevlan"
        subnet = "1:2:3:4::/64"
        create_nodes(yp_client, node_ids=[node_id], vlan_id=vlan_id, subnet=subnet)

        pod_set_id = create_pod_set(yp_client)

        def create_pod(virtual_service_ids=None):
            spec = dict(
                ip6_address_requests=[
                    dict(
                        vlan_id="somevlan",
                        network_id="somenet",
                    ),
                ],
                enable_scheduling=True,
            )

            if virtual_service_ids is not None:
                spec["ip6_address_requests"][0]["virtual_service_ids"] = virtual_service_ids

            return create_pod_with_boilerplate(yp_client, pod_set_id, spec=spec)

        # Check correctness of other parameters.
        pod_id = create_pod()
        yp_client.remove_object("pod", pod_id)

        for virtual_service_id in ("/", "", "*"):
            with pytest.raises(YpInvalidObjectIdError):
                create_pod([virtual_service_id])

    def test_update_virtual_service_tunnel(self, yp_env):
        yp_client = yp_env.yp_client

        specs = [
            { "ip4_addresses": ["1.2.3.4"] },
            { "ip6_addresses": ["1:2:3:4"] },
            { "ip4_addresses": ["1.2.3.4"], "ip6_addresses": ["1:2:3:4"] },
            { },
        ]

        pod_id = self._create_pod_with_boilerplate(yp_client, spec={
            "ip6_address_requests": [
                {"vlan_id": "somevlan", "network_id": "somenet"},
            ],
            "enable_scheduling": True
        })

        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned")

        for spec in specs:
            vs_id = yp_client.create_object(object_type="virtual_service", attributes={"spec": spec})

            update = {
                "path": "/spec/ip6_address_requests",
                "value": [{"vlan_id": "somevlan", "network_id": "somenet", "virtual_service_ids": [vs_id]}]
            }

            def check_vs_status():
                addresses = yp_client.get_object("pod", pod_id, selectors=["/status/ip6_address_allocations"])[0][0]
                ip4 = addresses["virtual_services"][0].get("ip4_addresses", [])
                ip6 = addresses["virtual_services"][0].get("ip6_addresses", [])
                spec_ip4, spec_ip6 = spec.get("ip4_addresses", []), spec.get("ip6_addresses", [])

                assert ip4 == spec_ip4
                assert ip6 == spec_ip6

            yp_client.update_object("pod", pod_id, set_updates=[update])
            check_vs_status()
            yp_client.update_object("pod", pod_id, set_updates=[update])
            check_vs_status()
