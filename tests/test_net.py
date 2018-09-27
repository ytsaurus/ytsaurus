import pytest

from yp.common import YtResponseError, YpNoSuchObjectError, wait

from yt.yson import YsonEntity

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

        pod_set_id = yp_client.create_object("pod_set", attributes={
            "spec": {
                "node_segment_id": "default"
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
            "spec": spec
        })

        return pod_id

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

        with pytest.raises(YtResponseError):
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
        assert allocations[0]["manual"] == False

    def test_assign_pod_ip6_subnet_scheduler(self, yp_env):
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
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/iss_payload", "value": "123"}])

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
