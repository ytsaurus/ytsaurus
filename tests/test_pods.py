import pytest

from yp.client import YpResponseError
from yt.yson import YsonEntity, YsonUint64

@pytest.mark.usefixtures("yp_env")
class TestPods(object):
    def test_pod_set_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpResponseError): yp_client.create_object(object_type="pod")

    def test_get_pod(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        result = yp_client.get_object("pod", pod_id, selectors=["/status/agent/state", "/meta/id", "/meta/pod_set_id"])
        assert result[0] == "unknown"
        assert result[1] == pod_id
        assert result[2] == pod_set_id

    def test_parent_pod_set_must_exist(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpResponseError): yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": "nonexisting_pod_set_id"}})

    def test_pod_create_destroy(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        def get_counts():
            return (len(list(yt_client.select_rows("* from [//yp/db/pod_sets] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/pods] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/parents]"))))

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_attributes = {"meta": {"pod_set_id": pod_set_id}}
        pod_ids = [yp_client.create_object(object_type="pod", attributes=pod_attributes)
                   for i in xrange(10)]

        assert get_counts() == (1, 10, 10)

        yp_client.remove_object("pod", pod_ids[0])

        assert get_counts() == (1, 9, 9)

        yp_client.remove_object("pod_set", pod_set_id)

        assert get_counts() == (0, 0, 0)

    def test_pod_set_empty_selectors(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        assert yp_client.get_object("pod_set", pod_set_id, selectors=[]) == []

    def _create_node(self, yp_client, cpu_capacity=0, memory_capacity=0):
        node_id = "test.yandex.net"
        assert yp_client.create_object("node", attributes={
                "meta": {
                    "id": node_id
                }
            }) == node_id
        yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "kind": "cpu",
                    "total_capacity": cpu_capacity
                }
            })
        yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "kind": "memory",
                    "total_capacity": memory_capacity
                }
            })
        return node_id

    def test_pod_assignment_failure(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_id = self._create_node(yp_client)
        with pytest.raises(YpResponseError):
            yp_client.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "resource_requests": {
                            "vcpu_guarantee": 100,
                            "memory_guarantee": 2000
                        },
                        "node_id": node_id
                    }
                })

    def test_pod_assignment_success(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_id = self._create_node(yp_client, 100, 2000)
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "resource_requests": {
                        "vcpu_guarantee": 100,
                        "memory_guarantee": 2000
                    },
                    "node_id": node_id
                }
            })

    def test_pod_fqdns(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_id = self._create_node(yp_client)
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                }})

        assert yp_client.get_object("pod", pod_id, selectors=["/status/dns/persistent_fqdn", "/status/dns/transient_fqdn"]) == \
               [pod_id + ".test.yp-c.yandex.net", YsonEntity()]

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_id", "value": node_id}])

        assert yp_client.get_object("pod", pod_id, selectors=["/status/dns/persistent_fqdn", "/status/dns/transient_fqdn", "/status/generation_number"]) == \
               [pod_id + ".test.yp-c.yandex.net", "test-1." + pod_id + ".test.yp-c.yandex.net", 1]

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_id", "value": ""}])

        assert yp_client.get_object("pod", pod_id, selectors=["/status/dns/persistent_fqdn", "/status/dns/transient_fqdn", "/status/generation_number"]) == \
               [pod_id + ".test.yp-c.yandex.net", YsonEntity(), 1]

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_id", "value": node_id}])

        assert yp_client.get_object("pod", pod_id, selectors=["/status/dns/persistent_fqdn", "/status/dns/transient_fqdn", "/status/generation_number"]) == \
               [pod_id + ".test.yp-c.yandex.net", "test-2." + pod_id + ".test.yp-c.yandex.net", 2]

    def test_master_spec_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        
        ts1 = yp_client.get_object("pod", pod_id, selectors=["/status/master_spec_timestamp"])[0]

        tx_id = yp_client.start_transaction()
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_filter", "value": "0 = 1"}], transaction_id=tx_id)
        ts2 = yp_client.commit_transaction(tx_id)["commit_timestamp"]

        assert ts1 < ts2
        assert ts2 == yp_client.get_object("pod", pod_id, selectors=["/status/master_spec_timestamp"])[0]

    def test_iss_spec(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        iss_spec = {
            "instances": [{
                "id": {
                    "slot": {
                        "service": "some-service",
                        "host": "some-host"
                    },
                    "configuration": {
                        "groupId": "some-groupId",
                    }
                }
            }]
        }
        
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/iss"])[0] == {}
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/iss", "value": iss_spec}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/iss"])[0] == iss_spec
        
        yp_client.get_object("pod", pod_id, selectors=["/spec/iss_payload"])[0]

        yp_client.update_object("pod",pod_id, set_updates=[{"path": "/spec/iss/instances/0/id/slot/service", "value": "another-service"}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/iss/instances/0/id/slot/service"])[0] == "another-service"
        
    def test_iss_status(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        assert yp_client.get_object("pod", pod_id, selectors=["/status/agent/iss_payload", "/status/agent/iss"]) == ["", {}]

    def test_spec_updates(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/resource_requests", "value": {"vcpu_limit": 100}}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/resource_requests"])[0] == {"vcpu_limit": 100}
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/resource_requests/vcpu_limit"])[0] == 100
        assert yp_client.select_objects("pod", selectors=["/spec/resource_requests"])[0] == [{"vcpu_limit": 100}]
        assert yp_client.select_objects("pod", selectors=["/spec/resource_requests/vcpu_limit"])[0] == [100]
        assert yp_client.select_objects("pod", selectors=["/spec"])[0][0]["resource_requests"] == {"vcpu_limit": 100}

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/resource_requests/vcpu_limit", "value": YsonUint64(200)}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/resource_requests"])[0] == {"vcpu_limit": 200}
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/resource_requests/vcpu_limit"])[0] == 200
        assert yp_client.select_objects("pod", selectors=["/spec/resource_requests"])[0] == [{"vcpu_limit": 200}]
        assert yp_client.select_objects("pod", selectors=["/spec/resource_requests/vcpu_limit"])[0] == [200]
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/resource_requests"])[0] == {"vcpu_limit": 200}
        assert yp_client.select_objects("pod", selectors=["/spec"])[0][0]["resource_requests"] == {"vcpu_limit": 200}

    def test_host_devices(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = self._create_node(yp_client)
        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={
            "meta": {"pod_set_id": pod_set_id},
            "spec": {"node_id": node_id}
        })

        for x in ["/dev/kvm",
                  "/dev/net/tun"]:
            yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/host_devices", "value": [{"path": x, "mode": "rw"}]}])

        for x in ["/dev/xyz"]:
            with pytest.raises(YpResponseError):
                yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/host_devices", "value": [{"path": x, "mode": "rw"}]}])

    def test_sysctl_properties(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = self._create_node(yp_client)
        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={
            "meta": {"pod_set_id": pod_set_id},
            "spec": {"node_id": node_id}
        })

        for x in ["net.core.somaxconn",
                  "net.unix.max_dgram_qlen",
                  "net.ipv4.icmp_echo_ignore_all",
                  "net.ipv4.icmp_echo_ignore_broadcasts",
                  "net.ipv4.icmp_ignore_bogus_error_responses",
                  "net.ipv4.icmp_errors_use_inbound_ifaddr",
                  "net.ipv4.icmp_ratelimit",
                  "net.ipv4.icmp_ratemask",
                  "net.ipv4.ping_group_range",
                  "net.ipv4.tcp_ecn",
                  "net.ipv4.tcp_ecn_fallback",
                  "net.ipv4.ip_dynaddr",
                  "net.ipv4.ip_early_demux",
                  "net.ipv4.ip_default_ttl",
                  "net.ipv4.ip_local_port_range",
                  "net.ipv4.ip_local_reserved_ports",
                  "net.ipv4.ip_no_pmtu_disc",
                  "net.ipv4.ip_forward_use_pmtu",
                  "net.ipv4.ip_nonlocal_bind",
                  "net.ipv4.tcp_mtu_probing",
                  "net.ipv4.tcp_base_mss",
                  "net.ipv4.tcp_probe_threshold",
                  "net.ipv4.tcp_probe_interval",
                  "net.ipv4.tcp_keepalive_time",
                  "net.ipv4.tcp_keepalive_probes",
                  "net.ipv4.tcp_keepalive_intvl",
                  "net.ipv4.tcp_syn_retries",
                  "net.ipv4.tcp_synack_retries",
                  "net.ipv4.tcp_syncookies",
                  "net.ipv4.tcp_reordering",
                  "net.ipv4.tcp_retries1",
                  "net.ipv4.tcp_retries2",
                  "net.ipv4.tcp_orphan_retries",
                  "net.ipv4.tcp_fin_timeout",
                  "net.ipv4.tcp_notsent_lowat",
                  "net.ipv4.tcp_tw_reuse",
                  "net.ipv6.bindv6only",
                  "net.ipv6.ip_nonlocal_bind",
                  "net.ipv6.icmp.ratelimit",
                  "net.ipv6.route.gc_thresh",
                  "net.ipv6.route.max_size",
                  "net.ipv6.route.gc_min_interval",
                  "net.ipv6.route.gc_timeout",
                  "net.ipv6.route.gc_interval",
                  "net.ipv6.route.gc_elasticity",
                  "net.ipv6.route.mtu_expires",
                  "net.ipv6.route.min_adv_mss",
                  "net.ipv6.route.gc_min_interval_ms",
                  "net.ipv4.conf.blablabla",
                  "net.ipv6.conf.blablabla",
                  "net.ipv4.neigh.blablabla",
                  "net.ipv6.neigh.blablabla"]:
            yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/sysctl_properties", "value": [{"name": x, "value": "0"}]}])

        for x in ["someother.property",
                  "net.ipv4.blablabla",
                  "net.ipv4.neigh.default.blablabla"]:
            with pytest.raises(YpResponseError):
                yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/sysctl_properties", "value": [{"name": x, "value": "0"}]}])
