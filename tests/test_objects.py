import pytest

from .conftest import ZERO_RESOURCE_REQUESTS

from yp.common import YtResponseError, YpInvalidObjectTypeError
from yp.local import OBJECT_TYPES

from yp.data_model import TPodSetMeta, TPodSetSpec, TPodSet

from six.moves import xrange

from yt.yson import YsonEntity

@pytest.mark.usefixtures("yp_env")
class TestObjects(object):
    def test_uuids(self, yp_env):
        yp_client = yp_env.yp_client

        ids = [yp_client.create_object("pod_set") for _ in xrange(10)]
        uuids = [yp_client.get_object("pod_set", id, selectors=["/meta/uuid"])[0] for id in ids]
        assert len(set(uuids)) == 10

    def test_cannot_change_uuid(self, yp_env):
        yp_client = yp_env.yp_client
        id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object("pod_set", id, set_updates=[{"path": "/meta/uuid", "value": "1-2-3-4"}])

    def test_names_allowed(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["account", "group"]:
            yp_client.create_object(type, attributes={"meta": {"name": "some_name"}})

    def test_names_forbidden(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["pod", "pod_set"]:
            with pytest.raises(YtResponseError):
                yp_client.create_object(type, attributes={"meta": {"name": "some_name"}})

    def test_zero_selectors_yp_563(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("pod_set")
        assert yp_client.select_objects("pod_set", selectors=["/status"]) == [[{}]]

    def test_select_null(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpInvalidObjectTypeError):
            yp_client.select_objects("null", selectors=["/meta"])

    def test_select_limit(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(10):
            yp_client.create_object("node")

        assert len(yp_client.select_objects("node", selectors=["/meta/id"])) == 10
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], limit=5)) == 5
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], limit=0)) == 0
        with pytest.raises(YtResponseError):
            yp_client.select_objects("node", selectors=["/meta/id"], limit=-10)

    def test_select_offset(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(10):
            yp_client.create_object("node")

        assert len(yp_client.select_objects("node", selectors=["/meta/id"])) == 10
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=20)) == 0
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=10)) == 0
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=2)) == 8
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=0)) == 10
        with pytest.raises(YtResponseError):
            yp_client.select_objects("node", selectors=["/meta/id"], offset=-10)

    def test_select_paging(self, yp_env):
        yp_client = yp_env.yp_client

        N = 10
        for _ in xrange(N):
            yp_client.create_object("node")

        node_ids = []
        for i in xrange(N):
            result = yp_client.select_objects("node", selectors=["/meta/id"], offset=i, limit=1)
            assert len(result) == 1
            assert len(result[0]) == 1
            node_ids.append(result[0][0])

        assert len(set(node_ids)) == N

    def test_select_filter_with_alias(self, yp_env):
        yp_client = yp_env.yp_client
        assert yp_client.select_objects(
            "account",
            selectors=["/meta/id"],
            filter="([/meta/id] as a) = \"tmp\"",
        ) == [["tmp"]]

    def test_select_filter_with_transform_default_values(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set", attributes=dict(meta=dict(id="podset")))
        pod_id = yp_client.create_object("pod", attributes=dict(
            meta=dict(id="pod", pod_set_id=pod_set_id),
        ))
        assert yp_client.select_objects(
            "pod",
            selectors=["/meta/id"],
            filter="transform([/meta/id], (\"x\"), (\"y\"), ([/meta/pod_set_id])) = \"{}\"".format(pod_set_id)
        ) == [["pod"]]

    def test_many_to_one_attribute(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("account", attributes={"meta": {"id": "myaccount"}})
        yp_client.create_object("pod_set", attributes={"meta": {"id": "mypodset"}})
        yp_client.create_object("pod", attributes={
            "meta": {"id": "mypod", "pod_set_id": "mypodset"},
            "spec": {
                "resource_requests": ZERO_RESOURCE_REQUESTS
            }
        })

        yp_client.update_object("pod", "mypod", set_updates=[
            {"path": "/spec/account_id", "value": "myaccount"}
        ])
        assert yp_client.get_object("pod", "mypod", selectors=["/spec/account_id"])[0] == "myaccount"

        with pytest.raises(YtResponseError):
            yp_client.update_object("pod", "mypod", set_updates=[
                {"path": "/spec/account_id", "value": "nonexisting"}
            ])
        assert yp_client.get_object("pod", "mypod", selectors=["/spec/account_id"])[0] == "myaccount"

        yp_client.update_object("pod", "mypod", remove_updates=[
            {"path": "/spec/account_id"}
        ])
        assert yp_client.get_object("pod", "mypod", selectors=["/spec/account_id"])[0] == ""

    def test_set_composite_attribute_spec_success(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object("replica_set", attributes={"spec": {"account_id": account_id}})
        yp_client.update_object("replica_set", rs_id, set_updates=[
            {"path": "/spec", "value": {
                "account_id": account_id,
                "replica_count": 10
            }}
        ])

        assert yp_client.get_object("replica_set", rs_id, selectors=["/spec/account_id", "/spec/replica_count"]) == [account_id, 10]

    def test_set_composite_attribute_spec_fail(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        with pytest.raises(YtResponseError):
            yp_client.update_object("node", node_id, set_updates=[
                {"path": "/spec", "value": {"zzz": 1}}
            ])

    def test_select_missing_proto_attribute(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.select_objects("node", selectors=["/spec/xyz"])

    def test_select_improper_proto_attribute_access(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.select_objects("node", selectors=["/spec/ip6_addresses/0/address/xyz"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/generation_number/xyz"])

    def test_control_is_hidden(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        data = yp_client.get_object("pod", pod_id, selectors=[""])[0]
        assert "meta" in data
        assert "spec" in data
        assert "status" in data
        assert "labels" in data
        assert "annotations" in data
        assert "control" not in data

    def test_get_object_fetch_options(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        get_rsp1 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                        selectors=["/spec/antiaffinity_constraints"])
        assert "timestamp" in get_rsp1["result"][0]
        assert "value" not in get_rsp1["result"][0]

        get_rsp2 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True,
                                       selectors=["/spec/antiaffinity_constraints"])
        assert "timestamp" not in get_rsp2["result"][0]
        assert "value" in get_rsp2["result"][0]

    def test_select_objects_fetch_options(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        select_rsp1 = yp_client.select_objects("pod_set", enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                               selectors=["/spec/antiaffinity_constraints"])["results"][0]
        assert "timestamp" in select_rsp1[0]
        assert "value" not in select_rsp1[0]

        select_rsp2 = yp_client.select_objects("pod_set", enable_structured_response=True,
                                               selectors=["/spec/antiaffinity_constraints"])["results"][0]
        assert "timestamp" not in select_rsp2[0]
        assert "value" in select_rsp2[0]

    def test_get_spec_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        create_rsp = yp_client.create_object("pod_set", enable_structured_response=True)
        pod_set_id = create_rsp["object_id"]
        create_ts = create_rsp["commit_timestamp"]

        get_rsp1 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                        selectors=["/spec/antiaffinity_constraints", "/spec/node_segment_id"])
        assert get_rsp1["timestamp"] > create_ts
        assert get_rsp1["result"][0]["timestamp"] == create_ts
        assert get_rsp1["result"][1]["timestamp"] == create_ts

        update_rsp1 = yp_client.update_object("pod_set", pod_set_id, set_updates=[{"path": "/spec/antiaffinity_constraints", "value": [{"key": "node", "max_pods": 1}]}])
        update_ts = update_rsp1["commit_timestamp"]
        assert update_ts > create_ts

        get_rsp2 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                        selectors=["/spec/antiaffinity_constraints", "/spec/node_segment_id", "/spec"])
        assert get_rsp2["timestamp"] > update_ts
        assert get_rsp2["result"][0]["timestamp"] == update_ts
        assert get_rsp2["result"][1]["timestamp"] == create_ts
        assert get_rsp2["result"][2]["timestamp"] == update_ts

    def test_get_meta_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        create_rsp = yp_client.create_object("pod_set", enable_structured_response=True)
        pod_set_id = create_rsp["object_id"]
        create_ts = create_rsp["commit_timestamp"]

        get_rsp = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                       selectors=["/meta"])
        assert get_rsp["timestamp"] > create_ts
        assert get_rsp["result"][0]["timestamp"] == create_ts

    def test_select_spec_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        create_rsp = yp_client.create_object("pod_set", enable_structured_response=True)
        pod_set_id = create_rsp["object_id"]
        create_ts = create_rsp["commit_timestamp"]

        get_rsp1 = yp_client.select_objects("pod_set", enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                            selectors=["/spec/antiaffinity_constraints", "/spec/node_segment_id"])
        assert get_rsp1["timestamp"] > create_ts
        assert get_rsp1["results"][0][0]["timestamp"] == create_ts
        assert get_rsp1["results"][0][1]["timestamp"] == create_ts

        update_rsp1 = yp_client.update_object("pod_set", pod_set_id, set_updates=[{"path": "/spec/antiaffinity_constraints", "value": [{"key": "node", "max_pods": 1}]}])
        update_ts = update_rsp1["commit_timestamp"]
        assert update_ts > create_ts

        get_rsp2 = yp_client.select_objects("pod_set", enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                            selectors=["/spec/antiaffinity_constraints", "/spec/node_segment_id", "/spec"])
        assert get_rsp2["timestamp"] > update_ts
        assert get_rsp2["results"][0][0]["timestamp"] == update_ts
        assert get_rsp2["results"][0][1]["timestamp"] == create_ts
        assert get_rsp2["results"][0][2]["timestamp"] == update_ts

    def test_get_annotation_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        create_rsp = yp_client.create_object("pod_set", enable_structured_response=True)
        pod_set_id = create_rsp["object_id"]
        create_ts = create_rsp["commit_timestamp"]

        get_rsp1 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": True},
                                        selectors=["/annotations/missing"])
        assert get_rsp1["timestamp"] > create_ts
        assert get_rsp1["result"][0]["timestamp"] == 0
        assert get_rsp1["result"][0]["value"] == YsonEntity()

        update_rsp1 = yp_client.update_object("pod_set", pod_set_id, set_updates=[{"path": "/annotations/existing", "value": 123}])
        update_ts = update_rsp1["commit_timestamp"]
        assert update_ts > create_ts

        get_rsp2 = yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": True},
                                        selectors=["/annotations/existing"])
        assert get_rsp2["timestamp"] > update_ts
        assert get_rsp2["result"][0]["timestamp"] == update_ts
        assert get_rsp2["result"][0]["value"] == 123

    def test_no_timestamp_for_whole_annotations(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.get_object("pod_set", pod_set_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                 selectors=["/annotations"])

    def test_spec_timestamp_available_for_all_types(self, yp_env):
        yp_client = yp_env.yp_client

        def _check(type, attributes={}):
            create_rsp = yp_client.create_object(type, attributes=attributes, enable_structured_response=True)
            create_ts = create_rsp["commit_timestamp"]
            id = create_rsp["object_id"]
            get_rsp = yp_client.get_object(type, id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                           selectors=["/spec"])
            assert get_rsp["timestamp"] > create_ts
            assert get_rsp["result"][0]["timestamp"] == create_ts

        account_id = yp_client.create_object("account")
        _check("node")
        _check("pod_set")
        _check("endpoint_set")
        _check("network_project", attributes={"spec": {"project_id": 123}})
        _check("node_segment", attributes={"spec": {"node_filter": ""}})
        _check("virtual_service")
        _check("user")
        _check("group")
        _check("ip4_address_pool", attributes={"meta": {"acl": []}})
        _check("internet_address", attributes={
            "meta": {"ip4_address_pool_id": "default_ip4_address_pool"},
            "spec": {"ip4_address": "1.2.3.4", "network_module_id": "xyz"}
        })
        _check("dns_record_set")
        _check("account")
        _check("multi_cluster_replica_set")
        _check("replica_set", {"spec": {"account_id": account_id}})
        _check("stage")

        node_id = yp_client.create_object("node")
        _check("resource", attributes={"meta": {"node_id": node_id}, "spec": {"memory": {"total_capacity": 1000}}})

        pod_set_id = yp_client.create_object("pod_set")
        _check("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        _check("dynamic_resource", attributes={"meta": {"pod_set_id": pod_set_id}})
        _check("resource_cache", attributes={"meta": {"pod_set_id": pod_set_id}})

        endpoint_set_id = yp_client.create_object("endpoint_set")
        _check("endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}})

    def test_attr_timestamp_prerequisite_success1(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        get_rsp = yp_client.get_object("pod", pod_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                       selectors=["/spec"])
        ts = get_rsp["result"][0]["timestamp"]

        FILTER = "[/labels/label] = 123"
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_filter", "value": FILTER}],
                                attribute_timestamp_prerequisites=[{"path": "/spec", "timestamp": ts}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/node_filter"])[0] == FILTER

    def test_attr_timestamp_prerequisite_success2(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        get_rsp = yp_client.get_object("pod", pod_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                       selectors=["/spec/node_filter"])
        ts = get_rsp["result"][0]["timestamp"]

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/enable_scheduling", "value": False}])

        FILTER = "[/labels/label] = 123"
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_filter", "value": FILTER}],
                                attribute_timestamp_prerequisites=[{"path": "/spec/node_filter", "timestamp": ts}])
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/node_filter"])[0] == FILTER

    def test_attr_timestamp_prerequisite_failure(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        get_rsp = yp_client.get_object("pod", pod_id, enable_structured_response=True, options={"fetch_timestamps": True, "fetch_values": False},
                                       selectors=["/spec/enable_scheduling"])
        ts = get_rsp["result"][0]["timestamp"]

        FILTER = "[/labels/label] = 123"
        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/node_filter", "value": FILTER}])

        with pytest.raises(YtResponseError):
            yp_client.update_object("pod", pod_id, set_updates=[{"path": "/spec/enable_scheduling", "value": True}],
                                    attribute_timestamp_prerequisites=[{"path": "/spec", "timestamp": ts}])

    def test_payload_to_proto_conversion_get_object(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("pod_set", attributes={"meta": {"id": "somepodset"}})
        meta = yp_client.get_object("pod_set", "somepodset", selectors=[{"path": "/meta", "proto_class": TPodSetMeta}])[0]
        assert meta.id == "somepodset"

    def test_payload_to_proto_conversion_select_object(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("pod_set", attributes={"meta": {"id": "somepodset"}})
        results = yp_client.select_objects("pod_set", selectors=[{"path": "/meta", "proto_class": TPodSetMeta}])
        assert len(results) == 1
        assert len(results[0]) == 1
        meta = results[0][0]
        assert meta.id == "somepodset"

    def test_payload_to_proto_conversion_create_object(self, yp_env):
        yp_client = yp_env.yp_client

        attributes = TPodSet()
        attributes.meta.id = "somepodset"
        yp_client.create_object("pod_set", attributes=attributes)
        meta = yp_client.get_object("pod_set", "somepodset", selectors=["/meta"])[0]
        assert meta["id"] == "somepodset"

    def test_payload_to_proto_conversion_update_object(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")

        spec = TPodSetSpec()
        FILTER = "0 = 1"
        spec.node_filter = FILTER
        yp_client.update_object("pod_set", pod_set_id, set_updates=[{"path": "/spec", "value": spec}])
        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/spec/node_filter"])[0] == FILTER

@pytest.mark.usefixtures("yp_env_configurable")
class TestDisabledExtensibleAttributes(object):
    YP_MASTER_CONFIG = {
        "object_manager": {
            "enable_extensible_attributes": False
        }
    }

    def test_disabled_extensible_attributes(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        account_id = yp_client.create_object("account")
        rs_id = yp_client.create_object(object_type="replica_set", attributes={"spec": {"account_id": account_id, "replica_count": 1}})

        with pytest.raises(YtResponseError):
            yp_client.update_object("replica_set", rs_id, set_updates=[{"path": "/spec/hello", "value": "world"}])
