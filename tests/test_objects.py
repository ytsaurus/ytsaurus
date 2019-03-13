import pytest

from .conftest import ZERO_RESOURCE_REQUESTS
from yp.common import YtResponseError, YpInvalidObjectTypeError
from six.moves import xrange

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
        rs_id = yp_client.create_object("replica_set")
        yp_client.update_object("replica_set", rs_id, set_updates=[
            {"path": "/spec", "value": {
                "account_id": account_id,
                "replica_count": 10
            }}
        ])

        assert yp_client.get_object("replica_set", rs_id, selectors=["/spec/account_id", "/spec/replica_count"]) == [account_id, 10]

    def test_set_composite_attribute_spec_fail(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object("replica_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object("replica_set", rs_id, set_updates=[
                {"path": "/spec", "value": {"zzz": 1}}
            ])

    def test_select_missing_proto_attribute(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object("replica_set")
        with pytest.raises(YtResponseError):
            yp_client.select_objects("replica_set", selectors=["/spec/xyz"])

    def test_select_improper_proto_attribute_access(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.select_objects("replica_set", selectors=["/spec/replica_count/xyz"])
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
