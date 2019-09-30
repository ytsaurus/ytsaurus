import yp_proto.yp.client.api.proto.object_service_pb2 as object_service_pb2

import yp.data_model as data_model

from yp.common import YtResponseError

from yt.yson import YsonEntity

import pytest

@pytest.mark.usefixtures("yp_env")
class TestObjectService(object):
    def test_select_empty_field(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        selection_result = yp_client.select_objects("pod", selectors=["/spec/virtual_service_options/ip4_mtu"])
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)
        selection_result = yp_client.select_objects("pod", selectors=["/status/agent/iss/currentStates"])
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)
        selection_result = yp_client.select_objects("pod", selectors=["/status/agent/iss/currentStates/1"])
        assert len(selection_result) == 1 and isinstance(selection_result[0][0], YsonEntity)

    def test_select_nonexistent_field(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/agent/iss/currentStates/1a"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/agent/iss/nonexistentField"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/status/agent/iss/currentStates/1/nonexistentField"])
        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/spec/ip6_address_requests/network_id/network_id"])

    def test_get_nonexistent(self, yp_env):
        yp_client = yp_env.yp_client

        existent_id = "existent_id"
        nonexistent_id = "nonexistent_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": existent_id}}) == existent_id

        with pytest.raises(YtResponseError):
            yp_client.get_objects("pod_set", [existent_id, nonexistent_id], selectors=["/meta/id"])

        with pytest.raises(YtResponseError):
            yp_client.get_object("pod_set", nonexistent_id, selectors=["/meta/id"])

        objects = yp_client.get_objects("pod_set", [existent_id, nonexistent_id], selectors=["/meta/id"], options={"ignore_nonexistent": True})
        assert len(objects) == 2
        assert len(objects[0]) == 1 and objects[0][0] == existent_id
        assert objects[1] is None

        assert yp_client.get_object("pod_set", nonexistent_id, selectors=["/meta/id"], options={"ignore_nonexistent": True}) is None

    def test_create_object_null_attributes_payload(self, yp_env):
        yp_client = yp_env.yp_client

        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD_SET
        req.attributes_payload.null = True

        rsp = object_stub.CreateObject(req)

        assert len(rsp.object_id) > 0

    def test_create_objects_null_attributes_payload(self, yp_env):
        yp_client = yp_env.yp_client

        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObjects()
        subreq = req.subrequests.add()
        subreq.object_type = data_model.OT_POD_SET
        subreq.attributes_payload.null = True

        rsp = object_stub.CreateObjects(req)

        assert len(rsp.subresponses[0].object_id) > 0
