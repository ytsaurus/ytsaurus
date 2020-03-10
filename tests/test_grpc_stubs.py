import yp_proto.yp.client.api.proto.object_service_pb2 as object_service_pb2

import yp.data_model as data_model

import yt.yson as yson

import pytest

try:
    from itertools import imap
except ImportError:  # Python 3
    imap = map


@pytest.mark.usefixtures("yp_env")
class TestGrpcStubs(object):
    def _test_some_methods(self, yp_client):
        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD_SET
        rsp = object_stub.CreateObject(req)

        pod_set_id = rsp.object_id

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD
        req.attributes = yson.dumps({"meta": {"pod_set_id": pod_set_id}})
        rsp = object_stub.CreateObject(req)

        pod_id = rsp.object_id

        req = object_service_pb2.TReqGetObject()
        req.object_type = data_model.OT_POD
        req.object_id = pod_id
        req.selector.paths[:] = ["/status/agent/state", "/meta/id", "/meta/pod_set_id"]
        rsp = object_stub.GetObject(req)

        assert list(imap(yson._loads_from_native_str, rsp.result.values)) == [
            "unknown",
            pod_id,
            pod_set_id,
        ]

    def test_grpc_client(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_some_methods(yp_client)

    def test_http_client(self, yp_env):
        with yp_env.yp_instance.create_client(transport="http") as yp_client:
            self._test_some_methods(yp_client)

    def _test_proto_payload_create_object(self, yp_client):
        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD_SET
        rsp = object_stub.CreateObject(req)
        pod_set_id = rsp.object_id

        pod = data_model.TPod()
        pod.spec.enable_scheduling = True
        pod.meta.pod_set_id = pod_set_id

        req = object_service_pb2.TReqCreateObject()
        req.object_type = data_model.OT_POD
        req.attributes_payload.protobuf = pod.SerializeToString()
        rsp = object_stub.CreateObject(req)

        pod_id = rsp.object_id

        meta = yp_client.get_object("pod", pod_id, selectors=["/meta"])[0]
        assert meta["id"] == pod_id
        assert meta["pod_set_id"] == pod_set_id

    def test_proto_payload_create_object(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_proto_payload_create_object(yp_client)

    def _test_proto_payload_get_object(self, yp_client):
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        object_stub = yp_client.create_grpc_object_stub()
        req = object_service_pb2.TReqGetObject()
        req.object_type = data_model.OT_POD
        req.object_id = pod_id
        req.format = object_service_pb2.PF_PROTOBUF
        req.selector.paths[:] = ["/meta"]
        rsp = object_stub.GetObject(req)

        assert len(rsp.result.value_payloads) == 1
        meta = data_model.TPodMeta.FromString(rsp.result.value_payloads[0].protobuf)
        assert meta.id == pod_id
        assert meta.pod_set_id == pod_set_id

    def test_proto_payload_get_object(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_proto_payload_get_object(yp_client)

    def _test_proto_payload_select_objects(self, yp_client):
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        object_stub = yp_client.create_grpc_object_stub()
        req = object_service_pb2.TReqSelectObjects()
        req.object_type = data_model.OT_POD
        req.format = object_service_pb2.PF_PROTOBUF
        req.selector.paths[:] = ["/meta"]
        rsp = object_stub.SelectObjects(req)

        assert len(rsp.results) == 1
        assert len(rsp.results[0].value_payloads) == 1
        meta = data_model.TPodMeta.FromString(rsp.results[0].value_payloads[0].protobuf)
        assert meta.id == pod_id
        assert meta.pod_set_id == pod_set_id

    def test_proto_payload_select_objects(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_proto_payload_select_objects(yp_client)

    def _test_proto_payload_update_object(self, yp_client):
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        object_stub = yp_client.create_grpc_object_stub()
        req = object_service_pb2.TReqUpdateObject()
        req.object_type = data_model.OT_POD
        req.object_id = pod_id
        update = req.set_updates.add()
        pod_agent_payload = data_model.TPodSpec.TPodAgentPayload()
        pod_agent_payload.spec.id = "some_id"
        update.value_payload.protobuf = pod_agent_payload.SerializeToString()
        update.path = "/spec/pod_agent_payload"
        object_stub.UpdateObject(req)

        assert (
            yp_client.get_object("pod", pod_id, selectors=["/spec/pod_agent_payload/spec/id"])[0]
            == "some_id"
        )

    def test_proto_payload_update_object(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_proto_payload_update_object(yp_client)

    def _test_proto_payload_update_objects(self, yp_client):
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        object_stub = yp_client.create_grpc_object_stub()
        req = object_service_pb2.TReqUpdateObjects()
        subreq = req.subrequests.add()
        subreq.object_type = data_model.OT_POD
        subreq.object_id = pod_id
        update = subreq.set_updates.add()
        pod_agent_payload = data_model.TPodSpec.TPodAgentPayload()
        pod_agent_payload.spec.id = "some_id"
        update.value_payload.protobuf = pod_agent_payload.SerializeToString()
        update.path = "/spec/pod_agent_payload"
        object_stub.UpdateObjects(req)

        assert (
            yp_client.get_object("pod", pod_id, selectors=["/spec/pod_agent_payload/spec/id"])[0]
            == "some_id"
        )

    def test_proto_payload_update_objects(self, yp_env):
        with yp_env.yp_instance.create_client(transport="grpc") as yp_client:
            self._test_proto_payload_update_objects(yp_client)
