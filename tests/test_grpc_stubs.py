from yp.client import YpClient

import yp_proto.yp.client.api.proto.object_service_pb2 as object_service_pb2
import yp_proto.yp.client.api.proto.object_type_pb2 as object_type_pb2
import yp_proto.yp.client.api.proto.data_model_pb2 as data_model_pb2

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
        req.object_type = object_type_pb2.OT_POD_SET
        rsp = object_stub.CreateObject(req)

        pod_set_id = rsp.object_id

        req = object_service_pb2.TReqCreateObject()
        req.object_type = object_type_pb2.OT_POD
        req.attributes = yson.dumps({"meta": {"pod_set_id": pod_set_id}})
        rsp = object_stub.CreateObject(req)

        pod_id = rsp.object_id

        req = object_service_pb2.TReqGetObject()
        req.object_type = object_type_pb2.OT_POD
        req.object_id = pod_id
        req.selector.paths[:] = ["/status/agent/state", "/meta/id", "/meta/pod_set_id"]
        rsp = object_stub.GetObject(req)

        assert list(imap(yson._loads_from_native_str, rsp.result.values)) == ["unknown", pod_id, pod_set_id]

    def test_grpc_client(self, yp_env):
        self._test_some_methods(yp_env.yp_instance.create_client(transport="grpc"))

    def test_http_client(self, yp_env):
        self._test_some_methods(yp_env.yp_instance.create_client(transport="http"))

    def test_proto_payload(self, yp_env):
        yp_client = yp_env.yp_instance.create_client(transport="grpc")
        object_stub = yp_client.create_grpc_object_stub()

        req = object_service_pb2.TReqCreateObject()
        req.object_type = object_type_pb2.OT_POD_SET
        rsp = object_stub.CreateObject(req)
        pod_set_id = rsp.object_id


        pod = data_model_pb2.TPod()
        pod.spec.enable_scheduling = True
        pod.meta.pod_set_id = pod_set_id

        req = object_service_pb2.TReqCreateObject()
        req.object_type = object_type_pb2.OT_POD
        req.attributes_payload.protobuf = pod.SerializeToString()
        rsp = object_stub.CreateObject(req)

        pod_id = rsp.object_id

        req = object_service_pb2.TReqGetObject()
        req.object_type = object_type_pb2.OT_POD
        req.object_id = pod_id
        req.format = object_service_pb2.PF_PROTOBUF
        req.selector.paths[:] = ["/meta"]

        rsp = object_stub.GetObject(req)
        assert len(rsp.result.value_payloads) == 1
        pod_meta_rsp = data_model_pb2.TPodMeta.FromString(rsp.result.value_payloads[0].protobuf)
        assert pod_meta_rsp.id == pod_id
        assert pod_meta_rsp.pod_set_id == pod_set_id
