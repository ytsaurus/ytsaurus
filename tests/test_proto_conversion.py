from yp.client import to_proto_enum

from yp_proto.yp.client.api.proto.data_model_pb2 import EObjectType

def test_enum():
    enum = to_proto_enum(EObjectType, "pod")
    assert enum.name == "OT_POD"

