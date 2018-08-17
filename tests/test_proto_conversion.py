from yp.client import to_proto_enum

from yp_proto.yp.client.api.proto.object_type_pb2 import EObjectType

def test_enum():
    enum = to_proto_enum(EObjectType, "pod")
    assert enum.name == "OT_POD"
