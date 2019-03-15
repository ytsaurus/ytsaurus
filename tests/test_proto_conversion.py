from yp.client import to_proto_enum
from yp.data_model import EObjectType


def test_enum():
    enum = to_proto_enum(EObjectType, "pod")
    assert enum.name == "OT_POD"
