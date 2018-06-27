try:
    from yt_yson_bindings import loads_proto, dumps_proto, loads, dumps
except ImportError:
    from yt_yson_bindings.yson_lib import loads_proto, dumps_proto, loads, dumps

from yt_proto.yt.core.misc.proto.error_pb2 import TError
from yt_proto.yt.core.ytree.proto.attributes_pb2 import TAttribute
from yp_proto.yp.client.api.proto.data_model_pb2 import TPodSetMeta, EObjectType
from yp_proto.yp.client.api.proto.object_service_pb2 import TReqCreateObject

class TestYsonProto(object):
    def test_simple(self):
        error = TError()
        error.code = 1
        error.message = "Hello World!"

        attribute = TAttribute()
        attribute.key = "host"
        attribute.value = "localhost"
        error.attributes.attributes.extend([attribute])

        error_dict = loads(dumps_proto(error))
        assert error_dict == {"code": 1, "message": "Hello World!", "attributes": {"host": "localhost"}}

        error_converted = loads_proto(dumps_proto(error), TError)
        assert error_converted.code == error.code
        assert error_converted.message == error.message

        meta = TPodSetMeta()
        meta.id = "1-1-1-1"
        meta.type = EObjectType.Value("OT_POD_SET")
        meta.creation_time = 0
        meta.id = "2-3-4-5"

        meta_dict = loads(dumps_proto(meta))
        assert meta_dict == {"id": "1-1-1-1", "type": "pod_set", "creation_time": 0, "id": "2-3-4-5"}

        meta_converted = loads_proto(dumps_proto(meta), TPodSetMeta)
        assert meta_converted.id == meta.id
        assert meta_converted.type == meta.type
        assert meta_converted.creation_time == meta.creation_time
        assert meta_converted.id == meta.id

        req = TReqCreateObject()
        req.object_type = EObjectType.Value("OT_POD_SET")

        req_dict = loads(dumps_proto(req))
        assert req_dict == {"object_type": "pod_set"}

        loads_proto(dumps_proto(req), TReqCreateObject)
