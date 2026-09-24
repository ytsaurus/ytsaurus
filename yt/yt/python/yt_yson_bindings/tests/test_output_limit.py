from google.protobuf import descriptor_pb2

from yt.testlib import authors

import yt_yson_bindings


@authors("pechatnov")
def test_dumps_proto_respects_small_output_limit():
    message = descriptor_pb2.FileDescriptorProto(
        name="x" * 20,
        package="y" * 20,
    )

    result = yt_yson_bindings.dumps_proto(message, yson_format="text", output_limit=10)

    assert result == b'{"name"="xx";}'
