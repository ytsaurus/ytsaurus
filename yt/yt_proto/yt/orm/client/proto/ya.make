PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt_proto.yt.orm.client.proto)

SRCS(
    error.proto
    generic_object_service.proto
    object.proto
)

IF (GO_PROTO)
    PEERDIR(
        yt/go/proto/client/api/rpc_proxy
        yt/go/proto/core/yson
    )
ELSE()
    PEERDIR(
        yt/yt_proto/yt/client
        yt/yt_proto/yt/core
    )
ENDIF()

END()
