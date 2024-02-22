PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

SRCS(
    controller_service.proto
)

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt_proto.yt.core)

EXCLUDE_TAGS(GO_PROTO)

PEERDIR(
    yt/yt_proto/yt/core
)

END()
