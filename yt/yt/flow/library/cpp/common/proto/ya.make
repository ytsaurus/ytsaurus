PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

PROTO_NAMESPACE(yt)

SRCS(
    admin_service.proto
    message.proto
    timer.proto
    visit.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

EXCLUDE_TAGS(GO_PROTO)

END()
