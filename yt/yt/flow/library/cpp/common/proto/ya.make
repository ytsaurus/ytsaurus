PROTO_LIBRARY()

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
