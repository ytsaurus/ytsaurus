PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

SRCS(
    message_service.proto
)

PEERDIR(
    yt/yt_proto/yt/core
    yt/yt/flow/library/cpp/common/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
