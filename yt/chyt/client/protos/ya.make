PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

SRCS(
    query_service.proto
)

PEERDIR(yt/yt_proto/yt/core)

EXCLUDE_TAGS(GO_PROTO)

END()
