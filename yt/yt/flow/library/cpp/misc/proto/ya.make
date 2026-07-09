PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

SRCS(
    node_info.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

EXCLUDE_TAGS(GO_PROTO)

END()
