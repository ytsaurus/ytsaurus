PROTO_LIBRARY()

PEERDIR(
    yt/yt_proto/yt/formats
)

SRCS(
    config.proto
    kv.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
