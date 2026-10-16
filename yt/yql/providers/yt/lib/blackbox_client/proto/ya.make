PROTO_LIBRARY()

SRCS(
    blackbox.proto
)

PEERDIR(
    yt/yql/providers/yt/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
