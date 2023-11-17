PROTO_LIBRARY()

SRCS(
    viewer.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
