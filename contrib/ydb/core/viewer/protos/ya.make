PROTO_LIBRARY()

SRCS(
    viewer.proto
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/graph/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
