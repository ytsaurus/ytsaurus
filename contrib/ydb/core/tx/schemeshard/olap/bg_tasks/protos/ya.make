PROTO_LIBRARY()

SRCS(
    data.proto
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
