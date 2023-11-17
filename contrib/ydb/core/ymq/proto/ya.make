PROTO_LIBRARY()

SRCS(
    events.proto
    records.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
