PROTO_LIBRARY()

SRCS(
    events.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
