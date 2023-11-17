PROTO_LIBRARY()

SRCS(
    dq_events.proto
    dq_stats.proto
    dq_status_codes.proto
)

PEERDIR(
    library/cpp/actors/protos
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/public/types
)

EXCLUDE_TAGS(GO_PROTO)

END()
