PROTO_LIBRARY()

SRCS(
    graph_params.proto
)

PEERDIR(
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/dq/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
