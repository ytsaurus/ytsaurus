PROTO_LIBRARY()

SRCS(
    service.proto
    dqs.proto
    task_command_executor.proto
)

PEERDIR(
    library/cpp/actors/protos
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/common/metrics/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
