PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    service.proto
    dqs.proto
    task_command_executor.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/dq/proto
    yql/essentials/providers/common/metrics/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
