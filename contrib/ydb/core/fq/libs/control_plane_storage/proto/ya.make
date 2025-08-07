PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/public/api/protos
)

SRCS(
    yq_internal.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
