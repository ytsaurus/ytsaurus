PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/public/api/protos
)

SRCS(
    dq_effects.proto
    fq_private.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
