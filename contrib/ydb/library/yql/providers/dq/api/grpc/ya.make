PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

GRPC()

SRCS(
    api.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
