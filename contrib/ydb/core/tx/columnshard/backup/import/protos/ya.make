PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    task.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

END()
