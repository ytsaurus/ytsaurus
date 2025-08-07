PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    data.proto
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/services/bg_tasks/protos
)

END()
