PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    cursor.proto
    selector.proto
    storage.proto
    task.proto
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/core/protos
)

END()
