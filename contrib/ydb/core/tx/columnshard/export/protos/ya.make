PROTO_LIBRARY()

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
