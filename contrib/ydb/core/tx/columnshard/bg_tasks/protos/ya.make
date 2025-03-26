PROTO_LIBRARY()

SRCS(
    data.proto
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/services/bg_tasks/protos
)

END()
