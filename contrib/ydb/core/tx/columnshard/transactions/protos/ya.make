PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    tx_event.proto
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/core/protos
)

END()
