PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    data.proto
    events.proto
    sessions.proto
    initiator.proto
    links.proto
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/library/actors/protos
    contrib/ydb/core/tx/columnshard/blobs_action/protos
)

END()
