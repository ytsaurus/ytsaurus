PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    events.proto
    blobs.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
)

END()
