PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    events.proto
    records.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
