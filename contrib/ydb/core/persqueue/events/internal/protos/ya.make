PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SET(PROTOC_TRANSITIVE_HEADERS "no")

SRCS(
    events.proto
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
