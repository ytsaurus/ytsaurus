PROTO_LIBRARY()

SRCS(
    event.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()

