PROTO_LIBRARY()

SRCS(
    counters_shard.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
