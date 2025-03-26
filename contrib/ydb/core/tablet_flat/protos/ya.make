LIBRARY()

SRCS(
    flat_table_part.proto
    flat_table_shard.proto
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/core/protos
    contrib/ydb/core/scheme/protos
)

END()
