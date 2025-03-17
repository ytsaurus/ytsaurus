PROTO_LIBRARY()

GRPC()

SRCS(
    fq_private_v1.proto
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/core/fq/libs/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
