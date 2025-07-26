PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    metadata.proto
    operation.proto
)

PEERDIR(
    contrib/ydb/public/api/client/nc_private
    contrib/ydb/public/api/client/nc_private/buf/validate
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
