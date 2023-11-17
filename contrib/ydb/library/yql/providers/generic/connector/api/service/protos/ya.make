PROTO_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/public/api/protos
)

# Because Go is excluded in YDB protofiles
EXCLUDE_TAGS(GO_PROTO)

SRCS(
    connector.proto
)

END()
