LIBRARY()

SRCS(
    client.cpp
    error.cpp
    utils.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/grpc
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/library/yql/providers/generic/connector/api/service
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/utils
)

END()

RECURSE(
    ut_helpers
)
