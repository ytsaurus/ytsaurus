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
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/library/yql/dq/actors/protos
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/proto
    contrib/ydb/library/yql/providers/generic/connector/api/service
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    yql/essentials/public/issue
    yql/essentials/utils
    yql/essentials/utils/log
)

END()

RECURSE(
    ut_helpers
)
