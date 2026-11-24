LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    out.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/topic/codecs
    contrib/libs/ydb-cpp-sdk/src/client/topic/common
    contrib/libs/ydb-cpp-sdk/src/client/topic/impl
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic

    contrib/libs/ydb-cpp-sdk/src/client/proto
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/table

    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
)

END()
