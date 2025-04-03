LIBRARY()

SRCS(
    coordination.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/common_client
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/public/sdk/cpp/src/client/types/status
)

END()
