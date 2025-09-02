LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/client/value
    contrib/ydb/public/sdk/cpp/src/client/proto
)

END()
