LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/result/result.h)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/types/fatal_error_handlers
    contrib/libs/ydb-cpp-sdk/src/client/value
    contrib/libs/ydb-cpp-sdk/src/client/proto
)

END()
