LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/client/value
    contrib/ydb/public/sdk/cpp/src/client/proto
)

END()
