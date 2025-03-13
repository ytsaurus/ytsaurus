LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_endpoints
    contrib/ydb/public/sdk/cpp/src/client/types/operation
)

END()
