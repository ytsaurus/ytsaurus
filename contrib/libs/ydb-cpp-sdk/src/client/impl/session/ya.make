LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    kqp_session_common.cpp
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/endpoints
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
)

END()
