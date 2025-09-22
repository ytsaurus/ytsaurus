LIBRARY()

SRCS(
    kqp_session_common.cpp
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/endpoints
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

END()
