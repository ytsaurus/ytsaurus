LIBRARY()

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/sdk/cpp/client/impl/ydb_endpoints
)


END()
