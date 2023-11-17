LIBRARY()

SRCS(
    rate_limiter.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

END()
