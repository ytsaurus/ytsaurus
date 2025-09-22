LIBRARY()

SRCS(
    authenticator.cpp
    endpoint_pool.cpp
    state.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/threading/future
    contrib/ydb/public/sdk/cpp/src/client/impl/endpoints
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/logger
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
