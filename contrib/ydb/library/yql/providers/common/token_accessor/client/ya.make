LIBRARY()

SRCS(
    bearer_credentials_provider.cpp
    factory.cpp
    token_accessor_client.cpp
    token_accessor_client_factory.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic_bool
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/common/token_accessor/grpc
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
    contrib/ydb/public/sdk/cpp/src/client/types/credentials/login
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/providers/common/structured_token
)

END()
