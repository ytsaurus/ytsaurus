LIBRARY()

SRCS(
    bearer_credentials_provider.cpp
    factory.cpp
    token_accessor_client.cpp
    token_accessor_client_factory.cpp
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/threading/atomic
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/common/structured_token
    contrib/ydb/library/yql/providers/common/token_accessor/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()
