LIBRARY()

SRCS(
    secret.cpp
    GLOBAL secret_behaviour.cpp
    checker_secret.cpp
    access.cpp
    GLOBAL access_behaviour.cpp
    checker_access.cpp

    manager.cpp
    snapshot.cpp
    initializer.cpp
    fetcher.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services
    contrib/ydb/services/metadata/request
    contrib/ydb/services/metadata/secret/accessor
)

END()

RECURSE_FOR_TESTS(
    ut
)