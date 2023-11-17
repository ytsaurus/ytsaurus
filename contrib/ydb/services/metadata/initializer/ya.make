LIBRARY()

SRCS(
    accessor_init.cpp
    behaviour.cpp
    common.cpp
    events.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
    initializer.cpp
    fetcher.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services
    contrib/ydb/services/metadata/request
)

END()

RECURSE_FOR_TESTS(
    ut
)