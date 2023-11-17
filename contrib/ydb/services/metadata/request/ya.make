LIBRARY()

SRCS(
    request_actor.cpp
    request_actor_cb.cpp
    config.cpp
    common.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services
)

END()
