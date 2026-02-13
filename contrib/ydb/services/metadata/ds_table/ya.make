LIBRARY()

SRCS(
    accessor_refresh.cpp
    accessor_subscribe.cpp
    accessor_snapshot_simple.cpp
    accessor_snapshot_base.cpp
    behaviour_registrator_actor.cpp
    scheme_describe.cpp
    table_exists.cpp
    service.cpp
    config.cpp
    registration.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/services/metadata/common
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/secret
)

END()
