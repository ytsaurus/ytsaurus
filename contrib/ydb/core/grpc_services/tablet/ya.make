LIBRARY()

SRCS(
    rpc_change_schema.cpp
    rpc_execute_mkql.cpp
    rpc_restart_tablet.cpp
    service_tablet.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/protos
    contrib/ydb/library/mkql_proto
    yql/essentials/minikql
    yql/essentials/minikql/computation
    contrib/ydb/public/api/protos
    library/cpp/protobuf/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
