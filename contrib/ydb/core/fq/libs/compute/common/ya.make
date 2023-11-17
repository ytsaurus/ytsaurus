LIBRARY()

SRCS(
    pinger.cpp
    run_actor_params.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/json/yson
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/fq/libs/grpc
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
