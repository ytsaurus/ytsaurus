LIBRARY()

SRCS(
    pinger.cpp
    run_actor_params.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/json/yson
    contrib/ydb/core/fq/libs/config/protos
    # ydb/core/fq/libs/control_plane_storage/internal
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/fq/libs/grpc
    contrib/ydb/core/fq/libs/shared_resources
    yql/essentials/public/issue
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/public/lib/ydb_cli/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
