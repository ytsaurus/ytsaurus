LIBRARY()

SRCS(
    kqp_gateway.cpp
    kqp_ic_gateway.cpp
    kqp_metadata_loader.cpp
)

PEERDIR(
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/gateway/behaviour/external_data_source
    contrib/ydb/core/kqp/gateway/behaviour/resource_pool
    contrib/ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    contrib/ydb/core/kqp/gateway/behaviour/table
    contrib/ydb/core/kqp/gateway/behaviour/tablestore
    contrib/ydb/core/kqp/gateway/behaviour/view
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/kqp/query_data
    contrib/ydb/core/statistics/service
    contrib/ydb/core/sys_view/common
    contrib/ydb/library/actors/core
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    behaviour
    local_rpc
    utils
)

RECURSE_FOR_TESTS(ut)
