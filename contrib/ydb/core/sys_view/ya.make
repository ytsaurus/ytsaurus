LIBRARY()

SRCS(
    scan.h
    scan.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/sys_view/auth
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/sys_view/compile_cache
    contrib/ydb/core/sys_view/nodes
    contrib/ydb/core/sys_view/partition_stats
    contrib/ydb/core/sys_view/pg_tables
    contrib/ydb/core/sys_view/query_stats
    contrib/ydb/core/sys_view/resource_pool_classifiers
    contrib/ydb/core/sys_view/resource_pools
    contrib/ydb/core/sys_view/service
    contrib/ydb/core/sys_view/sessions
    contrib/ydb/core/sys_view/show_create
    contrib/ydb/core/sys_view/storage
    contrib/ydb/core/sys_view/tablets
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/wrappers
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    auth
    common
    nodes
    partition_stats
    pg_tables
    processor
    query_stats
    resource_pool_classifiers
    resource_pools
    service
    storage
    tablets
)

RECURSE_FOR_TESTS(
    ut
    ut_large
)
