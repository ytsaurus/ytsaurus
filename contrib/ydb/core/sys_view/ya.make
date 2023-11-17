LIBRARY()

SRCS(
    scan.h
    scan.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/sys_view/nodes
    contrib/ydb/core/sys_view/partition_stats
    contrib/ydb/core/sys_view/query_stats
    contrib/ydb/core/sys_view/service
    contrib/ydb/core/sys_view/storage
    contrib/ydb/core/sys_view/tablets
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    common
    nodes
    partition_stats
    processor
    query_stats
    service
    storage
    tablets
)

RECURSE_FOR_TESTS(
    ut_kqp
)
