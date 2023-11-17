LIBRARY()

SRCS(
    kqp_node_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
