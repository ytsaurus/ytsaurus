LIBRARY()

SRCS(
    kqp_node_service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/actors/async
)

YQL_LAST_ABI_VERSION()

END()
