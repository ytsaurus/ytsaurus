LIBRARY()

SRCS(
    kqp_resource_estimation.cpp
    kqp_resource_info_exchanger.cpp
    kqp_rm_service.cpp
    kqp_snapshot_manager.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/util
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
