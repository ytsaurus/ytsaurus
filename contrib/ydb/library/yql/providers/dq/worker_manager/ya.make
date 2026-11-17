YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/ydb_issue
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    contrib/ydb/library/yql/utils/actors
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

SRCS(
    local_worker_manager.cpp
)

END()

RECURSE(
    interface
)

RECURSE_FOR_TESTS(
    ut
)
