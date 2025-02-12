LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/ydb_issue
    yql/essentials/utils/failure_injector
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/providers/dq/task_runner_actor
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
)

YQL_LAST_ABI_VERSION()

SRCS(
    local_worker_manager.cpp
)

END()

RECURSE(
    interface
)
