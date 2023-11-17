LIBRARY()

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/ydb_issue
    contrib/ydb/library/yql/utils/failure_injector
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/common/metrics
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
