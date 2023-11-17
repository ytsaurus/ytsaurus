LIBRARY()

PEERDIR(
    library/cpp/svnversion
    library/cpp/threading/task_scheduler
    library/cpp/yson/node
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/counters
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    tasks_runner_local.cpp
    tasks_runner_proxy.cpp
    tasks_runner_pipe.cpp
)

END()
