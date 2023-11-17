LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/task_runner
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    task_command_executor.cpp
    runtime_data.cpp
)

END()
