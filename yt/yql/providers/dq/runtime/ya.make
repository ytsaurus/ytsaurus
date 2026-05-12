LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/parser/pg_catalog
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/provider
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/failure_injector
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    task_command_executor.cpp
)

END()

RECURSE_FOR_TESTS(ut)
