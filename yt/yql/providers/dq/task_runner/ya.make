YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/task_runner
    library/cpp/svnversion
    library/cpp/threading/task_scheduler
    library/cpp/yson/node
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/invoke_builtins
    yql/essentials/protos
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

SRCS(
    file_cache.cpp
    tasks_runner_pipe.cpp
)

IF (OS_WINDOWS)
    SRCS(tasks_runner_pipe_process_win.cpp)
ELSE()
    SRCS(tasks_runner_pipe_process.cpp)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
