YQL_LIBRARY()

SRCS(
    worker_dq.cpp
)

PEERDIR(
    library/cpp/svnversion
    yt/cpp/mapreduce/interface
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/essentials/utils/backtrace
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    yt/yql/providers/dq/service
    yql/providers/dq/metrics
    yt/yql/providers/dq/stats_collector
    yt/yql/providers/yt/dq_task_preprocessor
    yql/tools/yqlworker/config
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/global_worker_manager
    yql/tools/yqlworker/executor
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/rpc
    yql/tools/yqlworker/solomon_agent
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        yql/tools/yqlworker/dq/yt_log
    )
ELSE()
    PEERDIR(
        yql/tools/yqlworker/dq/yt_log_win
    )
ENDIF()

END()
