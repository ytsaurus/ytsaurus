YQL_LIBRARY()

SRCS(
    worker_dq.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    library/cpp/svnversion
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/providers/common/metrics
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/providers/dq/metrics
    yql/tools/yqlworker/config
    yql/tools/yqlworker/executor
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/rpc
    yql/tools/yqlworker/solomon_agent
    yt/cpp/mapreduce/interface
    yt/yql/providers/dq/actors
    yt/yql/providers/dq/actors/events
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/global_worker_manager
    yt/yql/providers/dq/service
    yt/yql/providers/dq/stats_collector
    yt/yql/providers/yt/dq_task_preprocessor
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
