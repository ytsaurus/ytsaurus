YQL_LIBRARY()

SRCS(
    task_subscriptions.cpp
    worker_core.cpp
)

PEERDIR(
    library/cpp/svnversion
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/tools/yqlworker/config
    yql/tools/yqlworker/executor
    yql/tools/yqlworker/executor/subproc
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/rpc
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
