YQL_LIBRARY()

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/counters
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/invoke_builtins
    yql/essentials/protos
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
)

SRCS(
    tasks_runner_local.cpp
    tasks_runner_proxy.cpp
)

END()
