YQL_LIBRARY()

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/task_runner
    yql/essentials/minikql
    yql/essentials/utils/log
)

SRCS(
    events.cpp
    counters.cpp
)

END()
