YQL_LIBRARY()

SRCS(
    task_runner_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/utils/actors
)

END()
