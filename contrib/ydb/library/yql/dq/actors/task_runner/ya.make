YQL_LIBRARY()

SRCS(
    events.cpp
    task_runner_actor_local.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/comp_nodes/operator_memory_quota
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    yql/essentials/minikql/computation
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/library/services
)

END()
