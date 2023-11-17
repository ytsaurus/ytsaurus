LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/library/yql/providers/pq/task_meta
)

SRCS(
    dq_state_load_plan.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
