YQL_LIBRARY()

SRCS(
    compute_actor.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    grouped_issues.cpp
    proto_builder.cpp
    resource_allocator.cpp
    result_receiver.cpp
    task_controller.cpp
    worker_actor.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/core/fq/libs/checkpointing/events
    contrib/ydb/library/actors/core
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    contrib/ydb/library/yql/utils/actor_log
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/public/api/protos
    yql/essentials/core
    yql/essentials/providers/common/metrics
    yql/essentials/public/issue
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
