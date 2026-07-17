LIBRARY()

SRCS(
    compute_actor.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    full_result_writer.cpp
    graph_execution_events_actor.cpp
    grouped_issues.cpp
    proto_builder.cpp
    resource_allocator.cpp
    result_aggregator.cpp
    result_receiver.cpp
    task_controller.cpp
    worker_actor.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/actors/core
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/providers/dq/actors/events
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/providers/dq/task_runner_actor
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/library/yql/utils/actor_log
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/yson_value
    yql/essentials/core
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
