LIBRARY()

SRCS(
    compute_actor.cpp
    dummy_lock.cpp
    dynamic_nameserver.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    graph_execution_events_actor.cpp
    resource_allocator.cpp
    task_controller.cpp
    worker_actor.cpp
    result_aggregator.cpp
    result_receiver.cpp
    full_result_writer.cpp
    proto_builder.cpp
    grouped_issues.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/yson
    contrib/ydb/library/mkql_proto
    yql/essentials/core/services
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/core
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/library/yql/utils/actor_log
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/yson_value
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/actors/compute
    yql/essentials/utils/failure_injector
    yql/essentials/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/actors/events
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/config
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/providers/dq/task_runner_actor
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    yt
)

RECURSE_FOR_TESTS(
    ut
)
