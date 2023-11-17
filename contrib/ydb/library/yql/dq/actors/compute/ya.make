LIBRARY()

SRCS(
    dq_task_runner_exec_ctx.cpp
    dq_async_compute_actor.cpp
    dq_compute_actor_async_io_factory.cpp
    dq_compute_actor_channels.cpp
    dq_compute_actor_checkpoints.cpp
    dq_compute_actor_metrics.cpp
    dq_compute_actor_stats.cpp
    dq_compute_actor_watermarks.cpp
    dq_compute_actor.cpp
    dq_compute_issues_buffer.cpp
    retry_queue.cpp
    dq_request_context.h
    dq_request_context.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/wilson/protos
    contrib/ydb/library/services
    contrib/ydb/library/ydb_issue/proto
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/public/issue
    contrib/ydb/core/quoter/public
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
