LIBRARY()

SRCS(
    task_runner_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/runtime # TODO: split runtime/runtime_data
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/library/yql/providers/dq/task_runner
)

YQL_LAST_ABI_VERSION()

END()
