LIBRARY()

SRCS(
    events.cpp
    task_runner_actor_local.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
