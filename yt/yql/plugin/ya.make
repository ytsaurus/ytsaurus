LIBRARY()

SRCS(
    plugin.cpp
    config.cpp
    dq_manager.cpp
)

PEERDIR(
    yt/yt/core

    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/stats_collector
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/actors/yt
    contrib/ydb/library/yql/providers/dq/service
    contrib/ydb/library/yql/providers/dq/global_worker_manager
    contrib/ydb/library/yql/providers/yt/dq_task_preprocessor

    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_wrapper

    yt/yql/providers/yt/codec
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bridge
    dynamic
    native
    process
)
