LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/mkql
    contrib/ydb/library/yql/providers/dq/opt
)

SRCS(
    dqs_task_graph.cpp
    execution_planner.cpp
)

YQL_LAST_ABI_VERSION()

END()
