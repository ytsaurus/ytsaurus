LIBRARY()

PEERDIR(
    yql/essentials/core
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/type_ann
    yql/essentials/ast
)

SRCS(
    dq_task_program.cpp
)


   YQL_LAST_ABI_VERSION()


END()
