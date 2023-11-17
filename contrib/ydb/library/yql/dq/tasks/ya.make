LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/ast
)

SRCS(
    dq_task_program.cpp
)


   YQL_LAST_ABI_VERSION()


END()
