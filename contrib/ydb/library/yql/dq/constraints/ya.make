YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/type_ann
    yql/essentials/core
    yql/essentials/ast
    yql/essentials/providers/common/transform
)

SRCS(
    dq_constraints.cpp
)

END()
