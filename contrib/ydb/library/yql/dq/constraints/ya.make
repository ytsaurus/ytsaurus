YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/dq/expr_nodes
    yql/essentials/core
    yql/essentials/providers/common/transform
)

SRCS(
    dq_constraints.cpp
)

END()
