LIBRARY()

PEERDIR(
    yql/essentials/core/dq_integration
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
)

SRCS(
    yql_dq_helper_impl.cpp
)

END()
