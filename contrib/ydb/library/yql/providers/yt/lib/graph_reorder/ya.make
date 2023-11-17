LIBRARY()

SRCS(
    yql_graph_reorder.cpp
    yql_graph_reorder_old.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()
