LIBRARY()

SRCS(
    collection.cpp
    predicate_node.cpp
    settings.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core/expr_nodes_gen
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
