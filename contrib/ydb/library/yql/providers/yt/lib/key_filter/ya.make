LIBRARY()

SRCS(
    yql_key_filter.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/ast
)

YQL_LAST_ABI_VERSION()

END()
