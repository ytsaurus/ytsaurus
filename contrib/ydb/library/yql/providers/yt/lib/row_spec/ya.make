LIBRARY()

SRCS(
    yql_row_spec.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/expr_nodes_gen
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
