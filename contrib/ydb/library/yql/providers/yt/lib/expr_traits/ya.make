LIBRARY()

SRCS(
    yql_expr_traits.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
