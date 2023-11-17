LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/expr_nodes
)

SRCS(
    dqs_mkql_compiler.cpp
)

YQL_LAST_ABI_VERSION()

END()
