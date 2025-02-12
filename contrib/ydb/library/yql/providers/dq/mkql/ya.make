LIBRARY()

PEERDIR(
    yql/essentials/core
    yql/essentials/core/dq_integration
    yql/essentials/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/dq/expr_nodes
)

SRCS(
    dqs_mkql_compiler.cpp
    parser.cpp
)

YQL_LAST_ABI_VERSION()

END()
