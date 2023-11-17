LIBRARY()

SRCS(
    yql_result_provider.cpp
    yql_result_provider.h
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
