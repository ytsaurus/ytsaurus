LIBRARY()

SRCS(
    yql_yt_join.cpp
    yql_yt_key_selector.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/row_spec
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/ast
)


   YQL_LAST_ABI_VERSION()


END()
