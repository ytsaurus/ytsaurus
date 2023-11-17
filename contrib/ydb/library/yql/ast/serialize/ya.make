LIBRARY()

SRCS(
    yql_expr_serialize.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/minikql
)

END()
