LIBRARY()

SRCS(
    yql_expr_nodes_gen.h
    yql_expr_nodes_gen.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/public/udf
)

END()
