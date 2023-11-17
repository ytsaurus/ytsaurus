LIBRARY()

SRCS(
    yql_expr_schema.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/parser/pg_catalog
)

END()
