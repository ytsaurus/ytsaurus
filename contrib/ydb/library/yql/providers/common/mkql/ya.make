LIBRARY()

SRCS(
    parser.cpp
    parser.h
    yql_provider_mkql.cpp
    yql_provider_mkql.h
    yql_type_mkql.cpp
    yql_type_mkql.h
)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
