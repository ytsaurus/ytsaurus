LIBRARY()

SRCS(
    yql_mkql_schema.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
