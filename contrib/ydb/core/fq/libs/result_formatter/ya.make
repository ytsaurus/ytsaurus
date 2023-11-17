LIBRARY()

SRCS(
    result_formatter.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/public/udf
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_value
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/schema/mkql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
