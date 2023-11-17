UNITTEST_FOR(contrib/ydb/library/yql/core)

SRCS(
    yql_csv_ut.cpp
    yql_execution_ut.cpp
    yql_expr_constraint_ut.cpp
    yql_expr_discover_ut.cpp
    yql_expr_optimize_ut.cpp
    yql_expr_providers_ut.cpp
    yql_expr_type_annotation_ut.cpp
    yql_library_compiler_ut.cpp
    yql_opt_utils_ut.cpp
    yql_udf_index_ut.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/core/ut_common
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/provider
    contrib/ydb/library/yql/sql/pg
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
