UNITTEST_FOR(contrib/ydb/library/yql/parser/pg_wrapper)

TIMEOUT(600)
SIZE(MEDIUM)

NO_COMPILER_WARNINGS()

INCLUDE(../cflags.inc)

SRCS(
    codegen_ut.cpp
    error_ut.cpp
    parser_ut.cpp
    sort_ut.cpp
    type_cache_ut.cpp
    ../../../minikql/comp_nodes/ut/mkql_test_factory.cpp
)


ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/minikql/codegen
    library/cpp/resource
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
