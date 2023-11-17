UNITTEST_FOR(contrib/ydb/library/yql/minikql/arrow)

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    mkql_functions_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
