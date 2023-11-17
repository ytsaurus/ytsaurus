UNITTEST_FOR(contrib/ydb/library/yql/minikql/invoke_builtins/llvm)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCDIR(contrib/ydb/library/yql/minikql/invoke_builtins)

SRCS(
    mkql_builtins_ut.cpp
)

END()
