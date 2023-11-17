UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/comp_nodes)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    yql_mkql_output_ut.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/providers/yt/comp_nodes
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
