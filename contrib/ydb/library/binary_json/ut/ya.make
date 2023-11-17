UNITTEST_FOR(contrib/ydb/library/binary_json)

SRCS(
    container_ut.cpp
    identity_ut.cpp
    entry_ut.cpp
    test_base.cpp
    valid_ut.cpp
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    FORK_SUBTESTS()
    TIMEOUT(2400)
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/library/binary_json
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
