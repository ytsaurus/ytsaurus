UNITTEST_FOR(contrib/ydb/library/arrow_kernels)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
)

ADDINCL(
    contrib/ydb/library/arrow_clickhouse
)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_common.cpp
    ut_arithmetic.cpp
    ut_math.cpp
    ut_round.cpp
)

END()
