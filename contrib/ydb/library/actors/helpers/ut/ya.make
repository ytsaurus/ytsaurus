UNITTEST_FOR(contrib/ydb/library/actors/helpers)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()


PEERDIR(
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/actors/core
)

SRCS(
    selfping_actor_ut.cpp
)

END()
