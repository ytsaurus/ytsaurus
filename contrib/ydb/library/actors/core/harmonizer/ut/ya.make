UNITTEST_FOR(contrib/ydb/library/actors/core/harmonizer)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    TIMEOUT(600)
ELSE()
    SIZE(SMALL)
    TIMEOUT(60)
ENDIF()


PEERDIR(
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/testlib
)

SRCS(
    harmonizer_ut.cpp
    history_ut.cpp
)

END()
