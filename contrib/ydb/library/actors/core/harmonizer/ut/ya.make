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
    cpu_count_ut.cpp
    harmonizer_ut.cpp
    history_ut.cpp
    shared_info_ut.cpp
)

END()
