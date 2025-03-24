UNITTEST_FOR(contrib/ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/library/actors/testlib
)

IF (NOT OS_WINDOWS)
SRCS(
    http_ut.cpp
)
ELSE()
ENDIF()

END()
