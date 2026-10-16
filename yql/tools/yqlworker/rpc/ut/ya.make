IF (OS_LINUX OR OS_DARWIN)

UNITTEST_FOR(yql/tools/yqlworker/rpc)

IF (SANITIZER_TYPE)
    TIMEOUT(120)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

SRCS(
    inspector_client_msgbus_ut.cpp
)

PEERDIR(
    library/cpp/testing/mock_server
)

END()

ENDIF()
