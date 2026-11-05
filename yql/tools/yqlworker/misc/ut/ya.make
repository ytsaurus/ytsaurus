
IF (OS_LINUX OR OS_DARWIN)

UNITTEST_FOR(yql/tools/yqlworker/misc)

IF (SANITIZER_TYPE)
    TIMEOUT(120)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

SRCS(
    duplex_ipc_channel_ut.cpp
    expiration_tracker_ut.cpp
    test.proto
    multiprocessing_ut.cpp
    socket_transfer_ut.cpp
)

REQUIREMENTS(ram:9)

END()

ENDIF()

