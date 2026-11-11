UNITTEST_FOR(yql/tools/yqlworker/executor/subproc)

SRCS(
    data_protocol_ut.cpp
    task_index_ut.cpp
)

IF (OS_LINUX OR OS_DARWIN)

SRCS(
    control_protocol_ut.cpp
)

ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
