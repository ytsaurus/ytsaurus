YQL_LIBRARY()

SRCS(
    task_executor.cpp
)

PEERDIR(
    yql/tools/yqlworker/executor/inproc
)

IF (OS_LINUX OR OS_DARWIN)
    PEERDIR(
        yql/tools/yqlworker/executor/subproc
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
