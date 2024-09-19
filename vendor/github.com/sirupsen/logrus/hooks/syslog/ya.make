GO_LIBRARY()

LICENSE(MIT)

IF (OS_LINUX)
    SRCS(
        syslog.go
    )

    GO_TEST_SRCS(syslog_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        syslog.go
    )

    GO_TEST_SRCS(syslog_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
