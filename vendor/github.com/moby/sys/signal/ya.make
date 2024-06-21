GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    signal.go
)

GO_TEST_SRCS(signal_test.go)

IF (OS_LINUX)
    SRCS(
        signal_linux.go
        signal_unix.go
    )

    GO_TEST_SRCS(signal_linux_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        signal_darwin.go
        signal_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        signal_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
