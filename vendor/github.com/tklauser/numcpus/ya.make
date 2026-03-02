GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.11.0)

SRCS(
    numcpus.go
)

GO_XTEST_SRCS(
    example_test.go
    numcpus_test.go
)

IF (OS_LINUX)
    SRCS(
        numcpus_linux.go
    )

    GO_TEST_SRCS(numcpus_linux_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        numcpus_bsd.go
        numcpus_list_unsupported.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        numcpus_list_unsupported.go
        numcpus_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        numcpus_linux.go
    )

    GO_TEST_SRCS(numcpus_linux_test.go)
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        numcpus_list_unsupported.go
        numcpus_unsupported.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
