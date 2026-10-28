GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.47.0)

IF (OS_WINDOWS)
    SRCS(
        key.go
        syscall.go
        value.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(export_test.go)

    GO_XTEST_SRCS(registry_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
