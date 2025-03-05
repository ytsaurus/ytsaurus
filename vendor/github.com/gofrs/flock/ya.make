GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.12.1)

SRCS(
    flock.go
)

GO_TEST_SRCS(flock_internal_test.go)

GO_XTEST_SRCS(
    flock_example_test.go
    flock_test.go
)

IF (OS_LINUX)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        flock_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
