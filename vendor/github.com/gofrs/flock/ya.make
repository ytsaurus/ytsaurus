GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.12.1)

SRCS(
    flock.go
)

IF (ARCH_X86_64)
    GO_TEST_SRCS(flock_internal_test.go)

    GO_XTEST_SRCS(
        flock_example_test.go
        flock_test.go
    )
ENDIF()

IF (ARCH_ARM64)
    GO_TEST_SRCS(flock_internal_test.go)

    GO_XTEST_SRCS(
        flock_example_test.go
        flock_test.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM6 OR OS_LINUX AND ARCH_ARM7)
    GO_TEST_SRCS(flock_internal_test.go)

    GO_XTEST_SRCS(
        flock_example_test.go
        flock_test.go
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

IF (OS_ANDROID)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        flock_others.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
