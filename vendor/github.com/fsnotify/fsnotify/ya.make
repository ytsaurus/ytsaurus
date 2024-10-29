GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.7.0)

GO_SKIP_TESTS(TestInotifyNoBlockingSyscalls)

SRCS(
    fsnotify.go
)

GO_TEST_SRCS(
    fsnotify_test.go
    helpers_test.go
)

IF (OS_LINUX)
    SRCS(
        backend_inotify.go
    )

    GO_TEST_SRCS(backend_inotify_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        backend_kqueue.go
        system_darwin.go
    )

    GO_TEST_SRCS(backend_kqueue_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        backend_windows.go
    )

    GO_TEST_SRCS(backend_windows_test.go)
ENDIF()

END()

RECURSE(
    cmd
    gotest
)

IF (OS_LINUX)
    RECURSE(
        internal
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        internal
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        internal
    )
ENDIF()
