GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    path.go
)

GO_TEST_SRCS(path_test.go)

IF (OS_LINUX)
    SRCS(
        atime_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        atime_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        atime_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
