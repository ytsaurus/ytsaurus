GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.4.0)

SRCS(
    idtools.go
    user.go
)

GO_TEST_SRCS(user_test.go)

IF (OS_LINUX)
    SRCS(
        idtools_unix.go
        lookup_unix.go
    )

    GO_TEST_SRCS(idtools_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        idtools_unix.go
        lookup_unix.go
    )

    GO_TEST_SRCS(idtools_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        idtools_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        idtools_unix.go
        lookup_unix.go
    )

    GO_TEST_SRCS(idtools_unix_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
