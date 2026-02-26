GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.6.0)

SRCS(
    inmem_socket.go
    proxy.go
    sockets.go
    tcp_socket.go
    unix_socket.go
)

GO_TEST_SRCS(
    inmem_socket_test.go
    unix_socket_test.go
)

IF (OS_LINUX)
    SRCS(
        sockets_unix.go
        unix_socket_unix.go
    )

    GO_TEST_SRCS(unix_socket_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sockets_unix.go
        unix_socket_unix.go
    )

    GO_TEST_SRCS(unix_socket_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sockets_windows.go
        unix_socket_windows.go
    )

    GO_TEST_SRCS(unix_socket_windows_test.go)
ENDIF()

IF (OS_ANDROID)
    SRCS(
        sockets_unix.go
        unix_socket_unix.go
    )

    GO_TEST_SRCS(unix_socket_unix_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
