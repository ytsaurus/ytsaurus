GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.2.5)

SRCS(
    channel.go
    client.go
    codec.go
    config.go
    doc.go
    errors.go
    handshake.go
    interceptor.go
    metadata.go
    request.pb.go
    server.go
    services.go
    stream.go
    stream_server.go
)

GO_TEST_SRCS(
    channel_test.go
    client_test.go
    interceptor_test.go
    metadata_test.go
    server_test.go
    services_test.go
    stream_test.go
)

IF (OS_LINUX)
    SRCS(
        unixcreds_linux.go
    )

    GO_TEST_SRCS(server_linux_test.go)
ENDIF()

IF (OS_ANDROID)
    SRCS(
        unixcreds_linux.go
    )

    GO_TEST_SRCS(server_linux_test.go)
ENDIF()

END()

RECURSE(
    gotest
    internal
)
