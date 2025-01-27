GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

DEPENDS(yt/yt/core/rpc/unittests/bin)

SRCS(
    bus.go
    client.go
    send_options.go
)

GO_TEST_SRCS(
    bus_test.go
    client_test.go
    test_service_test.go
)

IF (OPENSOURCE)
    ENV(TEST_SERVICE_BINARY_PATH=${ARCADIA_BUILD_ROOT}/yt/yt/core/rpc/unittests/bin/rpc-test-service)
    SRCS(
        test_service_binary.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        test_service_binary_internal.go
    )
ENDIF()

END()

RECURSE(
    example
    gotest
    tcptest
)
