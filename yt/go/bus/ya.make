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
)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(
        client_test.go
        test_service_test.go
    )
ENDIF()

END()

RECURSE(
    example
    gotest
)
