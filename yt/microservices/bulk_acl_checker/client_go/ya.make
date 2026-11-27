GO_LIBRARY()

SRCS(
    client.go
)

GO_TEST_SRCS(client_test.go)

IF (OPENSOURCE)
    SRCS(
        client_external.go
    )
ELSE()
    SRCS(
        client_internal.go
    )
ENDIF()

END()

RECURSE(gotest)
