GO_LIBRARY()

SRCS(
    batch.go
    client.go
    http_reader.go
    http_writer.go
    table_reader.go
    table_writer.go
    tablet_tx.go
    testing.go
)

IF (OPENSOURCE)
    SRCS(
        test_client.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        test_client_internal.go
    )
ENDIF()

GO_TEST_SRCS(client_test.go)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
