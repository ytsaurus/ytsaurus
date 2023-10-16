GO_LIBRARY()

SRCS(
    call.go
    client.go
    conn_pool.go
    encoder.go
    error_wrapper.go
    helpers.go
    logging.go
    method.go
    mutation_retrier.go
    object_type.go
    request.go
    retrier.go
    rpc_proxy.go
    table_reader.go
    tablet_tx.go
    testing.go
    tracing.go
    tx.go
    wire.go
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

GO_TEST_SRCS(
    client_test.go
    conn_pool_test.go
    helpers_test.go
    mutation_retrier_test.go
    object_type_test.go
    retrier_test.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
