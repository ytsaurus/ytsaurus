GO_LIBRARY()

SRCS(
    call.go
    encoder.go
    error_wrapper.go
    logging.go
    mutation_retrier.go
    params_gen.go
    pinger.go
    proxy_set.go
    retrier.go
    stop_group.go
    tracing.go
    transaction.go
    unmapper.go
    verb.go
)

IF (OPENSOURCE)
    SRCS(
        cert_pool.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        cert_pool_internal.go
    )
ENDIF()

GO_TEST_SRCS(
    mutation_retrier_test.go
    proxy_set_test.go
    retrier_test.go
    stop_group_test.go
    transaction_test.go
    verb_test.go
)

END()

RECURSE(
    gotest
    httpclient
    rpcclient
    smartreader
    yt-gen-client
)
