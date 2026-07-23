GO_LIBRARY()

LICENSE(MIT)

VERSION(v3.0.1)

SRCS(
    chain.go
    constantdelay.go
    cron.go
    doc.go
    logger.go
    option.go
    parser.go
    spec.go
)

GO_TEST_SRCS(
    chain_test.go
    constantdelay_test.go
    cron_test.go
    option_test.go
    parser_test.go
    spec_test.go
)

END()

RECURSE(
    gotest
)
