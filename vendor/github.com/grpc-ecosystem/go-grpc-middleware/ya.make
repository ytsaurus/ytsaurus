GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    chain.go
    doc.go
    wrappers.go
)

GO_TEST_SRCS(
    chain_test.go
    wrappers_test.go
)

END()

RECURSE(
    auth
    # gotest
    logging
    ratelimit
    recovery
    retry
    tags
    testing
    tracing
    # tracing/opentracing
    util
    validator
)
