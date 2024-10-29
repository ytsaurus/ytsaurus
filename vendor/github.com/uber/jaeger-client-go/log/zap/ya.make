GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.30.0+incompatible)

SRCS(
    field.go
    logger.go
    tracer.go
)

GO_TEST_SRCS(
    field_test.go
    logger_test.go
    tracer_test.go
)

END()

RECURSE(
    gotest
    mock_opentracing
)
