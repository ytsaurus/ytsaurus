GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    factory.go
    metrics.go
)

GO_TEST_SRCS(
    factory_test.go
    metrics_test.go
)

END()

RECURSE(
    expvar
    gotest
    # influx
)
