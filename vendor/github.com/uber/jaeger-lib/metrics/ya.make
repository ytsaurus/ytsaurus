GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    counter.go
    factory.go
    gauge.go
    histogram.go
    keys.go
    metrics.go
    stopwatch.go
    timer.go
)

GO_XTEST_SRCS(metrics_test.go)

END()

RECURSE(
    adapters
    expvar
    fork
    go-kit
    gotest
    metricstest
    multi
    prometheus
    tally
)
