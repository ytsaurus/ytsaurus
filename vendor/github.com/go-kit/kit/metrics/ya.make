GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    metrics.go
    timer.go
)

GO_XTEST_SRCS(timer_test.go)

END()

RECURSE(
    # cloudwatch
    # cloudwatch2
    # dogstatsd
    expvar
    generic
    gotest
    # graphite
    # influx
    # influxstatsd
    internal
    # multi
    # pcp
    # prometheus
    # provider
    # statsd
    teststat
)
