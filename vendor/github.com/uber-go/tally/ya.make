GO_LIBRARY()

LICENSE(MIT)

SRCS(
    histogram.go
    key_gen.go
    pool.go
    reporter.go
    sanitize.go
    scope.go
    scope_registry.go
    stats.go
    types.go
    version.go
)

GO_TEST_SRCS(
    histogram_test.go
    key_gen_test.go
    sanitize_test.go
    scope_benchmark_test.go
    scope_registry_test.go
    scope_test.go
    stats_benchmark_test.go
    stats_test.go
)

END()

RECURSE(
    example
    gotest
    instrument
    internal
    # m3
    multi
    # prometheus
    # statsd
    thirdparty
)
