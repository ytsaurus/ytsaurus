GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.19.0)

SRCS(
    batch.go
    doc.go
    exporter.go
    instrumentation.go
    logger.go
    processor.go
    provider.go
    record.go
    ring.go
    setting.go
    simple.go
)

GO_TEST_SRCS(
    batch_test.go
    bench_test.go
    exporter_test.go
    logger_bench_test.go
    logger_test.go
    provider_test.go
    record_test.go
    ring_test.go
    setting_test.go
)

GO_XTEST_SRCS(
    example_test.go
    simple_test.go
)

END()

RECURSE(
    gotest
    internal
)
