GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.19.0)

SRCS(
    doc.go
    keyvalue.go
    kind_string.go
    logger.go
    provider.go
    record.go
    severity.go
    severity_string.go
)

GO_XTEST_SRCS(
    keyvalue_bench_test.go
    keyvalue_test.go
    logger_test.go
    record_bench_test.go
    record_test.go
    severity_test.go
)

END()

RECURSE(
    embedded
    global
    gotest
    internal
    noop
)
