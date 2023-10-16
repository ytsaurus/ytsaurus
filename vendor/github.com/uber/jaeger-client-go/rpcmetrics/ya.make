GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    endpoints.go
    metrics.go
    normalizer.go
    observer.go
)

GO_TEST_SRCS(
    endpoints_test.go
    metrics_test.go
    normalizer_test.go
    observer_test.go
)

END()

RECURSE(gotest)
