GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    metricregistry.go
    metrics.go
)

GO_TEST_SRCS(metricregistry_test.go)

END()

RECURSE(
    gotest
)
