GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    local.go
    metricstest.go
)

GO_TEST_SRCS(
    local_test.go
    metricstest_test.go
)

END()

RECURSE(
    gotest
)
