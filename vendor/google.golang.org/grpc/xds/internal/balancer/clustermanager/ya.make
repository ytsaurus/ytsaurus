GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    balancerstateaggregator.go
    clustermanager.go
    config.go
    picker.go
)

GO_TEST_SRCS(
    clustermanager_test.go
    config_test.go
)

END()

RECURSE(
    e2e_test
    gotest
)
