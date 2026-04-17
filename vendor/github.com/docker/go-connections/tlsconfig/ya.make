GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.6.0)

SRCS(
    certpool.go
    config.go
)

GO_TEST_SRCS(config_test.go)

END()

RECURSE(
    fixtures
    gotest
)
