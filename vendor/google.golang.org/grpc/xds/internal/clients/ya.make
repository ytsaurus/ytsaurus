GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

SRCS(
    config.go
    transport_builder.go
)

GO_TEST_SRCS(config_test.go)

END()

RECURSE(
    gotest
)
