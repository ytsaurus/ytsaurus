GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.30.0+incompatible)

SRCS(
    config.go
    config_env.go
    options.go
)

GO_TEST_SRCS(
    config_test.go
    options_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
