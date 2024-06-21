GO_LIBRARY()

LICENSE(MIT)

SRCS(
    config.go
)

GO_TEST_SRCS(config_test.go)

END()

RECURSE(
    gotest
)
