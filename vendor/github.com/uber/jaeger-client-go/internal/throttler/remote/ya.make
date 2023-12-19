GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    options.go
    throttler.go
)

GO_TEST_SRCS(
    options_test.go
    throttler_test.go
)

END()

RECURSE(gotest)
