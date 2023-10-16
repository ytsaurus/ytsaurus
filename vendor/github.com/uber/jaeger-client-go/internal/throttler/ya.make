GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(throttler.go)

GO_TEST_SRCS(throttler_test.go)

END()

RECURSE(
    gotest
    remote
)
