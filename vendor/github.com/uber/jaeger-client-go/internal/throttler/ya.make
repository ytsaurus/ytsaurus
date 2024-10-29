GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.30.0+incompatible)

SRCS(
    throttler.go
)

GO_TEST_SRCS(throttler_test.go)

END()

RECURSE(
    gotest
    remote
)
