GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    errors.go
    framer.go
    http2bridge.go
)

GO_TEST_SRCS(
    errors_test.go
    http2bridge_test.go
)

END()

RECURSE(
    gotest
)
