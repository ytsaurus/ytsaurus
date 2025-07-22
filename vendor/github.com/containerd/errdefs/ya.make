GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.0.0)

SRCS(
    errors.go
    resolve.go
)

GO_TEST_SRCS(
    errors_test.go
    resolve_test.go
)

END()

RECURSE(
    gotest
)
