GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    bootstrap.go
    credentials.go
)

GO_TEST_SRCS(bootstrap_test.go)

END()

RECURSE(
    gotest
)
