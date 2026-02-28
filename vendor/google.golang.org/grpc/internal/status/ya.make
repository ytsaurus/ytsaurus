GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    status.go
)

GO_XTEST_SRCS(status_test.go)

END()

RECURSE(
    gotest
)
