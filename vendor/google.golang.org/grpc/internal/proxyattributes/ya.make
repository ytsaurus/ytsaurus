GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    proxyattributes.go
)

GO_TEST_SRCS(proxyattributes_test.go)

END()

RECURSE(
    gotest
)
