GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

SRCS(
    proxyattributes.go
)

GO_TEST_SRCS(proxyattributes_test.go)

END()

RECURSE(
    gotest
)
