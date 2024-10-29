GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.30.0+incompatible)

SRCS(
    doc.go
    http.go
)

GO_TEST_SRCS(http_test.go)

END()

RECURSE(
    gotest
    zipkin
)
