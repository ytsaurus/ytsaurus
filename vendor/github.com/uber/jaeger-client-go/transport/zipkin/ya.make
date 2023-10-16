GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    http.go
)

GO_TEST_SRCS(http_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
