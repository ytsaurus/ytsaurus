GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.35.0)

SRCS(
    codes.go
    doc.go
)

GO_TEST_SRCS(codes_test.go)

END()

RECURSE(
    gotest
)
