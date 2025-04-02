GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.69.4)

SRCS(
    code_string.go
    codes.go
)

GO_TEST_SRCS(codes_test.go)

END()

RECURSE(
    gotest
)
