GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    matcher_header.go
    string_matcher.go
)

GO_TEST_SRCS(
    matcher_header_test.go
    string_matcher_test.go
)

END()

RECURSE(
    gotest
)
