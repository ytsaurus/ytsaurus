GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.1.0)

SRCS(
    diff.go
)

GO_TEST_SRCS(diff_test.go)

END()

RECURSE(
    gotest
)
