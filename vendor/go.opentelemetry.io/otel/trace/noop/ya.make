GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.36.0)

SRCS(
    noop.go
)

GO_TEST_SRCS(noop_test.go)

END()

RECURSE(
    gotest
)
