GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.63.2)

SRCS(
    flags.go
)

GO_TEST_SRCS(flags_test.go)

END()

RECURSE(
    gotest
)
