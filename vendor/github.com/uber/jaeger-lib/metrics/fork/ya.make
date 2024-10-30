GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    fork.go
)

GO_TEST_SRCS(fork_test.go)

END()

RECURSE(
    gotest
)
