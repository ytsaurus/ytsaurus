GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(fork.go)

GO_TEST_SRCS(fork_test.go)

END()

RECURSE(gotest)
