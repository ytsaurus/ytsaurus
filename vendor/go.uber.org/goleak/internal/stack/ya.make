GO_LIBRARY()

LICENSE(MIT)

SRCS(stacks.go)

GO_TEST_SRCS(stacks_test.go)

END()

RECURSE(gotest)
