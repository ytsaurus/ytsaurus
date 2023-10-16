GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(multi.go)

GO_TEST_SRCS(multi_test.go)

END()

RECURSE(gotest)
