GO_LIBRARY()

SRCS(retry.go)

GO_TEST_SRCS(retry_test.go)

END()

RECURSE(gotest)
