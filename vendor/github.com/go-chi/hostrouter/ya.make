GO_LIBRARY()

LICENSE(MIT)

SRCS(hostrouter.go)

GO_TEST_SRCS(hostrouter_test.go)

END()

RECURSE(gotest)
