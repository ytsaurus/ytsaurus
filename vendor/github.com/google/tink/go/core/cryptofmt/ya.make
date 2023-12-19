GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(cryptofmt.go)

GO_XTEST_SRCS(cryptofmt_test.go)

END()

RECURSE(gotest)
