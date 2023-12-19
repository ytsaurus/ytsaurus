GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(monitoring_util.go)

GO_XTEST_SRCS(monitoring_util_test.go)

END()

RECURSE(gotest)
