GO_LIBRARY()

LICENSE(MIT)

SRCS(expvar.go)

GO_TEST_SRCS(expvar_test.go)

END()

RECURSE(gotest)
