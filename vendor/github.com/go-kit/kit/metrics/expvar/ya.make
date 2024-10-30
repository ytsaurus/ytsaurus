GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.13.0)

SRCS(
    expvar.go
)

GO_TEST_SRCS(expvar_test.go)

END()

RECURSE(
    gotest
)
