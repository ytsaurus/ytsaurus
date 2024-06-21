GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
)

GO_TEST_SRCS(context_test.go)

END()

RECURSE(
    gotest
    logtest
)
