GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.35.0)

SRCS(
    baggage.go
    context.go
)

GO_TEST_SRCS(context_test.go)

END()

RECURSE(
    gotest
)
