GO_LIBRARY()

LICENSE(MIT)

SRCS(
    clock.go
    context.go
)

GO_TEST_SRCS(
    # clock_test.go
    context_test.go
)

END()

RECURSE(gotest)
