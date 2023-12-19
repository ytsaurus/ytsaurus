GO_LIBRARY()

LICENSE(MIT)

SRCS(
    chain.go
    chi.go
    context.go
    mux.go
    tree.go
)

GO_TEST_SRCS(
    context_test.go
    mux_test.go
    tree_test.go
)

END()

RECURSE(
    gotest
    middleware
)
