GO_LIBRARY()

LICENSE(MIT)

SRCS(generic.go)

GO_XTEST_SRCS(generic_test.go)

END()

RECURSE(
    # gotest
)
