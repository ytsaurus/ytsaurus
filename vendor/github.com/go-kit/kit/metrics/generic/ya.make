GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.13.0)

SRCS(
    generic.go
)

GO_XTEST_SRCS(generic_test.go)

END()

RECURSE(
    # gotest
)
