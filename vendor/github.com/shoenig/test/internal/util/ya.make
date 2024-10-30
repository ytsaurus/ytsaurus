GO_LIBRARY()

LICENSE(MPL-2.0)

VERSION(v0.6.6)

SRCS(
    slices.go
)

GO_TEST_SRCS(slices_test.go)

END()

RECURSE(
    gotest
)
