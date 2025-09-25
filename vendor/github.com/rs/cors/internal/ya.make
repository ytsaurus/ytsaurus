GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.11.1)

SRCS(
    sortedset.go
)

GO_TEST_SRCS(sortedset_test.go)

END()

RECURSE(
    gotest
)
