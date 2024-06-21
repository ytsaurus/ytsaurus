GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    any.go
    compare.go
    timestamp.go
)

GO_TEST_SRCS(any_test.go)

END()

RECURSE(
    gotest
    plugin
    proto
    types
)
