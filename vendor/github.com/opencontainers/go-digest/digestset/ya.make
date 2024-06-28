GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    set.go
)

GO_TEST_SRCS(set_test.go)

END()

RECURSE(
    gotest
)
