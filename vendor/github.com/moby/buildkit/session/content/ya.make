GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    attachable.go
    caller.go
)

GO_TEST_SRCS(content_test.go)

END()

RECURSE(
    gotest
)
