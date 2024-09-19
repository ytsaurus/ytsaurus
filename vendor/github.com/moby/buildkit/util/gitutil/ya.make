GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    git_protocol.go
    git_ref.go
)

GO_TEST_SRCS(
    git_protocol_test.go
    git_ref_test.go
)

END()

RECURSE(
    gotest
)
