GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    common.go
    config.go
    doc.go
    handler.go
    labeler.go
    transport.go
    version.go
    wrap.go
)

GO_TEST_SRCS(
    transport_example_test.go
    transport_test.go
)

GO_XTEST_SRCS(
    handler_example_test.go
    handler_test.go
)

END()

RECURSE(
    filters
    gotest
    internal
)
