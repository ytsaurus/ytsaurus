GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.7.0-rc.1)

SRCS(
    call.go
    callset.go
    controller.go
    doc.go
    matchers.go
)

GO_TEST_SRCS(
    call_test.go
    callset_test.go
)

GO_XTEST_SRCS(
    controller_test.go
    example_test.go
    matchers_test.go
    mock_test.go
)

END()

RECURSE(
    #gotest
    internal
)
