GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    call.go
    callset.go
    controller.go
    matchers.go
)

GO_TEST_SRCS(
    call_test.go
    callset_test.go
)

GO_XTEST_SRCS(
    controller_114_test.go
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
