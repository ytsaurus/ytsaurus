GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    bridge.go
)

GO_TEST_SRCS(bridge_test.go)

END()

RECURSE(
    gotest
)