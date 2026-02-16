GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    resource_watcher.go
    testutils.go
)

GO_TEST_SRCS(balancer_test.go)

END()

RECURSE(
    fakeclient
    gotest
)
