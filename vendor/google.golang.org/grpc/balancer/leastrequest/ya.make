GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

SRCS(
    leastrequest.go
)

GO_TEST_SRCS(balancer_test.go)

END()

RECURSE(
    gotest
)
