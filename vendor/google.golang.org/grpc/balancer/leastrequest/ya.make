GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.63.2)

SRCS(
    leastrequest.go
)

GO_TEST_SRCS(balancer_test.go)

END()

RECURSE(
    gotest
)
