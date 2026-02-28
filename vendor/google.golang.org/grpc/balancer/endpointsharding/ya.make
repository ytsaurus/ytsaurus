GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    endpointsharding.go
)

GO_XTEST_SRCS(endpointsharding_test.go)

END()

RECURSE(
    gotest
)
