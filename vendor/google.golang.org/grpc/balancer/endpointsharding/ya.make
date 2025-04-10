GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

SRCS(
    endpointsharding.go
)

GO_XTEST_SRCS(endpointsharding_test.go)

END()

RECURSE(
    gotest
)
