GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

SRCS(
    weight.go
)

GO_XTEST_SRCS(weight_test.go)

END()

RECURSE(
    gotest
)
