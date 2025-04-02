GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.69.4)

SRCS(
    attributes.go
)

GO_XTEST_SRCS(attributes_test.go)

END()

RECURSE(
    gotest
)
