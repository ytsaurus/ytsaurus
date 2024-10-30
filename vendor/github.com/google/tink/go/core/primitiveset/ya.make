GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.7.0)

SRCS(
    primitiveset.go
)

GO_XTEST_SRCS(primitiveset_test.go)

END()

RECURSE(
    gotest
)
