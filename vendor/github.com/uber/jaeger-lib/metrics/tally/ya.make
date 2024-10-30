GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    factory.go
    metrics.go
)

GO_TEST_SRCS(factory_test.go)

END()

RECURSE(
    gotest
)
