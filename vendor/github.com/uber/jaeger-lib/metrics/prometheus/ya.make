GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    cache.go
    factory.go
)

GO_XTEST_SRCS(factory_test.go)

END()

RECURSE(
    gotest
)
