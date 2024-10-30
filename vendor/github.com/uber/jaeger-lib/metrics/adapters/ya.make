GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.4.1+incompatible)

SRCS(
    cache.go
    factory.go
    tagless.go
)

GO_TEST_SRCS(
    cache_test.go
    factory_test.go
)

END()

RECURSE(
    gotest
)
