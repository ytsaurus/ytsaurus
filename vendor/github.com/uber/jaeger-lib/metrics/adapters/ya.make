GO_LIBRARY()

LICENSE(Apache-2.0)

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

RECURSE(gotest)
