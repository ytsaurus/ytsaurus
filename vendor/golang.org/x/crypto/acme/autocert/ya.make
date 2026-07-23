GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.53.0)

SRCS(
    autocert.go
    cache.go
    listener.go
    renewal.go
)

GO_TEST_SRCS(
    autocert_test.go
    cache_test.go
    renewal_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    internal
)
