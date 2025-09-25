GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.11.1)

SRCS(
    cors.go
    utils.go
)

GO_TEST_SRCS(
    bench_test.go
    cors_test.go
    utils_test.go
)

END()

RECURSE(
    # examples
    gotest
    internal
    # wrapper
)
