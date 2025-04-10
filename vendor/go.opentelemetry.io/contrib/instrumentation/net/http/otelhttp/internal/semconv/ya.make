GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.60.0)

SRCS(
    env.go
    gen.go
    httpconv.go
    util.go
    v1.20.0.go
)

GO_TEST_SRCS(
    bench_test.go
    env_test.go
    httpconv_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
