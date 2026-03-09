GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.62.0)

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

GO_XTEST_SRCS(
    common_test.go
    httpconvtest_test.go
)

END()

RECURSE(
    gotest
)
