GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.53.0)

SRCS(
    env.go
    util.go
    v1.20.0.go
    v1.24.0.go
)

GO_TEST_SRCS(
    bench_test.go
    common_test.go
    util_test.go
    v1.20.0_test.go
    v1.24.0_test.go
)

END()

RECURSE(
    gotest
)
