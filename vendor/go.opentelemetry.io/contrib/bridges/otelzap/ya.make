GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.13.0)

SRCS(
    convert.go
    core.go
    encoder.go
    gen.go
)

GO_TEST_SRCS(
    bench_test.go
    convert_test.go
    core_test.go
    encoder_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
