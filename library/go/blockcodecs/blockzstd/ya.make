GO_LIBRARY()

SRCS(
    pooled.go
    zstd.go
)

GO_XTEST_SRCS(
    pooled_benchmark_test.go
    pooled_test.go
)

END()

RECURSE_FOR_TESTS(gotest)
