GO_LIBRARY()

LICENSE(MIT)

VERSION(v3.5.0+incompatible)

SRCS(
    accumulator.go
)

GO_XTEST_SRCS(accumulator_benchmark_test.go)

END()

RECURSE(
    gotest
)
