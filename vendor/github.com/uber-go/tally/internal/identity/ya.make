GO_LIBRARY()

LICENSE(MIT)

SRCS(accumulator.go)

GO_XTEST_SRCS(accumulator_benchmark_test.go)

END()

RECURSE(gotest)
