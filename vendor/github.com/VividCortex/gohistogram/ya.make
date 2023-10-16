GO_LIBRARY()

LICENSE(MIT)

SRCS(
    histogram.go
    numerichistogram.go
    weightedhistogram.go
)

GO_TEST_SRCS(
    histogram_test.go
    sample_data_test.go
)

END()

RECURSE(gotest)
