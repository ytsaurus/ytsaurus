GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.0.0)

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

RECURSE(
    gotest
)
