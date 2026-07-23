GO_LIBRARY()

LICENSE(MIT)

VERSION(v5.0.3)

SRCS(
    backoff.go
    error.go
    exponential.go
    retry.go
    ticker.go
    timer.go
)

GO_TEST_SRCS(
    backoff_test.go
    exponential_test.go
    retry_test.go
    ticker_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
