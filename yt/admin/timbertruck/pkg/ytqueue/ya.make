GO_LIBRARY()

SRCS(
    compressor.go
    ytqueue.go
)

GO_TEST_SRCS(
    ytqueue_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
)
