GO_LIBRARY()

LICENSE(MIT)

DATA(arcadia/vendor/github.com/HdrHistogram/hdrhistogram-go/test)

TEST_CWD(vendor/github.com/HdrHistogram/hdrhistogram-go)

SRCS(
    hdr.go
    hdr_encoding.go
    log_reader.go
    log_writer.go
    window.go
    zigzag.go
)

GO_TEST_SRCS(
    hdr_encoding_whitebox_test.go
    hdr_whitebox_test.go
    log_writer_test.go
    zigzag_whitebox_test.go
)

GO_XTEST_SRCS(
    example_hdr_test.go
    example_log_writer_test.go
    hdr_benchmark_test.go
    hdr_encoding_test.go
    hdr_test.go
    window_test.go
)

END()

RECURSE(gotest)
