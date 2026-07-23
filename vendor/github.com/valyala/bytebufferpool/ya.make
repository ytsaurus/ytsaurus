GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.0.0)

SRCS(
    bytebuffer.go
    doc.go
    pool.go
)

GO_TEST_SRCS(
    bytebuffer_test.go
    bytebuffer_timing_test.go
    pool_test.go
)

GO_XTEST_SRCS(bytebuffer_example_test.go)

END()

RECURSE(
    gotest
)
