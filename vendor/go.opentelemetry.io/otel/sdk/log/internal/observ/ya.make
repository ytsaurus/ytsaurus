GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.19.0)

SRCS(
    doc.go
    simple_log_processor.go
)

GO_TEST_SRCS(simple_log_processor_test.go)

END()

RECURSE(
    gotest
)
