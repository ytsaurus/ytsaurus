GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.14.0)

SRCS(
    log.go
)

GO_TEST_SRCS(
    attr_test.go
    log_attr_test.go
    log_test.go
)

END()

RECURSE(
    gotest
)
