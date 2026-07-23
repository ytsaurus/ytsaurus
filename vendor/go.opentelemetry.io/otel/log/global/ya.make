GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

VERSION(v0.19.0)

SRCS(
    log.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(
    gotest
)
