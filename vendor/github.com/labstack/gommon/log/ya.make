GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.4.2)

SRCS(
    color.go
    log.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(
    gotest
)
