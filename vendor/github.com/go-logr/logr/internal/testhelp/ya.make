GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    slog.go
)

GO_TEST_SRCS(slog_test.go)

END()

RECURSE(
    gotest
)
