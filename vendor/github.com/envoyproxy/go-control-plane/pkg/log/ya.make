GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.5-0.20251024222203-75eaa193e329)

SRCS(
    default.go
    log.go
    test.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(
    gotest
)
