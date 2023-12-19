GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    rate.go
    ratelimit.go
)

GO_TEST_SRCS(
    rate_test.go
    ratelimit_test.go
)

END()

RECURSE(gotest)
