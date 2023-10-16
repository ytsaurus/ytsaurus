GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    error.go
    error_code.go
    interop.go
    utf8.go
)

GO_TEST_SRCS(
    error_test.go
    interop_test.go
    utf8_test.go
)

END()

RECURSE(
    gotest
    yt-gen-error-code
)
