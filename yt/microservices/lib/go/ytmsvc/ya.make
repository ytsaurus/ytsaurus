GO_LIBRARY()

SRCS(
    common.go
    crypto.go
    http.go
)

GO_TEST_SRCS(crypto_test.go)

IF (OPENSOURCE)
    SRCS(
        auth_external.go
        metrics_external.go
    )
ELSE()
    SRCS(
        auth_internal.go
        common_internal.go
        metrics_internal.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
