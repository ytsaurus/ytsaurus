GO_LIBRARY()

SRCS(
    common.go
    http.go
)

IF (NOT OPENSOURCE AND NOT RUN_MANUAL_TESTS)
    SRCS(
        common_internal.go
        auth_internal.go
    )
ENDIF()

END()
