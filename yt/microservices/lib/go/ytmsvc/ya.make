GO_LIBRARY()

SRCS(
    common.go
    http.go
)

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
