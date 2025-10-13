GO_LIBRARY()

SRCS(
    common.go
    http.go
)

IF (OPENSOURCE)
    SRCS(
        common_external.go
        auth_external.go
    )
ELSE()
    SRCS(
        common_internal.go
        auth_internal.go
    )
ENDIF()

END()
