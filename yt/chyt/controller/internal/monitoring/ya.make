GO_LIBRARY()

SRCS(
    commands.go
    config.go
    health_checker.go
    http.go
    leader_checker.go
)

IF (OPENSOURCE)
    SRCS(
        metrics_opensource.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        metrics_internal.go
    )
ENDIF()

END()
