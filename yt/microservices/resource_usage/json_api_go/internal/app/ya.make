GO_LIBRARY()

SRCS(
    api.go
    app.go
    config.go
    middleware.go
    models.go
)

IF (OPENSOURCE)
    SRCS(
        app_external.go
        config_external.go
        metrics_external.go
    )
ELSE()
    SRCS(
        app_internal.go
        config_internal.go
        metrics_internal.go
    )
ENDIF()

END()
