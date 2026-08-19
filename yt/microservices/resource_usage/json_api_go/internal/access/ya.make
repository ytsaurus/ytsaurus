GO_LIBRARY()

SRCS(
    accesschecker.go
    helpers.go
    middleware.go
    models.go
)

GO_TEST_SRCS(models_test.go)

IF (OPENSOURCE)
    SRCS(
        accesschecker_external.go
        middleware_external.go
        models_external.go
    )
ELSE()
    SRCS(
        accesschecker_internal.go
        middleware_internal.go
        models_internal.go
    )

    GO_TEST_SRCS(middleware_internal_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
