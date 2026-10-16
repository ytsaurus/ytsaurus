GO_LIBRARY()

SRCS(
    api.go
    commands.go
    config.go
    helpers.go
    http.go
)

GO_TEST_SRCS(
    default_options_test.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
