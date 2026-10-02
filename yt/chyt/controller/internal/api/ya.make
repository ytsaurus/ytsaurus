GO_LIBRARY()

SRCS(
    api.go
    commands.go
    config.go
    helpers.go
    http.go
)

GO_TEST_SRCS(
    creation_options_test.go
)

END()
