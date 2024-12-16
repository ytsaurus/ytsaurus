GO_LIBRARY()

SRCS(
    admin_panel.go
    app.go
)

GO_TEST_SRCS(app_test.go)

END()

RECURSE(
    gotest
)
