GO_LIBRARY()

SRCS(
    app.go
)

GO_TEST_SRCS(app_test.go)

END()

RECURSE(
    gotest
)
