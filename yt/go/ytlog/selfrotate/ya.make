GO_LIBRARY()

SRCS(
    helpers.go
    options.go
    sink.go
)

GO_TEST_SRCS(
    helpers_test.go
    sink_test.go
)

END()

RECURSE(gotest)
