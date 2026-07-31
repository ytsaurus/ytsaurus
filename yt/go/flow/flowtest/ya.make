GO_LIBRARY()

SRCS(
    harness.go
    response.go
)

GO_TEST_SRCS(
    harness_test.go
    response_test.go
)

END()

RECURSE(gotest)
