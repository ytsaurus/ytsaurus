GO_LIBRARY()

SRCS(
    runner.go
)

GO_TEST_SRCS(
    runner_test.go
)

END()

RECURSE(gotest)
