GO_PROGRAM()

SRCS(
    join_function.go
    main.go
)

GO_TEST_SRCS(
    join_function_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
