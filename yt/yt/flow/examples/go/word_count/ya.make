GO_PROGRAM()

SRCS(
    main.go
    word_count_mapper.go
)

GO_TEST_SRCS(
    word_count_mapper_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
