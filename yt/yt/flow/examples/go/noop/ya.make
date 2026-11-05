GO_PROGRAM()

SRCS(
    main.go
    reader.go
)

GO_TEST_SRCS(
    reader_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
