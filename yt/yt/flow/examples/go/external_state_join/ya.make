GO_PROGRAM()

SRCS(
    main.go
    lookup_join.go
)

GO_TEST_SRCS(
    lookup_join_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
