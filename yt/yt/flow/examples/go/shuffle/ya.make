GO_PROGRAM()

SRCS(
    main.go
    event_mapper.go
    event_reducer.go
)

GO_TEST_SRCS(
    event_mapper_test.go
    event_reducer_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
