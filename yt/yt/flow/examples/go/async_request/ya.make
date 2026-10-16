GO_PROGRAM()

SRCS(
    main.go
    request_processor.go
    state_keeper.go
)

GO_TEST_SRCS(
    request_processor_test.go
    state_keeper_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
