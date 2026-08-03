GO_PROGRAM()

SRCS(
    enricher.go
    main.go
    reference_loader.go
)

GO_TEST_SRCS(
    enricher_test.go
    reference_loader_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
